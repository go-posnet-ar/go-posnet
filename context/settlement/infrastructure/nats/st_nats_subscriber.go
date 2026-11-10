package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/observability"
	natsclient "github.com/nats-io/nats.go"
)

// maxDeliveries es el total de entregas de cada mensaje. Cambiarlo requiere recrear
// los consumers: la suscripción a un durable existente falla si el valor difiere.
const maxDeliveries = 5

// retryDelays es la espera antes de cada reentrega por un error transitorio, y da unos
// 12 minutos para que se recupere, por ejemplo, una base caída.
var retryDelays = [maxDeliveries - 1]time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
}

// Subscriber registra y gestiona los durable consumers del BC Settlement.
type Subscriber struct {
	js           natsclient.JetStreamContext
	batchHandler *command.BatchHandler
	log          *slog.Logger
}

func NewSubscriber(js natsclient.JetStreamContext, batchHandler *command.BatchHandler) *Subscriber {
	return &Subscriber{
		js:           js,
		batchHandler: batchHandler,
		log:          slog.Default().With(slog.String("component", "settlement.subscriber")),
	}
}

// Subscribe registra los 3 consumers del BC Settlement.
func (s *Subscriber) Subscribe() error {
	consumers := []struct {
		subject  string
		durable  string
		handler  natsclient.MsgHandler
		maxDeliv int
	}{
		{
			subject:  events.SubjectAuthApproved,
			durable:  "settlement-auth-consumer",
			handler:  s.handleAuthApproved,
			maxDeliv: maxDeliveries,
		},
		{
			subject:  events.SubjectReversalCompleted,
			durable:  "settlement-reversal-consumer",
			handler:  s.handleReversalCompleted,
			maxDeliv: maxDeliveries,
		},
		{
			subject:  events.SubjectBatchCloseRequested,
			durable:  "settlement-batch-consumer",
			handler:  s.handleBatchCloseRequested,
			maxDeliv: maxDeliveries,
		},
	}

	for _, c := range consumers {
		_, err := s.js.QueueSubscribe(
			c.subject,
			c.durable,
			countedHandler(c.handler),
			natsclient.Durable(c.durable),
			natsclient.AckExplicit(),
			natsclient.MaxDeliver(c.maxDeliv),
			natsclient.AckWait(30*time.Second),
			natsclient.DeliverNew(),
		)
		if err != nil {
			return fmt.Errorf("settlement subscriber: register %q: %w", c.durable, err)
		}
		s.log.Info("consumer registered", slog.String("durable", c.durable))
	}
	return nil
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (s *Subscriber) handleAuthApproved(msg *natsclient.Msg) {
	s.handle(msg, "subscriber.handleAuthApproved")
}

func (s *Subscriber) handleReversalCompleted(msg *natsclient.Msg) {
	s.handle(msg, "subscriber.handleReversalCompleted")
}

func (s *Subscriber) handleBatchCloseRequested(msg *natsclient.Msg) {
	s.handle(msg, "subscriber.handleBatchCloseRequested")
}

// handle procesa un mensaje y lo confirma, o lo devuelve para reintento según el error.
func (s *Subscriber) handle(msg *natsclient.Msg, spanName string) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, spanName)
	defer span.End()

	eventID, err := s.process(ctx, msg.Subject, msg.Data)
	if err != nil {
		s.nak(ctx, msg, err, eventID)
		return
	}
	_ = msg.Ack()
}

// process decodifica el evento y ejecuta el comando que le corresponde por subject. Lo usan el
// consumer y el reproceso de la DLQ. Un evento que no se puede decodificar es un error
// permanente: reintentarlo no lo arregla.
func (s *Subscriber) process(ctx context.Context, subject string, data []byte) (string, error) {
	envelope, err := events.UnmarshalEnvelope(data)
	if err != nil {
		return "", pkgerrors.NewValidationError("malformed envelope: " + err.Error())
	}

	switch subject {
	case events.SubjectAuthApproved:
		payload, err := events.Unwrap[events.AuthorizationApprovedPayload](envelope)
		if err != nil {
			return envelope.EventID, pkgerrors.NewValidationError("unwrap AuthorizationApproved: " + err.Error())
		}
		return envelope.EventID, s.batchHandler.RegisterApproval(ctx, port.RegisterApprovalCommand{
			EventID:       envelope.EventID,
			TransactionID: payload.TransactionID,
			TerminalID:    payload.TerminalID,
			MerchantID:    payload.MerchantID,
			AmountCents:   payload.AmountCents,
			Currency:      payload.Currency,
			AuthorizedAt:  payload.AuthorizedAt,
		})

	case events.SubjectReversalCompleted:
		payload, err := events.Unwrap[events.ReversalCompletedPayload](envelope)
		if err != nil {
			return envelope.EventID, pkgerrors.NewValidationError("unwrap ReversalCompleted: " + err.Error())
		}
		return envelope.EventID, s.batchHandler.RegisterReversal(ctx, port.RegisterReversalCommand{
			EventID:               envelope.EventID,
			OriginalTransactionID: payload.OriginalTransactionID,
			TerminalID:            payload.TerminalID,
			MerchantID:            payload.MerchantID,
			AmountCents:           payload.AmountCents,
			Currency:              payload.Currency,
			CompletedAt:           payload.CompletedAt,
		})

	case events.SubjectBatchCloseRequested:
		payload, err := events.Unwrap[events.BatchCloseRequestedPayload](envelope)
		if err != nil {
			return envelope.EventID, pkgerrors.NewValidationError("unwrap BatchCloseRequested: " + err.Error())
		}
		return envelope.EventID, s.batchHandler.ProcessBatchClose(ctx, port.ProcessBatchCloseCommand{
			EventID:        envelope.EventID,
			TerminalID:     payload.TerminalID,
			MerchantID:     payload.MerchantID,
			BatchDate:      payload.BatchDate,
			RequestedAt:    payload.RequestedAt,
			TerminalCount:  payload.TerminalCount,
			TerminalAmount: payload.TerminalAmount,
			Currency:       payload.Currency,
		})
	}
	return envelope.EventID, pkgerrors.NewValidationError("unexpected subject " + subject)
}

func (s *Subscriber) nak(ctx context.Context, msg *natsclient.Msg, err error, eventID string) {
	observability.RecordError(ctx, err)

	if isPermanent(err) {
		s.log.ErrorContext(ctx, "permanent error — terminating",
			slog.String("event_id", eventID),
			slog.String("error", err.Error()),
		)
		observability.RecordNATSFailed(ctx, msg.Subject, "validation")
		_ = msg.Term()
		return
	}

	delivered := deliveryCount(msg)
	if delivered >= maxDeliveries {
		// JetStream no vuelve a entregarlo: se guarda en la DLQ, de donde se reprocesa con la API.
		if dlqErr := s.deadLetter(ctx, msg, err, eventID, delivered); dlqErr != nil {
			s.log.ErrorContext(ctx, "deliveries exhausted and the dead letter could not be published — event will not be retried",
				slog.String("event_id", eventID),
				slog.String("subject", msg.Subject),
				slog.Uint64("delivered", delivered),
				slog.String("error", err.Error()),
				slog.String("dlq_error", dlqErr.Error()),
			)
			observability.RecordNATSFailed(ctx, msg.Subject, "exhausted")
			_ = msg.Nak()
			return
		}
		s.log.ErrorContext(ctx, "deliveries exhausted — event moved to the dead letter queue",
			slog.String("event_id", eventID),
			slog.String("subject", msg.Subject),
			slog.Uint64("delivered", delivered),
			slog.String("error", err.Error()),
		)
		observability.RecordNATSFailed(ctx, msg.Subject, "dead_lettered")
		_ = msg.Ack()
		return
	}

	delay := retryDelay(delivered)
	s.log.WarnContext(ctx, "transient error — nacking for retry",
		slog.String("event_id", eventID),
		slog.Uint64("delivered", delivered),
		slog.Duration("retry_in", delay),
		slog.String("error", err.Error()),
	)
	observability.RecordNATSFailed(ctx, msg.Subject, "transient")
	_ = msg.NakWithDelay(delay)
}

// isPermanent indica un error que no se arregla reintentando.
func isPermanent(err error) bool {
	var validationErr *pkgerrors.ValidationError
	var conflictErr *pkgerrors.ConflictError
	return errors.As(err, &validationErr) || errors.As(err, &conflictErr)
}

// deliveryCount devuelve el número de entrega del mensaje. Si no puede leerlo, asume
// la primera: es preferible reintentar de más que dar un evento por perdido.
func deliveryCount(msg *natsclient.Msg) uint64 {
	meta, err := msg.Metadata()
	if err != nil {
		return 1
	}
	return meta.NumDelivered
}

// retryDelay devuelve la espera después de la entrega indicada, contada desde 1.
func retryDelay(delivered uint64) time.Duration {
	if delivered < 1 {
		return retryDelays[0]
	}
	if delivered > uint64(len(retryDelays)) {
		return retryDelays[len(retryDelays)-1]
	}
	return retryDelays[delivered-1]
}

// countedHandler envuelve un MsgHandler para contabilizar cada mensaje entregado
// en posnet_nats_messages_processed_total{subject}.
func countedHandler(h natsclient.MsgHandler) natsclient.MsgHandler {
	return func(m *natsclient.Msg) {
		observability.RecordNATSProcessed(context.Background(), m.Subject)
		h(m)
	}
}
