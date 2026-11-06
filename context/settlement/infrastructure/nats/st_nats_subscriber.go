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
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleAuthApproved")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.AuthorizationApprovedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap AuthorizationApproved",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.RegisterApprovalCommand{
		EventID:       envelope.EventID,
		TransactionID: payload.TransactionID,
		TerminalID:    payload.TerminalID,
		MerchantID:    payload.MerchantID,
		AmountCents:   payload.AmountCents,
		Currency:      payload.Currency,
		AuthorizedAt:  payload.AuthorizedAt,
	}

	if err := s.batchHandler.RegisterApproval(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) handleReversalCompleted(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleReversalCompleted")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.ReversalCompletedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap ReversalCompleted",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.RegisterReversalCommand{
		EventID:               envelope.EventID,
		OriginalTransactionID: payload.OriginalTransactionID,
		TerminalID:            payload.TerminalID,
		MerchantID:            payload.MerchantID,
		AmountCents:           payload.AmountCents,
		Currency:              payload.Currency,
		CompletedAt:           payload.CompletedAt,
	}

	if err := s.batchHandler.RegisterReversal(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) handleBatchCloseRequested(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleBatchCloseRequested")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.BatchCloseRequestedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap BatchCloseRequested",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.ProcessBatchCloseCommand{
		EventID:        envelope.EventID,
		TerminalID:     payload.TerminalID,
		MerchantID:     payload.MerchantID,
		BatchDate:      payload.BatchDate,
		RequestedAt:    payload.RequestedAt,
		TerminalCount:  payload.TerminalCount,
		TerminalAmount: payload.TerminalAmount,
		Currency:       payload.Currency,
	}

	if err := s.batchHandler.ProcessBatchClose(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) nak(ctx context.Context, msg *natsclient.Msg, err error, eventID string) {
	observability.RecordError(ctx, err)

	var validationErr *pkgerrors.ValidationError
	var conflictErr *pkgerrors.ConflictError

	if errors.As(err, &validationErr) || errors.As(err, &conflictErr) {
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
		// JetStream no vuelve a entregarlo: sin intervención, el evento se pierde.
		s.log.ErrorContext(ctx, "deliveries exhausted — event will not be retried",
			slog.String("event_id", eventID),
			slog.String("subject", msg.Subject),
			slog.Uint64("delivered", delivered),
			slog.String("error", err.Error()),
		)
		observability.RecordNATSFailed(ctx, msg.Subject, "exhausted")
		_ = msg.Nak()
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
