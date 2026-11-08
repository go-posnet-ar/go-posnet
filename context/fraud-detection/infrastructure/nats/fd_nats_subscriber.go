package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/application/command"
	"github.com/juantevez/go-posnet/context/fraud-detection/application/port"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/observability"
	natsclient "github.com/nats-io/nats.go"
)

// Subscriber registra y gestiona el durable consumer del BC Fraud Detection.
type FD_Subscriber struct {
	js      natsclient.JetStreamContext
	handler *command.EvaluateTransactionHandler
	log     *slog.Logger
}

func NewSubscriber(
	js natsclient.JetStreamContext,
	handler *command.EvaluateTransactionHandler,
) *FD_Subscriber {
	return &FD_Subscriber{
		js:      js,
		handler: handler,
		log:     slog.Default().With(slog.String("component", "fraud-detection.subscriber")),
	}
}

// Subscribe registra el único consumer del BC Fraud Detection.
func (s *FD_Subscriber) Subscribe() error {
	_, err := s.js.QueueSubscribe(
		events.SubjectFraudCheckRequested,
		"fraud-check-consumer",
		countedHandler(s.handleFraudCheckRequested),
		natsclient.Durable("fraud-check-consumer"),
		natsclient.AckExplicit(),
		natsclient.MaxDeliver(5),
		natsclient.AckWait(30*time.Second),
		natsclient.DeliverNew(),
	)
	if err != nil {
		return fmt.Errorf("fd subscriber: register fraud-check-consumer: %w", err)
	}

	s.log.Info("consumer registered",
		slog.String("durable", "fraud-check-consumer"),
		slog.String("subject", events.SubjectFraudCheckRequested),
	)
	return nil
}

// handleFraudCheckRequested procesa el evento posnet.fraud.check-requested.v1.
// Traduce el payload al command y lo delega al EvaluateTransactionHandler.
func (s *FD_Subscriber) handleFraudCheckRequested(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleFraudCheckRequested")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope",
			slog.String("error", err.Error()),
		)
		_ = msg.Term() // Malformado — no reintentar
		return
	}

	payload, err := events.Unwrap[events.FraudCheckRequestedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap FraudCheckRequested payload",
			slog.String("error", err.Error()),
			slog.String("event_id", envelope.EventID),
		)
		_ = msg.Term()
		return
	}

	cmd := port.EvaluateTransactionCommand{
		EventID:       envelope.EventID,
		CorrelationID: envelope.CorrelationID,
		TransactionID: payload.TransactionID,
		TerminalID:    payload.TerminalID,
		MerchantID:    payload.MerchantID,
		AmountCents:   payload.AmountCents,
		Currency:      payload.Currency,
		CardNetwork:   payload.CardNetwork,
		EntryMode:     payload.EntryMode,
		OccurredAt:    payload.OccurredAt,
	}

	if err := s.handler.EvaluateTransaction(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}

	_ = msg.Ack()
}

// nak decide Nak (reintento) o Term (DLQ) según el tipo de error.
func (s *FD_Subscriber) nak(ctx context.Context, msg *natsclient.Msg, err error, eventID string) {
	observability.RecordError(ctx, err)

	var validationErr *pkgerrors.ValidationError
	var conflictErr *pkgerrors.ConflictError

	if errors.As(err, &validationErr) || errors.As(err, &conflictErr) {
		s.log.ErrorContext(ctx, "permanent error — terminating message",
			slog.String("event_id", eventID),
			slog.String("error", err.Error()),
		)
		observability.RecordNATSFailed(ctx, msg.Subject, "validation")
		_ = msg.Term()
		return
	}

	s.log.WarnContext(ctx, "transient error — nacking for retry",
		slog.String("event_id", eventID),
		slog.String("error", err.Error()),
	)
	observability.RecordNATSFailed(ctx, msg.Subject, "transient")
	_ = msg.Nak()
}

// countedHandler envuelve un MsgHandler para contabilizar cada mensaje entregado
// en posnet_nats_messages_processed_total{subject}.
func countedHandler(h natsclient.MsgHandler) natsclient.MsgHandler {
	return func(m *natsclient.Msg) {
		observability.RecordNATSProcessed(context.Background(), m.Subject)
		h(m)
	}
}
