package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/juantevez/go-posnet/context/notification/application/command"
	"github.com/juantevez/go-posnet/context/notification/application/port"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/observability"
	natsclient "github.com/nats-io/nats.go"
)

// Subscriber registra y gestiona los durable consumers del BC Notification.
type Subscriber struct {
	js      natsclient.JetStreamContext
	handler *command.NotifyHandler
	log     *slog.Logger
}

func NewSubscriber(js natsclient.JetStreamContext, handler *command.NotifyHandler) *Subscriber {
	return &Subscriber{
		js:      js,
		handler: handler,
		log:     slog.Default().With(slog.String("component", "notification.subscriber")),
	}
}

// Subscribe registra los consumers del BC Notification.
func (s *Subscriber) Subscribe() error {
	consumers := []struct {
		subject  string
		durable  string
		handler  natsclient.MsgHandler
		maxDeliv int
	}{
		{
			subject:  events.SubjectAuthApproved,
			durable:  "notify-auth-approved",
			handler:  s.handleAuthApproved,
			maxDeliv: 3,
		},
		{
			subject:  events.SubjectAuthRejected,
			durable:  "notify-auth-rejected",
			handler:  s.handleAuthRejected,
			maxDeliv: 3,
		},
		{
			subject:  events.SubjectBatchClosed,
			durable:  "notify-batch-closed",
			handler:  s.handleBatchClosed,
			maxDeliv: 3,
		},
		{
			subject:  events.SubjectSettlementCompleted,
			durable:  "notify-settlement-completed",
			handler:  s.handleSettlementCompleted,
			maxDeliv: 3,
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
			return fmt.Errorf("nt subscriber: register %q: %w", c.durable, err)
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

	cmd := port.NotifyApprovalCommand{
		EventID:       envelope.EventID,
		TransactionID: payload.TransactionID,
		TerminalID:    payload.TerminalID,
		MerchantID:    payload.MerchantID,
		AuthCode:      payload.AuthCode,
		AmountCents:   payload.AmountCents,
		Currency:      payload.Currency,
		CardLast4:     payload.CardLast4,
		CardNetwork:   payload.CardNetwork,
		EntryMode:     payload.EntryMode,
		AuthorizedAt:  payload.AuthorizedAt,
	}

	if err := s.handler.NotifyApproval(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) handleAuthRejected(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleAuthRejected")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.AuthorizationRejectedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap AuthorizationRejected",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.NotifyRejectionCommand{
		EventID:         envelope.EventID,
		TransactionID:   payload.TransactionID,
		TerminalID:      payload.TerminalID,
		MerchantID:      payload.MerchantID,
		RejectionCode:   payload.RejectionCode,
		RejectionReason: payload.RejectionReason,
		IsRetryable:     payload.IsRetryable,
		AmountCents:     payload.AmountCents,
		Currency:        payload.Currency,
		CardLast4:       payload.CardLast4,
		CardNetwork:     payload.CardNetwork,
		EntryMode:       payload.EntryMode,
		RejectedAt:      payload.RejectedAt,
	}

	if err := s.handler.NotifyRejection(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) handleBatchClosed(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleBatchClosed")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.BatchClosedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap BatchClosed",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.NotifyBatchClosedCommand{
		EventID:       envelope.EventID,
		BatchID:       payload.BatchID,
		TerminalID:    payload.TerminalID,
		MerchantID:    payload.MerchantID,
		BatchDate:     payload.BatchDate,
		TotalCount:    payload.TotalCount,
		TotalAmount:   payload.TotalAmount,
		Currency:      payload.Currency,
		Discrepancies: payload.Discrepancies,
		ClosedAt:      payload.ClosedAt,

		AdjustmentCount:  payload.AdjustmentCount,
		AdjustmentAmount: payload.AdjustmentAmount,
		ReconciledWith:   payload.ReconciledWith,
		Trigger:          payload.Trigger,
		Revision:         payload.IsRevision(),
	}

	if err := s.handler.NotifyBatchClosed(ctx, cmd); err != nil {
		s.nak(ctx, msg, err, envelope.EventID)
		return
	}
	_ = msg.Ack()
}

func (s *Subscriber) handleSettlementCompleted(msg *natsclient.Msg) {
	ctx := observability.ExtractTraceContext(context.Background(), msg)
	ctx, span := observability.StartSpan(ctx, "subscriber.handleSettlementCompleted")
	defer span.End()

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unmarshal envelope", slog.String("error", err.Error()))
		_ = msg.Term()
		return
	}

	payload, err := events.Unwrap[events.SettlementCompletedPayload](envelope)
	if err != nil {
		s.log.ErrorContext(ctx, "failed to unwrap SettlementCompleted",
			slog.String("event_id", envelope.EventID),
			slog.String("error", err.Error()),
		)
		_ = msg.Term()
		return
	}

	cmd := port.NotifySettlementCompletedCommand{
		EventID:          envelope.EventID,
		MerchantID:       payload.MerchantID,
		SettlementDate:   payload.SettlementDate,
		Currency:         payload.Currency,
		Revision:         payload.Revision,
		TotalBatches:     payload.TotalBatches,
		TotalAmount:      payload.TotalAmount,
		AdjustmentAmount: payload.AdjustmentAmount,
		NetAmount:        payload.NetAmount,
		MDRPercent:       payload.MDRPercent,
		DeltaBatches:     payload.DeltaBatches,
		DeltaAmount:      payload.DeltaAmount,
		DeltaNetAmount:   payload.DeltaNetAmount,
		CompletedAt:      payload.CompletedAt,
	}

	if err := s.handler.NotifySettlementCompleted(ctx, cmd); err != nil {
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
