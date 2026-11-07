// Package nats contiene los adaptadores NATS del BC Authorization.
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// EventPublisher implementa domain/service.EventPublisher usando NATS JetStream.
// Transforma los Domain Events internos del BC en eventos de integración
// (pkg/events) y los publica al stream correspondiente.
type EventPublisher struct {
	pub *natsutil.Publisher
}

// NewEventPublisher construye el publisher con el natsutil.Publisher inyectado.
func NewEventPublisher(pub *natsutil.Publisher) *EventPublisher {
	return &EventPublisher{pub: pub}
}

// PublishApproved publica AuthorizationApprovedPayload al stream POSNET_AUTH.
// Consumidores: Terminal Gateway, Settlement, Notification.
func (p *EventPublisher) PublishApproved(ctx context.Context, tx *aggregate.Transaction) error {
	if tx.AuthCode() == nil {
		return fmt.Errorf("publisher: cannot publish approved event — auth code is nil")
	}
	if tx.AuthorizedAt() == nil {
		return fmt.Errorf("publisher: cannot publish approved event — authorized_at is nil")
	}

	payload := events.AuthorizationApprovedPayload{
		TransactionID: tx.ID().String(),
		TerminalID:    tx.TerminalID().String(),
		MerchantID:    tx.MerchantID().String(),
		AuthCode:      tx.AuthCode().String(),
		AmountCents:   tx.Amount().Cents(),
		Currency:      tx.Amount().Currency().String(),
		CardLast4:     tx.PAN().Last4(),
		CardNetwork:   string(tx.PAN().Network()),
		EntryMode:     tx.EntryMode().String(),
		FraudScore:    tx.FraudDecision().Score,
		AuthorizedAt:  tx.AuthorizedAt().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectAuthApproved,
		events.SubjectAuthApproved,
		tx.ID().String(),
		"Transaction",
		tx.ID().String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("publisher: publish approved: %w", err)
	}
	return nil
}

// PublishRejected publica AuthorizationRejectedPayload al stream POSNET_AUTH.
// Consumidores: Terminal Gateway, Notification.
func (p *EventPublisher) PublishRejected(ctx context.Context, tx *aggregate.Transaction) error {
	rc := tx.RejectionCode()
	if rc == nil {
		return fmt.Errorf("publisher: cannot publish rejected event — rejection code is nil")
	}

	payload := events.AuthorizationRejectedPayload{
		TransactionID:   tx.ID().String(),
		TerminalID:      tx.TerminalID().String(),
		MerchantID:      tx.MerchantID().String(),
		RejectionCode:   rc.Code(),
		RejectionReason: rc.Description(),
		IsRetryable:     rc.IsRetryable(),
		CaptureCard:     rc.RequiresCardCapture(),
		Source:          string(rc.Source()),
		AmountCents:     tx.Amount().Cents(),
		Currency:        tx.Amount().Currency().String(),
		CardLast4:       tx.PAN().Last4(),
		CardNetwork:     string(tx.PAN().Network()),
		EntryMode:       tx.EntryMode().String(),
		RejectedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectAuthRejected,
		events.SubjectAuthRejected,
		tx.ID().String(),
		"Transaction",
		tx.ID().String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("publisher: publish rejected: %w", err)
	}
	return nil
}

// PublishInvalidRejected publica AuthorizationRejectedPayload para una transacción con datos
// inválidos. Consumidores: Terminal Gateway, Notification.
func (p *EventPublisher) PublishInvalidRejected(ctx context.Context, r service.InvalidTransactionRejection) error {
	payload := events.AuthorizationRejectedPayload{
		TransactionID:   r.TransactionID.String(),
		TerminalID:      r.TerminalID,
		MerchantID:      r.MerchantID,
		RejectionCode:   r.Rejection.Code(),
		RejectionReason: r.Rejection.Description(),
		IsRetryable:     r.Rejection.IsRetryable(),
		CaptureCard:     r.Rejection.RequiresCardCapture(),
		Source:          string(r.Rejection.Source()),
		AmountCents:     r.AmountCents,
		Currency:        r.Currency,
		CardLast4:       r.CardLast4,
		CardNetwork:     r.CardNetwork,
		EntryMode:       r.EntryMode,
		RejectedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectAuthRejected,
		events.SubjectAuthRejected,
		r.TransactionID.String(),
		"Transaction",
		r.TransactionID.String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("publisher: publish invalid rejected: %w", err)
	}
	return nil
}

// PublishFraudCheckRequested publica FraudCheckRequestedPayload al stream POSNET_FRAUD.
// Consumidor: Fraud Detection BC.
func (p *EventPublisher) PublishFraudCheckRequested(ctx context.Context, tx *aggregate.Transaction) error {
	payload := events.FraudCheckRequestedPayload{
		TransactionID: tx.ID().String(),
		TerminalID:    tx.TerminalID().String(),
		MerchantID:    tx.MerchantID().String(),
		AmountCents:   tx.Amount().Cents(),
		Currency:      tx.Amount().Currency().String(),
		CardNetwork:   string(tx.PAN().Network()),
		EntryMode:     tx.EntryMode().String(),
		OccurredAt:    tx.ReceivedAt().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectFraudCheckRequested,
		events.SubjectFraudCheckRequested,
		tx.ID().String(),
		"Transaction",
		tx.ID().String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("publisher: publish fraud check requested: %w", err)
	}
	return nil
}

// PublishReversalCompleted publica ReversalCompletedPayload al stream POSNET_AUTH.
// Consumidores: Settlement, Notification.
func (p *EventPublisher) PublishReversalCompleted(
	ctx context.Context,
	txID domain.TransactionID,
	tx *aggregate.Transaction,
) error {
	payload := events.ReversalCompletedPayload{
		OriginalTransactionID: txID.String(),
		TerminalID:            tx.TerminalID().String(),
		MerchantID:            tx.MerchantID().String(),
		AmountCents:           tx.Amount().Cents(),
		Currency:              tx.Amount().Currency().String(),
		CompletedAt:           time.Now().UTC().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectReversalCompleted,
		events.SubjectReversalCompleted,
		txID.String(),
		"Transaction",
		txID.String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("publisher: publish reversal completed: %w", err)
	}
	return nil
}
