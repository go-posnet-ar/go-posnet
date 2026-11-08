// Package nats contiene los adaptadores NATS del BC Authorization.
package nats

import (
	"context"

	"fmt"
	"github.com/jackc/pgx/v5"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

// EventPublisher implementa domain/service.EventPublisher: arma el envelope de cada evento de
// integración (pkg/events) y lo escribe en el outbox, dentro de la transacción de base que le
// pasan. El relay de pkg/outbox lo publica después a JetStream.
type EventPublisher struct {
	pub   *natsutil.Publisher
	store *outbox.Store
}

// NewEventPublisher construye el publisher. pub solo arma los envelopes: no publica.
func NewEventPublisher(pub *natsutil.Publisher, store *outbox.Store) *EventPublisher {
	return &EventPublisher{pub: pub, store: store}
}

// enqueue arma el envelope y lo escribe en el outbox, con el event_id como Nats-Msg-Id: si el
// relay lo publica dos veces, JetStream descarta la segunda.
func (p *EventPublisher) enqueue(ctx context.Context, dbTx pgx.Tx, subject, aggregateID string, payload any) error {
	eventID, data, err := p.pub.Build(ctx, subject, subject, aggregateID, "Transaction", aggregateID, "", payload)
	if err != nil {
		return err
	}
	return p.store.InsertTx(ctx, dbTx, subject, eventID, data)
}

// PublishApproved publica AuthorizationApprovedPayload al stream POSNET_AUTH.
// Consumidores: Terminal Gateway, Settlement, Notification.
func (p *EventPublisher) PublishApproved(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error {
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

	err := p.enqueue(ctx, dbTx, events.SubjectAuthApproved, tx.ID().String(), payload)
	if err != nil {
		return fmt.Errorf("publisher: enqueue approved: %w", err)
	}
	return nil
}

// PublishRejected publica AuthorizationRejectedPayload al stream POSNET_AUTH.
// Consumidores: Terminal Gateway, Notification.
func (p *EventPublisher) PublishRejected(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error {
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

	err := p.enqueue(ctx, dbTx, events.SubjectAuthRejected, tx.ID().String(), payload)
	if err != nil {
		return fmt.Errorf("publisher: enqueue rejected: %w", err)
	}
	return nil
}

// PublishInvalidRejected publica AuthorizationRejectedPayload para una transacción con datos
// inválidos. Consumidores: Terminal Gateway, Notification.
func (p *EventPublisher) PublishInvalidRejected(ctx context.Context, dbTx pgx.Tx, r valueobject.InvalidTransactionRejection) error {
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
		RejectedAt:      r.RejectedAt.UTC().Format(time.RFC3339),
	}

	err := p.enqueue(ctx, dbTx, events.SubjectAuthRejected, r.TransactionID.String(), payload)
	if err != nil {
		return fmt.Errorf("publisher: enqueue invalid rejected: %w", err)
	}
	return nil
}

// PublishFraudCheckRequested publica FraudCheckRequestedPayload al stream POSNET_FRAUD.
// Consumidor: Fraud Detection BC.
func (p *EventPublisher) PublishFraudCheckRequested(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error {
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

	err := p.enqueue(ctx, dbTx, events.SubjectFraudCheckRequested, tx.ID().String(), payload)
	if err != nil {
		return fmt.Errorf("publisher: enqueue fraud check requested: %w", err)
	}
	return nil
}

// PublishReversalCompleted publica ReversalCompletedPayload al stream POSNET_AUTH.
// Consumidores: Settlement, Notification.
func (p *EventPublisher) PublishReversalCompleted(
	ctx context.Context,
	dbTx pgx.Tx,
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

	err := p.enqueue(ctx, dbTx, events.SubjectReversalCompleted, txID.String(), payload)
	if err != nil {
		return fmt.Errorf("publisher: enqueue reversal completed: %w", err)
	}
	return nil
}
