// Package nats contiene los adaptadores NATS del BC Fraud Detection.
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

// EventPublisher implementa domain/service.EventPublisher: arma el envelope del score y lo
// escribe en el outbox, dentro de la transacción que le pasan. El relay de pkg/outbox lo publica
// después a JetStream, con el event_id como Nats-Msg-Id.
type EventPublisher struct {
	pub   *natsutil.Publisher
	store *outbox.Store
}

// NewEventPublisher construye el publisher. pub solo arma los envelopes: no publica.
func NewEventPublisher(pub *natsutil.Publisher, store *outbox.Store) *EventPublisher {
	return &EventPublisher{pub: pub, store: store}
}

// PublishFraudScoreCalculated encola el score para el stream POSNET_FRAUD.
// Consumido por: Authorization BC — continúa o corta la Saga según el score.
func (p *EventPublisher) PublishFraudScoreCalculated(
	ctx context.Context,
	dbTx pgx.Tx,
	fc *aggregate.FraudCase,
) error {
	// El momento de la evaluación, no el de la publicación: el relay puede demorarla.
	evaluatedAt := time.Now().UTC()
	if at := fc.EvaluatedAt(); at != nil {
		evaluatedAt = at.UTC()
	}
	payload := events.FraudScoreCalculatedPayload{
		TransactionID: fc.TransactionID().String(),
		Score:         fc.Score().Score(),
		Decision:      fc.Score().Decision().String(),
		RulesHit:      fc.Score().RulesHit(),
		EvaluatedAt:   evaluatedAt.Format(time.RFC3339),
	}

	txID := fc.TransactionID().String()
	eventID, data, err := p.pub.Build(ctx,
		events.SubjectFraudScoreCalculated,
		events.SubjectFraudScoreCalculated,
		txID, "FraudCase", txID, "",
		payload,
	)
	if err != nil {
		return fmt.Errorf("fd publisher: build FraudScoreCalculated: %w", err)
	}
	if err := p.store.InsertTx(ctx, dbTx, events.SubjectFraudScoreCalculated, eventID, data); err != nil {
		return fmt.Errorf("fd publisher: enqueue FraudScoreCalculated: %w", err)
	}
	return nil
}
