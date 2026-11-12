// Package nats contiene los adaptadores NATS del BC Settlement.
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// EventPublisher implementa domain/service.EventPublisher usando NATS JetStream.
type EventPublisher struct {
	pub *natsutil.Publisher
}

func NewEventPublisher(pub *natsutil.Publisher) *EventPublisher {
	return &EventPublisher{pub: pub}
}

// BuildBatchClosed serializa el evento BatchClosed sin publicarlo.
// Consumido por: Notification BC.
func (p *EventPublisher) BuildBatchClosed(ctx context.Context, b *aggregate.SettlementBatch, trigger string) (string, string, []byte, error) {
	if b.Summary() == nil {
		return "", "", nil, fmt.Errorf("st publisher: cannot build BatchClosed — summary is nil")
	}

	payload := events.BatchClosedPayload{
		BatchID:       b.ID(),
		TerminalID:    b.TerminalID().String(),
		MerchantID:    b.MerchantID().String(),
		BatchDate:     b.BatchDate().Format("2006-01-02"),
		TotalCount:    b.Summary().TotalCount(),
		TotalAmount:   b.Summary().TotalAmount().Cents(),
		Currency:      b.Currency(),
		Discrepancies: b.Discrepancies(),
		ClosedAt:      time.Now().UTC().Format(time.RFC3339),

		AdjustmentCount:  b.Summary().AdjustmentCount(),
		AdjustmentAmount: b.Summary().AdjustmentAmount(),
		ReconciledWith:   b.ReconciledWith(),
		Trigger:          trigger,
	}

	eventID, data, err := p.pub.Build(ctx,
		events.SubjectBatchClosed,
		events.SubjectBatchClosed,
		b.ID(), "SettlementBatch",
		b.ID(), "",
		payload,
	)
	if err != nil {
		return "", "", nil, fmt.Errorf("st publisher: build BatchClosed: %w", err)
	}
	return events.SubjectBatchClosed, eventID, data, nil
}

// BuildSettlementCompleted serializa el resumen de liquidación diaria sin publicarlo.
func (p *EventPublisher) BuildSettlementCompleted(ctx context.Context, c service.SettlementCompletion) (string, string, []byte, error) {
	merchantID := c.MerchantID
	payload := events.SettlementCompletedPayload{
		MerchantID:     c.MerchantID,
		SettlementDate: c.SettlementDate,
		TotalBatches:   c.TotalBatches,
		TotalAmount:    c.TotalAmount,
		Currency:       c.Currency,

		AdjustmentAmount: c.AdjustmentAmount,
		NetAmount:        c.NetAmount,
		MDRPercent:       c.MDRPercent,
		CompletedAt:      time.Now().UTC().Format(time.RFC3339),

		Revision:       c.Revision,
		DeltaBatches:   c.DeltaBatches,
		DeltaAmount:    c.DeltaAmount,
		DeltaNetAmount: c.DeltaNetAmount,
	}

	eventID, data, err := p.pub.Build(ctx,
		events.SubjectSettlementCompleted,
		events.SubjectSettlementCompleted,
		merchantID, "Settlement",
		merchantID, "",
		payload,
	)
	if err != nil {
		return "", "", nil, fmt.Errorf("st publisher: build SettlementCompleted: %w", err)
	}
	return events.SubjectSettlementCompleted, eventID, data, nil
}
