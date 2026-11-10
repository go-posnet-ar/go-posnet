package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func newTestPublisher(js *fakeJetStream) *EventPublisher {
	return NewEventPublisher(natsutil.NewPublisher(js))
}

// ─── BuildBatchClosed ────────────────────────────────────────────────────────

func TestBuildBatchClosed_NilSummary(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)
	b := newOpenBatch(t) // OPEN — Summary() es nil

	_, _, _, err := p.BuildBatchClosed(context.Background(), b)
	if err == nil || !strings.Contains(err.Error(), "summary is nil") {
		t.Fatalf("error = %v, want it to mention summary is nil", err)
	}
}

func TestBuildBatchClosed_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 7}
	p := newTestPublisher(js)
	b := newClosableBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	before := time.Now().UTC().Add(-time.Second)
	subject, eventID, data, err := p.BuildBatchClosed(context.Background(), b)
	if err != nil {
		t.Fatalf("BuildBatchClosed() error = %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	if subject != events.SubjectBatchClosed {
		t.Errorf("subject = %q, want %q", subject, events.SubjectBatchClosed)
	}

	envelope, err := events.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.AggregateType != "SettlementBatch" {
		t.Errorf("AggregateType = %q, want %q", envelope.AggregateType, "SettlementBatch")
	}
	if envelope.AggregateID != b.ID() {
		t.Errorf("AggregateID = %q, want %q", envelope.AggregateID, b.ID())
	}

	payload, err := events.Unwrap[events.BatchClosedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.BatchID != b.ID() {
		t.Errorf("BatchID = %q, want %q", payload.BatchID, b.ID())
	}
	if payload.TerminalID != b.TerminalID().String() {
		t.Errorf("TerminalID = %q, want %q", payload.TerminalID, b.TerminalID().String())
	}
	if payload.MerchantID != b.MerchantID().String() {
		t.Errorf("MerchantID = %q, want %q", payload.MerchantID, b.MerchantID().String())
	}
	if payload.BatchDate != b.BatchDate().Format("2006-01-02") {
		t.Errorf("BatchDate = %q, want %q", payload.BatchDate, b.BatchDate().Format("2006-01-02"))
	}
	if payload.TotalCount != b.Summary().TotalCount() {
		t.Errorf("TotalCount = %d, want %d", payload.TotalCount, b.Summary().TotalCount())
	}
	if payload.TotalAmount != b.Summary().TotalAmount().Cents() {
		t.Errorf("TotalAmount = %d, want %d", payload.TotalAmount, b.Summary().TotalAmount().Cents())
	}
	if payload.Currency != b.Currency() {
		t.Errorf("Currency = %q, want %q", payload.Currency, b.Currency())
	}
	if eventID != envelope.EventID {
		t.Errorf("eventID = %q, want the envelope's %q", eventID, envelope.EventID)
	}
	if payload.Discrepancies != b.Discrepancies() {
		t.Errorf("Discrepancies = %d, want %d", payload.Discrepancies, b.Discrepancies())
	}

	closedAt, err := time.Parse(time.RFC3339, payload.ClosedAt)
	if err != nil {
		t.Fatalf("ClosedAt %q is not RFC3339: %v", payload.ClosedAt, err)
	}
	if closedAt.Before(before) || closedAt.After(after) {
		t.Errorf("ClosedAt = %v, want between %v and %v", closedAt, before, after)
	}
}

// ─── BuildSettlementCompleted ────────────────────────────────────────────────

func TestBuildSettlementCompleted_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 3}
	p := newTestPublisher(js)
	merchantID := "merchant-1"

	before := time.Now().UTC().Add(-time.Second)
	subject, eventID, data, err := p.BuildSettlementCompleted(context.Background(), merchantID, "2026-01-15", 4, 100000, -400, 97500, "ARS", 2.5)
	if err != nil {
		t.Fatalf("BuildSettlementCompleted() error = %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	if subject != events.SubjectSettlementCompleted {
		t.Errorf("subject = %q, want %q", subject, events.SubjectSettlementCompleted)
	}

	envelope, err := events.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if eventID != envelope.EventID {
		t.Errorf("eventID = %q, want the envelope's %q", eventID, envelope.EventID)
	}
	if envelope.AggregateType != "Settlement" {
		t.Errorf("AggregateType = %q, want %q", envelope.AggregateType, "Settlement")
	}
	if envelope.AggregateID != merchantID {
		t.Errorf("AggregateID = %q, want %q", envelope.AggregateID, merchantID)
	}

	payload, err := events.Unwrap[events.SettlementCompletedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.MerchantID != merchantID {
		t.Errorf("MerchantID = %q, want %q", payload.MerchantID, merchantID)
	}
	if payload.SettlementDate != "2026-01-15" {
		t.Errorf("SettlementDate = %q, want %q", payload.SettlementDate, "2026-01-15")
	}
	if payload.TotalBatches != 4 {
		t.Errorf("TotalBatches = %d, want 4", payload.TotalBatches)
	}
	if payload.TotalAmount != 100000 {
		t.Errorf("TotalAmount = %d, want 100000", payload.TotalAmount)
	}
	if payload.AdjustmentAmount != -400 {
		t.Errorf("AdjustmentAmount = %d, want -400", payload.AdjustmentAmount)
	}
	if payload.NetAmount != 97500 {
		t.Errorf("NetAmount = %d, want 97500", payload.NetAmount)
	}
	if payload.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", payload.Currency, "ARS")
	}
	if payload.MDRPercent != 2.5 {
		t.Errorf("MDRPercent = %v, want 2.5", payload.MDRPercent)
	}

	completedAt, err := time.Parse(time.RFC3339, payload.CompletedAt)
	if err != nil {
		t.Fatalf("CompletedAt %q is not RFC3339: %v", payload.CompletedAt, err)
	}
	if completedAt.Before(before) || completedAt.After(after) {
		t.Errorf("CompletedAt = %v, want between %v and %v", completedAt, before, after)
	}
}

// Un lote con un ajuste lo informa aparte: el total ya lo incluye, la cantidad no.
func TestBuildBatchClosed_BreaksOutAdjustments(t *testing.T) {
	p := newTestPublisher(&fakeJetStream{ackSeq: 1})
	b := newClosableBatch(t)
	if err := b.AddAdjustment(domain.NewTransactionID(), -400); err != nil {
		t.Fatalf("AddAdjustment() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, _, data, err := p.BuildBatchClosed(context.Background(), b)
	if err != nil {
		t.Fatalf("BuildBatchClosed() error = %v", err)
	}
	envelope, err := events.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	payload, err := events.Unwrap[events.BatchClosedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TotalCount != 1 || payload.TotalAmount != 600 {
		t.Errorf("totals = %d/%d, want 1/600", payload.TotalCount, payload.TotalAmount)
	}
	if payload.AdjustmentCount != 1 || payload.AdjustmentAmount != -400 {
		t.Errorf("adjustments = %d/%d, want 1/-400", payload.AdjustmentCount, payload.AdjustmentAmount)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
}
