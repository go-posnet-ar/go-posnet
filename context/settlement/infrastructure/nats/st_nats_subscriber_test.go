package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// ─── logRecorder ─────────────────────────────────────────────────────────────

// logRecorder es un slog.Handler mínimo que guarda los records emitidos, para
// verificar la clasificación permanente/transitoria de nak() sin depender de
// una conexión NATS real (msg.Ack/Nak/Term no son observables desde afuera
// cuando el *nats.Msg no está atado a una suscripción real).
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (l *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (l *logRecorder) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r)
	return nil
}

func (l *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logRecorder) WithGroup(string) slog.Handler      { return l }

func (l *logRecorder) last() slog.Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.records[len(l.records)-1]
}

// ─── builders ────────────────────────────────────────────────────────────────

func newTestBatchHandler(pool pgutil.PgxPool, repo *fakeBatchRepo, publisher *fakeDomainPublisher, processor *fakeProcessor) *command.BatchHandler {
	return command.NewBatchHandler(repo, publisher, outbox.NewStore(idempotencySchema), processor, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool, 2.5)
}

func newEnvelopeMsg(t *testing.T, subject, aggregateID string, payload any) *natsclient.Msg {
	t.Helper()
	envelope, err := events.Wrap(subject, aggregateID, "Transaction", aggregateID, "", payload)
	if err != nil {
		t.Fatalf("events.Wrap() error = %v", err)
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return &natsclient.Msg{Subject: subject, Data: data}
}

// ─── Subscribe ──────────────────────────────────────────────────────────────

func TestSubscribe_RegistersAllConsumers(t *testing.T) {
	js := &fakeJetStream{}
	h := newTestBatchHandler(nil, &fakeBatchRepo{}, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(js, h)

	if err := s.Subscribe(); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(js.subscribeCalls) != 3 {
		t.Fatalf("QueueSubscribe calls = %d, want 3", len(js.subscribeCalls))
	}

	want := map[string]string{
		events.SubjectAuthApproved:        "settlement-auth-consumer",
		events.SubjectReversalCompleted:   "settlement-reversal-consumer",
		events.SubjectBatchCloseRequested: "settlement-batch-consumer",
	}
	for _, call := range js.subscribeCalls {
		durable, ok := want[call.subject]
		if !ok {
			t.Errorf("unexpected subject registered: %q", call.subject)
			continue
		}
		if call.durable != durable {
			t.Errorf("durable for %q = %q, want %q", call.subject, call.durable, durable)
		}
		delete(want, call.subject)
	}
	if len(want) != 0 {
		t.Errorf("missing subscriptions for: %v", want)
	}
}

func TestSubscribe_PropagatesRegistrationError(t *testing.T) {
	js := &fakeJetStream{subscribeErr: errors.New("nats unavailable")}
	h := newTestBatchHandler(nil, &fakeBatchRepo{}, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(js, h)

	err := s.Subscribe()
	if err == nil || !strings.Contains(err.Error(), `register "settlement-auth-consumer"`) {
		t.Fatalf("error = %v, want it to contain %q", err, `register "settlement-auth-consumer"`)
	}
}

// ─── handleAuthApproved ────────────────────────────────────────────────────────

func TestHandleAuthApproved_MalformedEnvelope(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectAuthApproved, Data: []byte("not-json")}
	s.handleAuthApproved(msg) // no debe hacer panic

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 (envelope malformado debe cortar antes)", repo.saveCallCount)
	}
}

func TestHandleAuthApproved_UnwrapError(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, "agg-1", 12345)
	s.handleAuthApproved(msg)

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 (unwrap fallido debe cortar antes)", repo.saveCallCount)
	}
}

func TestHandleAuthApproved_ValidationError(t *testing.T) {
	// terminal_id inválido → RegisterApproval falla en la etapa de validación,
	// antes de tocar la DB — no hace falta pgxmock acá.
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationApprovedPayload{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    "not-a-uuid",
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   1000,
		Currency:      "ARS",
		AuthorizedAt:  "2026-01-15T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, "agg-1", payload)

	s.handleAuthApproved(msg) // no debe hacer panic

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleAuthApproved_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t)
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newTestBatchHandler(pool, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationApprovedPayload{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   1000,
		Currency:      "ARS",
		AuthorizedAt:  "2026-01-15T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, payload.TransactionID, payload)

	s.handleAuthApproved(msg)

	if repo.saveCallCount != 1 {
		t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
	}
	if len(repo.lastSaved().Transactions()) != 1 {
		t.Errorf("saved batch transactions = %d, want 1", len(repo.lastSaved().Transactions()))
	}
}

// ─── handleReversalCompleted ────────────────────────────────────────────────────

func TestHandleReversalCompleted_MalformedEnvelope(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectReversalCompleted, Data: []byte("not-json")}
	s.handleReversalCompleted(msg)

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleReversalCompleted_UnwrapError(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectReversalCompleted, "agg-1", 12345)
	s.handleReversalCompleted(msg)

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleReversalCompleted_ValidationError(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.ReversalCompletedPayload{
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            "not-a-uuid",
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           1000,
		Currency:              "ARS",
		CompletedAt:           "2026-01-15T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectReversalCompleted, "agg-1", payload)

	s.handleReversalCompleted(msg) // no debe hacer panic — nak() debe clasificarlo como permanente

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleReversalCompleted_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t)
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newTestBatchHandler(pool, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.ReversalCompletedPayload{
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            domain.NewTerminalID().String(),
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           1000,
		Currency:              "ARS",
		CompletedAt:           "2026-01-15T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectReversalCompleted, payload.OriginalTransactionID, payload)

	s.handleReversalCompleted(msg)

	if repo.saveCallCount != 1 {
		t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
	}
}

// ─── handleBatchCloseRequested ──────────────────────────────────────────────────

func TestHandleBatchCloseRequested_MalformedEnvelope(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectBatchCloseRequested, Data: []byte("not-json")}
	s.handleBatchCloseRequested(msg)

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleBatchCloseRequested_UnwrapError(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectBatchCloseRequested, "agg-1", 12345)
	s.handleBatchCloseRequested(msg)

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleBatchCloseRequested_ValidationError(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.BatchCloseRequestedPayload{
		TerminalID:     "not-a-uuid",
		MerchantID:     domain.NewMerchantID().String(),
		BatchDate:      "2026-01-15",
		TerminalCount:  0,
		TerminalAmount: 0,
		Currency:       "ARS",
		RequestedAt:    "2026-01-15T23:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectBatchCloseRequested, "agg-1", payload)

	s.handleBatchCloseRequested(msg) // no debe hacer panic — nak() debe clasificarlo como permanente

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleBatchCloseRequested_NoOpenBatch(t *testing.T) {
	repo := &fakeBatchRepo{findOpenResult: nil}
	h := newTestBatchHandler(nil, repo, &fakeDomainPublisher{}, &fakeProcessor{})
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.BatchCloseRequestedPayload{
		TerminalID:     domain.NewTerminalID().String(),
		MerchantID:     domain.NewMerchantID().String(),
		BatchDate:      "2026-01-15",
		TerminalCount:  0,
		TerminalAmount: 0,
		Currency:       "ARS",
		RequestedAt:    "2026-01-15T23:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectBatchCloseRequested, "agg-1", payload)

	s.handleBatchCloseRequested(msg) // ProcessBatchClose retorna nil — no debe nakear

	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestHandleBatchCloseRequested_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)

	batch := newClosableBatch(t) // 1 compra de 1000
	repo := &fakeBatchRepo{findOpenResult: batch}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	h := newTestBatchHandler(pool, repo, &fakeDomainPublisher{}, processor)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.BatchCloseRequestedPayload{
		TerminalID:     batch.TerminalID().String(),
		MerchantID:     batch.MerchantID().String(),
		BatchDate:      "2026-01-15",
		TerminalCount:  1,
		TerminalAmount: 1000,
		Currency:       "ARS",
		RequestedAt:    "2026-01-15T23:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectBatchCloseRequested, "agg-1", payload)

	s.handleBatchCloseRequested(msg)

	if repo.saveCallCount != 2 { // close + submit
		t.Fatalf("Save call count = %d, want 2", repo.saveCallCount)
	}
	if processor.calls != 1 {
		t.Errorf("processor.Submit calls = %d, want 1", processor.calls)
	}
}

// ─── nak ────────────────────────────────────────────────────────────────────

func TestNak_ValidationErrorTerminatesMessage(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}
	msg := &natsclient.Msg{Subject: "test"}

	s.nak(context.Background(), msg, pkgerrors.NewValidationError("bad data"), "evt-1")

	last := rec.last()
	if last.Level != slog.LevelError {
		t.Errorf("level = %v, want Error", last.Level)
	}
	if last.Message != "permanent error — terminating" {
		t.Errorf("message = %q, want %q", last.Message, "permanent error — terminating")
	}
}

func TestNak_ConflictErrorTerminatesMessage(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}
	msg := &natsclient.Msg{Subject: "test"}

	s.nak(context.Background(), msg, pkgerrors.NewConflictError("evt-1"), "evt-1")

	last := rec.last()
	if last.Level != slog.LevelError {
		t.Errorf("level = %v, want Error", last.Level)
	}
	if last.Message != "permanent error — terminating" {
		t.Errorf("message = %q, want %q", last.Message, "permanent error — terminating")
	}
}

func TestNak_TransientErrorRetriesMessage(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}
	msg := &natsclient.Msg{Subject: "test"}

	s.nak(context.Background(), msg, errors.New("db timeout"), "evt-2")

	last := rec.last()
	if last.Level != slog.LevelWarn {
		t.Errorf("level = %v, want Warn", last.Level)
	}
	if last.Message != "transient error — nacking for retry" {
		t.Errorf("message = %q, want %q", last.Message, "transient error — nacking for retry")
	}
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		delivered uint64
		want      time.Duration
	}{
		{0, 5 * time.Second},
		{1, 5 * time.Second},
		{2, 30 * time.Second},
		{3, 2 * time.Minute},
		{4, 10 * time.Minute},
		{5, 10 * time.Minute},
		{99, 10 * time.Minute},
	}
	for _, tt := range tests {
		if got := retryDelay(tt.delivered); got != tt.want {
			t.Errorf("retryDelay(%d) = %v, want %v", tt.delivered, got, tt.want)
		}
	}
}

// jsMsg arma un mensaje con la metadata de JetStream de la entrega indicada.
func jsMsg(delivered int) *natsclient.Msg {
	return &natsclient.Msg{
		Subject: "test",
		Sub:     &natsclient.Subscription{},
		Reply:   fmt.Sprintf("$JS.ACK.STREAM.consumer.%d.10.10.1700000000000000000.0", delivered),
	}
}

func TestDeliveryCount(t *testing.T) {
	if got := deliveryCount(jsMsg(3)); got != 3 {
		t.Errorf("deliveryCount() = %d, want 3", got)
	}
	if got := deliveryCount(&natsclient.Msg{Subject: "test"}); got != 1 {
		t.Errorf("deliveryCount() without metadata = %d, want 1", got)
	}
}

func TestNak_TransientErrorBeforeLastDeliveryIsWarning(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}

	s.nak(context.Background(), jsMsg(maxDeliveries-1), errors.New("db timeout"), "evt-3")

	last := rec.last()
	if last.Level != slog.LevelWarn {
		t.Errorf("level = %v, want Warn", last.Level)
	}
}

func TestNak_LastDeliveryReportsExhausted(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}

	s.nak(context.Background(), jsMsg(maxDeliveries), errors.New("db timeout"), "evt-4")

	last := rec.last()
	if last.Level != slog.LevelError {
		t.Errorf("level = %v, want Error", last.Level)
	}
	if last.Message != "deliveries exhausted — event will not be retried" {
		t.Errorf("message = %q", last.Message)
	}
}
