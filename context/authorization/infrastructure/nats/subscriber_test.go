package nats

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// ─── logRecorder ─────────────────────────────────────────────────────────────

// logRecorder es un slog.Handler mínimo que guarda los records emitidos, para
// poder verificar la clasificación permanente/transitoria de nak() sin
// depender de una conexión NATS real (msg.Ack/Nak/Term no son observables
// desde afuera cuando el *nats.Msg no está atado a una suscripción real).
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

func (l *logRecorder) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.records)
}

// ─── Subscribe ──────────────────────────────────────────────────────────────

func TestSubscribe_RegistersAllConsumers(t *testing.T) {
	js := &fakeJetStream{}
	h := command.NewAuthorizationHandler(&fakeRepo{}, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	if err := s.Subscribe(); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(js.subscribeCalls) != 3 {
		t.Fatalf("QueueSubscribe calls = %d, want 3", len(js.subscribeCalls))
	}
	wantSubjects := []string{
		events.SubjectTransactionReceived,
		events.SubjectFraudScoreCalculated,
		events.SubjectReversalRequested,
	}
	for i, want := range wantSubjects {
		if js.subscribeCalls[i].subject != want {
			t.Errorf("call[%d].subject = %q, want %q", i, js.subscribeCalls[i].subject, want)
		}
		if js.subscribeCalls[i].durable == "" {
			t.Errorf("call[%d].durable is empty", i)
		}
	}
}

func TestSubscribe_PropagatesRegistrationError(t *testing.T) {
	js := &fakeJetStream{subscribeErr: errors.New("nats unavailable")}
	h := command.NewAuthorizationHandler(&fakeRepo{}, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	err := s.Subscribe()
	if err == nil || !strings.Contains(err.Error(), "register consumer") {
		t.Fatalf("error = %v, want it to contain %q", err, "register consumer")
	}
	if len(js.subscribeCalls) != 1 {
		t.Errorf("QueueSubscribe calls = %d, want 1 (debe detenerse en el primer fallo)", len(js.subscribeCalls))
	}
}

// ─── handleTransactionReceived ────────────────────────────────────────────────

func TestHandleTransactionReceived_MalformedEnvelope(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectTransactionReceived, Data: []byte("not-json")}
	s.handleTransactionReceived(msg) // no debe hacer panic

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0 (envelope malformado debe cortar antes)", len(repo.savedTxs))
	}
}

func TestHandleTransactionReceived_UnwrapError(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	// Data no deserializable al payload esperado (un número en vez de un objeto).
	envelope, err := events.Wrap(events.SubjectTransactionReceived, "agg-1", "Transaction", "corr-1", "", 12345)
	if err != nil {
		t.Fatalf("events.Wrap() error = %v", err)
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	msg := &natsclient.Msg{Subject: events.SubjectTransactionReceived, Data: data}
	s.handleTransactionReceived(msg)

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0 (unwrap fallido debe cortar antes)", len(repo.savedTxs))
	}
}

func TestHandleTransactionReceived_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{}
	pub := &fakeDomainPublisher{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, pub, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.TransactionReceivedPayload{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   5000,
		Currency:      "ARS",
		STAN:          1,
		EntryMode:     "CHIP",
		CardLast4:     "1234",
		CardNetwork:   "VISA",
		EMVDataBase64: "emv==",
		ISO8583Raw:    []byte{0x01},
		ReceivedAt:    "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectTransactionReceived, payload.TransactionID, payload)

	s.handleTransactionReceived(msg)

	if len(repo.savedTxs) != 1 {
		t.Fatalf("saved txs = %d, want 1", len(repo.savedTxs))
	}
	if repo.savedTxs[0].State() != valueobject.StateFraudChecking {
		t.Errorf("saved tx state = %v, want %v", repo.savedTxs[0].State(), valueobject.StateFraudChecking)
	}
	if pub.fraudCheckCalls != 1 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 1", pub.fraudCheckCalls)
	}
}

func TestHandleTransactionReceived_HandlerValidationError(t *testing.T) {
	// TransactionID inválido → AuthorizeTransaction falla en la etapa de
	// validación, antes de tocar la DB — no hace falta pgxmock acá.
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.TransactionReceivedPayload{
		TransactionID: "not-a-uuid",
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   5000,
		Currency:      "ARS",
		STAN:          1,
		EntryMode:     "CHIP",
		CardLast4:     "1234",
		CardNetwork:   "VISA",
	}
	msg := newEnvelopeMsg(t, events.SubjectTransactionReceived, "agg-1", payload)

	s.handleTransactionReceived(msg) // no debe hacer panic

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestHandleTransactionReceived_InvalidDataPublishesRejection(t *testing.T) {
	pub := &fakeDomainPublisher{}
	h := command.NewAuthorizationHandler(&fakeRepo{}, &fakeAcquirer{}, pub, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	id := domain.NewTransactionID().String()
	s.handleTransactionReceived(newEnvelopeMsg(t, events.SubjectTransactionReceived, id, events.TransactionReceivedPayload{
		TransactionID: id,
		CardNetwork:   "BOGUS",
	}))

	if pub.invalidRejectedCalls != 1 {
		t.Errorf("PublishInvalidRejected calls = %d, want 1", pub.invalidRejectedCalls)
	}
}

// ─── handleFraudScoreCalculated ────────────────────────────────────────────────

func TestHandleFraudScoreCalculated_MalformedEnvelope(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectFraudScoreCalculated, Data: []byte("not-json")}
	s.handleFraudScoreCalculated(msg)

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestHandleFraudScoreCalculated_UnwrapError(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectFraudScoreCalculated, "agg-1", 12345)
	s.handleFraudScoreCalculated(msg)

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestHandleFraudScoreCalculated_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	pub := &fakeDomainPublisher{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, pub, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.FraudScoreCalculatedPayload{
		TransactionID: repo.findResult.ID().String(),
		Score:         90,
		Decision:      valueobject.FraudDecisionReject,
		EvaluatedAt:   "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectFraudScoreCalculated, payload.TransactionID, payload)

	s.handleFraudScoreCalculated(msg)

	if len(repo.savedTxs) != 1 || repo.savedTxs[0].State() != valueobject.StateRejected {
		t.Fatalf("saved tx = %+v, want a single tx in state REJECTED", repo.savedTxs)
	}
	if pub.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", pub.rejectedCalls)
	}
}

func TestHandleFraudScoreCalculated_HandlerValidationError(t *testing.T) {
	// El claim de idempotencia ocurre antes de validar el TransactionID, así
	// que igual hace falta el pool mockeado.
	pool := newMockPool(t)

	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.FraudScoreCalculatedPayload{
		TransactionID: "not-a-uuid",
		Score:         10,
		Decision:      valueobject.FraudDecisionApprove,
	}
	msg := newEnvelopeMsg(t, events.SubjectFraudScoreCalculated, "agg-1", payload)

	s.handleFraudScoreCalculated(msg) // no debe hacer panic

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

// ─── handleReversalRequested ───────────────────────────────────────────────────

func TestHandleReversalRequested_MalformedEnvelope(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectReversalRequested, Data: []byte("not-json")}
	s.handleReversalRequested(msg)

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestHandleReversalRequested_UnwrapError(t *testing.T) {
	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectReversalRequested, "agg-1", 12345)
	s.handleReversalRequested(msg)

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestHandleReversalRequested_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newApprovedTransaction(t)}
	pub := &fakeDomainPublisher{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, pub, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.ReversalRequestedPayload{
		OriginalTransactionID: repo.findResult.ID().String(),
		TerminalID:            domain.NewTerminalID().String(),
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           5000,
		Currency:              "ARS",
		OriginalAuthCode:      "AB1234",
		RequestedAt:           "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectReversalRequested, payload.OriginalTransactionID, payload)

	s.handleReversalRequested(msg)

	if len(repo.savedTxs) != 1 || repo.savedTxs[0].State() != valueobject.StateReversed {
		t.Fatalf("saved tx = %+v, want a single tx in state REVERSED", repo.savedTxs)
	}
	if pub.reversalCalls != 1 {
		t.Errorf("PublishReversalCompleted calls = %d, want 1", pub.reversalCalls)
	}
}

func TestHandleReversalRequested_HandlerValidationError(t *testing.T) {
	pool := newMockPool(t)

	repo := &fakeRepo{}
	h := command.NewAuthorizationHandler(repo, &fakeAcquirer{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.ReversalRequestedPayload{
		OriginalTransactionID: "not-a-uuid",
		TerminalID:            domain.NewTerminalID().String(),
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           5000,
		Currency:              "ARS",
	}
	msg := newEnvelopeMsg(t, events.SubjectReversalRequested, "agg-1", payload)

	s.handleReversalRequested(msg) // no debe hacer panic

	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

// ─── nak ────────────────────────────────────────────────────────────────────

func TestNak_ValidationErrorTerminatesMessage(t *testing.T) {
	rec := &logRecorder{}
	s := &Subscriber{log: slog.New(rec)}
	msg := &natsclient.Msg{Subject: "test"}

	s.nak(context.Background(), msg, pkgerrors.NewValidationError("bad data"), "evt-1")

	if got := rec.count(); got != 1 {
		t.Fatalf("log records = %d, want 1", got)
	}
	last := rec.last()
	if last.Level != slog.LevelError {
		t.Errorf("level = %v, want Error", last.Level)
	}
	if last.Message != "permanent error — terminating message" {
		t.Errorf("message = %q, want %q", last.Message, "permanent error — terminating message")
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
	if last.Message != "permanent error — terminating message" {
		t.Errorf("message = %q, want %q", last.Message, "permanent error — terminating message")
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

// ─── helpers ────────────────────────────────────────────────────────────────

// newEnvelopeMsg empaqueta payload en un DomainEvent envelope y lo serializa
// en un *nats.Msg listo para pasarle a los handlers del Subscriber.
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
