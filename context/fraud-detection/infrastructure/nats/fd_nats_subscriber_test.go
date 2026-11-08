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

	"github.com/juantevez/go-posnet/context/fraud-detection/application/command"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
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

// ─── Subscribe ──────────────────────────────────────────────────────────────

func TestSubscribe_RegistersConsumer(t *testing.T) {
	js := &fakeJetStream{}
	h := command.NewEvaluateTransactionHandler(&fakeFraudCaseRepo{}, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	if err := s.Subscribe(); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(js.subscribeCalls) != 1 {
		t.Fatalf("QueueSubscribe calls = %d, want 1", len(js.subscribeCalls))
	}
	if js.subscribeCalls[0].subject != events.SubjectFraudCheckRequested {
		t.Errorf("subject = %q, want %q", js.subscribeCalls[0].subject, events.SubjectFraudCheckRequested)
	}
	if js.subscribeCalls[0].durable != "fraud-check-consumer" {
		t.Errorf("durable = %q, want %q", js.subscribeCalls[0].durable, "fraud-check-consumer")
	}
}

func TestSubscribe_PropagatesRegistrationError(t *testing.T) {
	js := &fakeJetStream{subscribeErr: errors.New("nats unavailable")}
	h := command.NewEvaluateTransactionHandler(&fakeFraudCaseRepo{}, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	err := s.Subscribe()
	if err == nil || !strings.Contains(err.Error(), "register fraud-check-consumer") {
		t.Fatalf("error = %v, want it to contain %q", err, "register fraud-check-consumer")
	}
}

// ─── handleFraudCheckRequested ─────────────────────────────────────────────────

func TestHandleFraudCheckRequested_MalformedEnvelope(t *testing.T) {
	repo := &fakeFraudCaseRepo{}
	h := command.NewEvaluateTransactionHandler(repo, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectFraudCheckRequested, Data: []byte("not-json")}
	s.handleFraudCheckRequested(msg) // no debe hacer panic

	if len(repo.savedCases) != 0 {
		t.Errorf("saved cases = %d, want 0 (envelope malformado debe cortar antes)", len(repo.savedCases))
	}
}

func TestHandleFraudCheckRequested_UnwrapError(t *testing.T) {
	repo := &fakeFraudCaseRepo{}
	h := command.NewEvaluateTransactionHandler(repo, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectFraudCheckRequested, "agg-1", 12345)
	s.handleFraudCheckRequested(msg)

	if len(repo.savedCases) != 0 {
		t.Errorf("saved cases = %d, want 0 (unwrap fallido debe cortar antes)", len(repo.savedCases))
	}
}

func TestHandleFraudCheckRequested_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeFraudCaseRepo{}
	pub := &fakeDomainPublisher{}
	h := command.NewEvaluateTransactionHandler(repo, newEngine(t), pub, natsutil.NewIdempotencyStore(pool, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.FraudCheckRequestedPayload{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   6_000_000,
		Currency:      "ARS",
		CardNetwork:   "VISA",
		EntryMode:     "MAGSTRIPE", // + monto alto → activa RULE-005 registrada en newEngine
		OccurredAt:    "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectFraudCheckRequested, payload.TransactionID, payload)

	s.handleFraudCheckRequested(msg)

	if len(repo.savedCases) != 1 {
		t.Fatalf("saved cases = %d, want 1", len(repo.savedCases))
	}
	if repo.savedCases[0].Score().Score() != 20 {
		t.Errorf("saved Score().Score() = %d, want 20 (RULE-005 activada)", repo.savedCases[0].Score().Score())
	}
	if pub.publishCalls != 1 {
		t.Errorf("PublishFraudScoreCalculated calls = %d, want 1", pub.publishCalls)
	}
}

// El subscriber pasa el correlation_id del envelope: con el transaction_id del payload ilegible,
// la transacción del envelope recibe el rechazo.
func TestHandleFraudCheckRequested_PassesTheEnvelopeCorrelationID(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	repo := &fakeFraudCaseRepo{}
	h := command.NewEvaluateTransactionHandler(repo, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(pool, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	txID := domain.NewTransactionID().String()
	payload := events.FraudCheckRequestedPayload{TransactionID: "not-a-uuid", AmountCents: 5000}
	s.handleFraudCheckRequested(newEnvelopeMsg(t, events.SubjectFraudCheckRequested, txID, payload))

	if len(repo.savedCases) != 1 || repo.savedCases[0].TransactionID().String() != txID {
		t.Fatalf("saved = %d cases, want 1 for the envelope transaction %s", len(repo.savedCases), txID)
	}
}

func TestHandleFraudCheckRequested_HandlerValidationError(t *testing.T) {
	// TransactionID inválido → EvaluateTransaction falla en la etapa de
	// validación, antes de tocar la DB — no hace falta pgxmock acá.
	repo := &fakeFraudCaseRepo{}
	h := command.NewEvaluateTransactionHandler(repo, newEngine(t), &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.FraudCheckRequestedPayload{
		TransactionID: "not-a-uuid",
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   5000,
		Currency:      "ARS",
	}
	msg := newEnvelopeMsg(t, events.SubjectFraudCheckRequested, "agg-1", payload)

	s.handleFraudCheckRequested(msg) // no debe hacer panic

	if len(repo.savedCases) != 0 {
		t.Errorf("saved cases = %d, want 0", len(repo.savedCases))
	}
}

// ─── nak ────────────────────────────────────────────────────────────────────

func TestNak_ValidationErrorTerminatesMessage(t *testing.T) {
	rec := &logRecorder{}
	s := &FD_Subscriber{log: slog.New(rec)}
	msg := &natsclient.Msg{Subject: "test"}

	s.nak(context.Background(), msg, pkgerrors.NewValidationError("bad data"), "evt-1")

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
	s := &FD_Subscriber{log: slog.New(rec)}
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
	s := &FD_Subscriber{log: slog.New(rec)}
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
