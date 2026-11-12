package nats

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/notification/application/command"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// ─── logRecorder ─────────────────────────────────────────────────────────────

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

// waitForSaves espera n señales del canal de saves con timeout, para
// sincronizar con los dispatch(...) asíncronos que NotifyHandler dispara en
// goroutines separadas (go h.dispatch(...)) sin recurrir a sleeps.
func waitForSaves(t *testing.T, ch chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for save #%d/%d", i+1, n)
		}
	}
}

// ─── Subscribe ──────────────────────────────────────────────────────────────

func TestSubscribe_RegistersAllConsumers(t *testing.T) {
	js := &fakeJetStream{}
	h := command.NewNotifyHandler(&fakeNotificationRepo{}, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	if err := s.Subscribe(); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if len(js.subscribeCalls) != 4 {
		t.Fatalf("QueueSubscribe calls = %d, want 4", len(js.subscribeCalls))
	}
	wantSubjects := []string{events.SubjectAuthApproved, events.SubjectAuthRejected, events.SubjectBatchClosed, events.SubjectSettlementCompleted}
	for i, want := range wantSubjects {
		if js.subscribeCalls[i].subject != want {
			t.Errorf("call[%d].subject = %q, want %q", i, js.subscribeCalls[i].subject, want)
		}
	}
}

func TestSubscribe_PropagatesRegistrationError(t *testing.T) {
	js := &fakeJetStream{subscribeErr: errors.New("nats unavailable")}
	h := command.NewNotifyHandler(&fakeNotificationRepo{}, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(js, h)

	err := s.Subscribe()
	if err == nil || !strings.Contains(err.Error(), "register") {
		t.Fatalf("error = %v, want it to contain %q", err, "register")
	}
	if len(js.subscribeCalls) != 1 {
		t.Errorf("QueueSubscribe calls = %d, want 1 (debe detenerse en el primer fallo)", len(js.subscribeCalls))
	}
}

// ─── handleAuthApproved ───────────────────────────────────────────────────────

func TestHandleAuthApproved_MalformedEnvelope(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectAuthApproved, Data: []byte("not-json")}
	s.handleAuthApproved(msg) // no debe hacer panic

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleAuthApproved_UnwrapError(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, "agg-1", 12345)
	s.handleAuthApproved(msg)

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleAuthApproved_HandlerValidationError(t *testing.T) {
	// TransactionID inválido → NotifyApproval falla en la etapa de validación,
	// antes de tocar la DB — no hace falta pgxmock acá.
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationApprovedPayload{TransactionID: "not-a-uuid", MerchantID: "also-not-a-uuid"}
	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, "agg-1", payload)

	s.handleAuthApproved(msg) // no debe hacer panic

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleAuthApproved_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	terminal := &fakeTerminalNotifier{delivered: true}
	webhook := &fakeWebhookDispatcher{httpStatus: 200}
	h := command.NewNotifyHandler(repo, terminal, webhook, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationApprovedPayload{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AuthCode:      "AB1234",
		AmountCents:   5000,
		Currency:      "ARS",
		CardLast4:     "1234",
		CardNetwork:   "VISA",
		EntryMode:     "CHIP",
		AuthorizedAt:  "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectAuthApproved, payload.TransactionID, payload)

	s.handleAuthApproved(msg)

	// 2 saves síncronos (persistencia inicial de terminalNotif + webhookNotif)
	// + 2 saves asíncronos (dispatch de cada una).
	waitForSaves(t, repo.saveSignal, 4)
	if repo.savedCount() != 4 {
		t.Errorf("saved count = %d, want 4", repo.savedCount())
	}
}

// ─── handleAuthRejected ───────────────────────────────────────────────────────

func TestHandleAuthRejected_MalformedEnvelope(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectAuthRejected, Data: []byte("not-json")}
	s.handleAuthRejected(msg)

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleAuthRejected_UnwrapError(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectAuthRejected, "agg-1", 12345)
	s.handleAuthRejected(msg)

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

// TestHandleAuthRejected_MissingAmountFailsValidation documenta una regresión
// real que existía antes de este fix: AuthorizationRejectedPayload no llevaba
// amount_cents/currency/card info, así que NotifyRejection fallaba siempre con
// "amount_cents must be positive" — ninguna notificación de rechazo llegaba
// jamás al terminal. Ahora el payload sí lleva esos campos (ver
// pkg/events/authorization_rejected.go), así que omitirlos deliberadamente en
// este test reproduce el bug original y confirma que sigue siendo detectable.
func TestHandleAuthRejected_MissingAmountFailsValidation(t *testing.T) {
	// La validación del receipt ocurre antes de tocar la DB — no hace falta
	// pgxmock acá (ver TestHandleAuthApproved_HandlerValidationError).
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationRejectedPayload{
		TransactionID:   domain.NewTransactionID().String(),
		TerminalID:      domain.NewTerminalID().String(),
		MerchantID:      domain.NewMerchantID().String(),
		RejectionCode:   "05",
		RejectionReason: "Do Not Honor",
		// AmountCents deliberadamente en 0 — simula el payload viejo/incompleto.
		RejectedAt: "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectAuthRejected, payload.TransactionID, payload)

	s.handleAuthRejected(msg) // no debe hacer panic

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0 (amount_cents=0 debe fallar la validación del receipt)", repo.savedCount())
	}
}

func TestHandleAuthRejected_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	terminal := &fakeTerminalNotifier{delivered: true}
	h := command.NewNotifyHandler(repo, terminal, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.AuthorizationRejectedPayload{
		TransactionID:   domain.NewTransactionID().String(),
		TerminalID:      domain.NewTerminalID().String(),
		MerchantID:      domain.NewMerchantID().String(),
		RejectionCode:   "05",
		RejectionReason: "Do Not Honor",
		Source:          "ACQUIRER",
		AmountCents:     5000,
		Currency:        "ARS",
		CardLast4:       "1234",
		CardNetwork:     "VISA",
		EntryMode:       "CHIP",
		RejectedAt:      "2026-01-01T10:00:00Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectAuthRejected, payload.TransactionID, payload)

	s.handleAuthRejected(msg)

	// 1 save síncrono + 1 save asíncrono del dispatch.
	waitForSaves(t, repo.saveSignal, 2)
	if repo.savedCount() != 2 {
		t.Errorf("saved count = %d, want 2", repo.savedCount())
	}
}

// ─── handleBatchClosed ─────────────────────────────────────────────────────────

func TestHandleBatchClosed_MalformedEnvelope(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := &natsclient.Msg{Subject: events.SubjectBatchClosed, Data: []byte("not-json")}
	s.handleBatchClosed(msg)

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleBatchClosed_UnwrapError(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	msg := newEnvelopeMsg(t, events.SubjectBatchClosed, "agg-1", 12345)
	s.handleBatchClosed(msg)

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleBatchClosed_HandlerValidationError(t *testing.T) {
	repo := &fakeNotificationRepo{}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{}, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.BatchClosedPayload{MerchantID: "not-a-uuid"}
	msg := newEnvelopeMsg(t, events.SubjectBatchClosed, "agg-1", payload)

	s.handleBatchClosed(msg) // no debe hacer panic

	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

func TestHandleBatchClosed_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	webhook := &fakeWebhookDispatcher{httpStatus: 200}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, webhook, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.BatchClosedPayload{
		BatchID:     domain.NewTransactionID().String(),
		TerminalID:  domain.NewTerminalID().String(),
		MerchantID:  domain.NewMerchantID().String(),
		BatchDate:   "2026-01-01",
		TotalCount:  10,
		TotalAmount: 50000,
		Currency:    "ARS",
		ClosedAt:    "2026-01-01T23:59:59Z",
	}
	msg := newEnvelopeMsg(t, events.SubjectBatchClosed, payload.BatchID, payload)

	s.handleBatchClosed(msg)

	waitForSaves(t, repo.saveSignal, 2)
	if repo.savedCount() != 2 {
		t.Errorf("saved count = %d, want 2", repo.savedCount())
	}
}

func TestHandleSettlementCompleted_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	webhook := &fakeWebhookDispatcher{httpStatus: 200}
	h := command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, webhook, &fakeDomainPublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	s := NewSubscriber(&fakeJetStream{}, h)

	payload := events.SettlementCompletedPayload{
		MerchantID: domain.NewMerchantID().String(), SettlementDate: "2026-01-01", Currency: "ARS",
		TotalBatches: 2, TotalAmount: 15000, NetAmount: 14625, MDRPercent: 2.5, CompletedAt: "2026-01-02T10:00:00Z",
		Revision: 2, DeltaBatches: 1, DeltaAmount: 5000, DeltaNetAmount: 4875,
	}
	s.handleSettlementCompleted(newEnvelopeMsg(t, events.SubjectSettlementCompleted, payload.MerchantID, payload))

	waitForSaves(t, repo.saveSignal, 2)
	if repo.savedCount() != 2 {
		t.Fatalf("saved count = %d, want 2", repo.savedCount())
	}
	settlement := repo.lastSaved().Receipt().Settlement
	if settlement == nil || settlement.Revision != 2 || settlement.DeltaNetAmount != 4875 || repo.lastSaved().Receipt().AmountCents != 14625 {
		t.Errorf("receipt = %+v, settlement = %+v", repo.lastSaved().Receipt(), settlement)
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
