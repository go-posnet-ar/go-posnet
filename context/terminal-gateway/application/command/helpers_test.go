package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/entity"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

const idempotencySchema = "terminal_gateway"

// ─── fakeSessionRepo ───────────────────────────────────────────────────────────

type fakeSessionRepo struct {
	saveErr       error
	saveCallCount int
	savedSessions []*aggregate.PaymentSession

	saveTxErr error

	findResult *aggregate.PaymentSession
	findErr    error
}

func (f *fakeSessionRepo) Save(_ context.Context, s *aggregate.PaymentSession) error {
	f.saveCallCount++
	f.savedSessions = append(f.savedSessions, s)
	return f.saveErr
}

func (f *fakeSessionRepo) SaveTx(_ context.Context, _ pgx.Tx, s *aggregate.PaymentSession) error {
	f.saveCallCount++
	f.savedSessions = append(f.savedSessions, s)
	return f.saveTxErr
}

func (f *fakeSessionRepo) lastSaved() *aggregate.PaymentSession {
	if len(f.savedSessions) == 0 {
		return nil
	}
	return f.savedSessions[len(f.savedSessions)-1]
}

func (f *fakeSessionRepo) FindByID(context.Context, domain.TransactionID) (*aggregate.PaymentSession, error) {
	return f.findResult, f.findErr
}

func (f *fakeSessionRepo) FindActiveByTerminal(context.Context, domain.TerminalID) (*aggregate.PaymentSession, error) {
	return nil, nil
}

func (f *fakeSessionRepo) DeleteExpired(context.Context) (int64, error) {
	return 0, nil
}

// ─── fakeTerminalRepo ──────────────────────────────────────────────────────────

type fakeTerminalRepo struct {
	findResult *entity.Terminal
	findErr    error
}

func (f *fakeTerminalRepo) FindByID(context.Context, domain.TerminalID) (*entity.Terminal, error) {
	return f.findResult, f.findErr
}

func (f *fakeTerminalRepo) FindByCertificateCN(context.Context, string) (*entity.Terminal, error) {
	return nil, nil
}

func (f *fakeTerminalRepo) Save(context.Context, *entity.Terminal) error {
	return nil
}

// ─── fakeNotifier ──────────────────────────────────────────────────────────────

type fakeNotifier struct {
	notifyResultErr          error
	notifyResultCalls        int
	notifySessionCreatedErr  error
	notifySessionCreatedCall int
}

func (f *fakeNotifier) NotifyResult(context.Context, *aggregate.PaymentSession) error {
	f.notifyResultCalls++
	return f.notifyResultErr
}

func (f *fakeNotifier) NotifySessionCreated(context.Context, *aggregate.PaymentSession) error {
	f.notifySessionCreatedCall++
	return f.notifySessionCreatedErr
}

func (f *fakeNotifier) NotifySessionExpired(context.Context, domain.TerminalID, domain.TransactionID) error {
	return nil
}

// ─── fakePublisher ─────────────────────────────────────────────────────────────

type fakePublisher struct {
	buildSubject string
	buildEventID string
	buildPayload []byte
	buildErr     error

	publishReversalErr    error
	publishReversalCalls  int
	publishBatchCloseErr  error
	publishBatchCloseCall int
}

func (f *fakePublisher) PublishTransactionReceived(context.Context, *aggregate.PaymentSession, []byte, string) error {
	return nil
}

func (f *fakePublisher) BuildTransactionReceived(
	_ context.Context, _ *aggregate.PaymentSession, _ []byte, _ string, _ string, _ string, _ string,
) (string, string, []byte, error) {
	if f.buildErr != nil {
		return "", "", nil, f.buildErr
	}
	subject := f.buildSubject
	if subject == "" {
		subject = "posnet.transaction.received.v1"
	}
	eventID := f.buildEventID
	if eventID == "" {
		eventID = "evt-1"
	}
	payload := f.buildPayload
	if payload == nil {
		payload = []byte(`{}`)
	}
	return subject, eventID, payload, nil
}

func (f *fakePublisher) PublishReversalRequested(context.Context, domain.TransactionID, *aggregate.PaymentSession) error {
	f.publishReversalCalls++
	return f.publishReversalErr
}

func (f *fakePublisher) PublishBatchCloseRequested(context.Context, domain.TerminalID, domain.MerchantID, string, int, int64, string) error {
	f.publishBatchCloseCall++
	return f.publishBatchCloseErr
}

// ─── builders ────────────────────────────────────────────────────────────────

func mustMoney(t *testing.T) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(1000, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return m
}

func mustSTAN(t *testing.T) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(123456)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
	}
	return s
}

func activeTerminal(terminalID domain.TerminalID) *entity.Terminal {
	return entity.ReconstitueTerminal(
		terminalID, domain.NewMerchantID(), "TRM-0042", "terminal.example.com",
		entity.TerminalActive, time.Now().UTC(), time.Now().UTC(),
	)
}

func blockedTerminal(terminalID domain.TerminalID) *entity.Terminal {
	return entity.ReconstitueTerminal(
		terminalID, domain.NewMerchantID(), "TRM-0042", "terminal.example.com",
		entity.TerminalBlocked, time.Now().UTC(), time.Now().UTC(),
	)
}

func awaitingSession(t *testing.T, terminalID domain.TerminalID) *aggregate.PaymentSession {
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: terminalID,
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelQR,
		State:      valueobject.StateAwaitingPayment,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}

func processingSession(t *testing.T, terminalID domain.TerminalID) *aggregate.PaymentSession {
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: terminalID,
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelQR,
		State:      valueobject.StateProcessing,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}

func approvedSession(t *testing.T, terminalID domain.TerminalID) *aggregate.PaymentSession {
	now := time.Now().UTC()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: terminalID,
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelQR,
		State:      valueobject.StateApproved,
		AuthCode:   "AUTH123",
		ExpiresAt:  now.Add(5 * time.Minute),
		CreatedAt:  now,
		ClosedAt:   &now,
	})
}
