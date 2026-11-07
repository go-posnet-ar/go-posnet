package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// fakeSessionRepo implementa repository.PaymentSessionRepository.
type fakeSessionRepo struct {
	findByIDResult *aggregate.PaymentSession
	findByIDErr    error

	findActiveResult *aggregate.PaymentSession
	findActiveErr    error
}

func (f *fakeSessionRepo) Save(context.Context, *aggregate.PaymentSession) error { return nil }

func (f *fakeSessionRepo) SaveTx(context.Context, pgx.Tx, *aggregate.PaymentSession) error {
	return nil
}

func (f *fakeSessionRepo) FindByID(context.Context, domain.TransactionID) (*aggregate.PaymentSession, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeSessionRepo) FindActiveByTerminal(context.Context, domain.TerminalID) (*aggregate.PaymentSession, error) {
	return f.findActiveResult, f.findActiveErr
}

func (f *fakeSessionRepo) ExpireAwaiting(context.Context, int) ([]repository.ExpiredSession, error) {
	return nil, nil
}

// ─── builders de dominio ──────────────────────────────────────────────────────

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

func newSession(t *testing.T, state valueobject.SessionState) *aggregate.PaymentSession {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelNFC,
		State:      state,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}
