package http

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── fakeBatchRepo ───────────────────────────────────────────────────────────

type fakeBatchRepo struct {
	saveErr error

	findByIDResult *aggregate.SettlementBatch
	findByIDErr    error

	listResult []*aggregate.SettlementBatch
	listErr    error
}

var _ repository.SettlementBatchRepository = (*fakeBatchRepo)(nil)

func (f *fakeBatchRepo) Save(context.Context, *aggregate.SettlementBatch) error { return f.saveErr }

func (f *fakeBatchRepo) SaveTx(_ context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(context.Background(), b)
}

func (f *fakeBatchRepo) MerchantDayBatchesTx(context.Context, pgx.Tx, domain.MerchantID, time.Time) ([]repository.MerchantDayBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeBatchRepo) FindOpenByTerminal(context.Context, domain.TerminalID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) FindOrCreateOpen(context.Context, domain.TerminalID, domain.MerchantID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) ListByMerchantDate(context.Context, domain.MerchantID, time.Time) ([]*aggregate.SettlementBatch, error) {
	return f.listResult, f.listErr
}

func (f *fakeBatchRepo) FindStaleOpenIDs(context.Context, time.Time, string) ([]string, error) {
	return nil, nil
}

func (f *fakeBatchRepo) ClosedTotalsForTerminalDay(context.Context, domain.TerminalID, time.Time, string, string) (int, int64, error) {
	return 0, 0, nil
}

// panickyBatchRepo simula un bug en la capa de aplicación para probar que
// recoverMiddleware efectivamente protege al proceso HTTP.
type panickyBatchRepo struct{}

var _ repository.SettlementBatchRepository = (*panickyBatchRepo)(nil)

func (p *panickyBatchRepo) Save(context.Context, *aggregate.SettlementBatch) error { return nil }

func (f *panickyBatchRepo) SaveTx(_ context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(context.Background(), b)
}

func (f *panickyBatchRepo) MerchantDayBatchesTx(context.Context, pgx.Tx, domain.MerchantID, time.Time) ([]repository.MerchantDayBatch, error) {
	return nil, nil
}

func (p *panickyBatchRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	panic("boom")
}

func (p *panickyBatchRepo) FindOpenByTerminal(context.Context, domain.TerminalID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (p *panickyBatchRepo) FindOrCreateOpen(context.Context, domain.TerminalID, domain.MerchantID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (p *panickyBatchRepo) ListByMerchantDate(context.Context, domain.MerchantID, time.Time) ([]*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (p *panickyBatchRepo) FindStaleOpenIDs(context.Context, time.Time, string) ([]string, error) {
	return nil, nil
}

func (p *panickyBatchRepo) ClosedTotalsForTerminalDay(context.Context, domain.TerminalID, time.Time, string, string) (int, int64, error) {
	return 0, 0, nil
}

// ─── fakeProcessor ───────────────────────────────────────────────────────────

type fakeProcessor struct {
	confirmationID string
	err            error
}

func (f *fakeProcessor) Submit(context.Context, *aggregate.SettlementBatch) (string, error) {
	return f.confirmationID, f.err
}

// ─── fakePool ────────────────────────────────────────────────────────────────

// fakePool implementa pgutil.PgxPool. Solo Ping() se ejercita en este
// paquete — ningún handler HTTP del BC Settlement abre transacciones.
type fakePool struct {
	pingErr error
}

func (f *fakePool) Ping(context.Context) error { return f.pingErr }

func (f *fakePool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("fakePool: BeginTx not implemented")
}

// ─── builders ────────────────────────────────────────────────────────────────

func newBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}

func newClosedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "11111111-1111-4111-8111-111111111111",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateClosed,
		Currency:   "ARS",
		CreatedAt:  time.Now(),
	})
}
