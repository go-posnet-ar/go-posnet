package command_test

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── fakeBatchRepo ───────────────────────────────────────────────────────────

type fakeBatchRepo struct {
	saveErr       error
	saveCallCount int
	savedBatches  []*aggregate.SettlementBatch

	findByIDResult *aggregate.SettlementBatch
	findByIDErr    error

	findOpenResult   *aggregate.SettlementBatch
	findOpenErr      error
	findOpenDate     time.Time
	findOpenCurrency string

	findOrCreateResult   *aggregate.SettlementBatch
	findOrCreateErr      error
	findOrCreateDate     time.Time
	findOrCreateCurrency string

	listResult []*aggregate.SettlementBatch
	listErr    error

	dayResult []repository.MerchantDayBatch
	dayErr    error

	auditEntries []repository.BatchAuditEntry
	auditErr     error

	staleIDs []string
	staleErr error
	staleNow time.Time // el now que recibió FindStaleOpenIDs

	priorCount  int
	priorAmount int64
	priorErr    error
}

var _ repository.SettlementBatchRepository = (*fakeBatchRepo)(nil)

func (f *fakeBatchRepo) Save(_ context.Context, b *aggregate.SettlementBatch) error {
	f.saveCallCount++
	f.savedBatches = append(f.savedBatches, b)
	return f.saveErr
}

func (f *fakeBatchRepo) SaveTx(ctx context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(ctx, b)
}

func (f *fakeBatchRepo) MerchantDayBatchesTx(context.Context, pgx.Tx, domain.MerchantID, time.Time) ([]repository.MerchantDayBatch, error) {
	return f.dayResult, f.dayErr
}

func (f *fakeBatchRepo) InsertAuditTx(_ context.Context, _ pgx.Tx, e repository.BatchAuditEntry) error {
	if f.auditErr != nil {
		return f.auditErr
	}
	f.auditEntries = append(f.auditEntries, e)
	return nil
}

func (f *fakeBatchRepo) lastSaved() *aggregate.SettlementBatch {
	if len(f.savedBatches) == 0 {
		return nil
	}
	return f.savedBatches[len(f.savedBatches)-1]
}

func (f *fakeBatchRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeBatchRepo) FindOpenByTerminal(_ context.Context, _ domain.TerminalID, date time.Time, currency string) (*aggregate.SettlementBatch, error) {
	f.findOpenDate, f.findOpenCurrency = date, currency
	return f.findOpenResult, f.findOpenErr
}

func (f *fakeBatchRepo) FindOrCreateOpenTx(_ context.Context, _ pgx.Tx, _ domain.TerminalID, _ domain.MerchantID, date time.Time, currency string) (*aggregate.SettlementBatch, error) {
	f.findOrCreateDate, f.findOrCreateCurrency = date, currency
	return f.findOrCreateResult, f.findOrCreateErr
}

func (f *fakeBatchRepo) ListByMerchantDate(context.Context, domain.MerchantID, time.Time) ([]*aggregate.SettlementBatch, error) {
	return f.listResult, f.listErr
}

func (f *fakeBatchRepo) FindStaleOpenIDs(_ context.Context, now time.Time, _ string) ([]string, error) {
	f.staleNow = now
	return f.staleIDs, f.staleErr
}

func (f *fakeBatchRepo) ClosedTotalsForTerminalDay(context.Context, domain.TerminalID, time.Time, string, string) (int, int64, error) {
	return f.priorCount, f.priorAmount, f.priorErr
}

// ─── fakePublisher / fakeProcessor ───────────────────────────────────────────

type fakePublisher struct {
	buildBatchClosedErr   error
	buildBatchClosedCalls int

	buildCompletedErr error
	completed         []completedEvent
}

type completedEvent struct {
	merchantID, date string
	batches          int
	gross, adj, net  int64
	currency         string
	mdr              float64
}

func (f *fakePublisher) BuildBatchClosed(context.Context, *aggregate.SettlementBatch) (string, string, []byte, error) {
	f.buildBatchClosedCalls++
	return "posnet.settlement.batch-closed.v1", "event-closed", []byte("{}"), f.buildBatchClosedErr
}

func (f *fakePublisher) BuildSettlementCompleted(_ context.Context, merchantID, date string, batches int, gross, adj, net int64, currency string, mdr float64) (string, string, []byte, error) {
	f.completed = append(f.completed, completedEvent{merchantID, date, batches, gross, adj, net, currency, mdr})
	return "posnet.settlement.completed.v1", "event-completed", []byte("{}"), f.buildCompletedErr
}

type fakeProcessor struct {
	confirmationID string
	err            error
	calls          int
}

func (f *fakeProcessor) Submit(context.Context, *aggregate.SettlementBatch) (string, error) {
	f.calls++
	return f.confirmationID, f.err
}

// ─── builders ────────────────────────────────────────────────────────────────

func newOpenBatch(t *testing.T, terminalID domain.TerminalID, merchantID domain.MerchantID) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(terminalID, merchantID, time.Now(), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}
