package postgres_test

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/context/settlement/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── Save ──────────────────────────────────────────────────────────────────────

func TestSave_NoSummary_NoTransactions(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	if err := repo.Save(context.Background(), newOpenBatch(t)); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestSave_WithSummaryAndTransactions(t *testing.T) {
	pool := newMockPool(t)
	b := newClosedBatch(t)

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("INSERT INTO settlement.batch_transactions").
		WithArgs(anyArgs(7)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	if err := repo.Save(context.Background(), b); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestSave_MultipleTransactions(t *testing.T) {
	pool := newMockPool(t)
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddTransaction(domain.NewTransactionID(), 2000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("INSERT INTO settlement.batch_transactions").
		WithArgs(anyArgs(7)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("INSERT INTO settlement.batch_transactions").
		WithArgs(anyArgs(7)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	if err := repo.Save(context.Background(), b); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestSave_UpsertBatchError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	err := repo.Save(context.Background(), newOpenBatch(t))
	if err == nil || !strings.Contains(err.Error(), "SettlementBatchRepo.Save: upsert batch") {
		t.Fatalf("error = %v, want it to contain %q", err, "SettlementBatchRepo.Save: upsert batch")
	}
}

func TestSave_InsertBatchTransactionError(t *testing.T) {
	pool := newMockPool(t)
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("INSERT INTO settlement.batch_transactions").
		WithArgs(anyArgs(7)...).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	err := repo.Save(context.Background(), b)
	if err == nil || !strings.Contains(err.Error(), "SettlementBatchRepo.Save: insert batch_transaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "SettlementBatchRepo.Save: insert batch_transaction")
	}
}

func TestSave_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	repo := postgres.NewSettlementBatchRepo(pool)
	err := repo.Save(context.Background(), newOpenBatch(t))
	if err == nil || !strings.Contains(err.Error(), "begin transaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "begin transaction")
	}
}

// ─── FindByID ─────────────────────────────────────────────────────────────────

func TestFindByID_Success_NoSummary(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindByID(context.Background(), row.id)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if b.ID() != row.id {
		t.Errorf("ID() = %q, want %q", b.ID(), row.id)
	}
	if b.TerminalID().String() != row.terminalID {
		t.Errorf("TerminalID() = %q, want %q", b.TerminalID().String(), row.terminalID)
	}
	if b.State() != valueobject.BatchStateOpen {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateOpen)
	}
	if b.Summary() != nil {
		t.Error("Summary() is not nil, want nil (no summary columns in the row)")
	}
	if len(b.Transactions()) != 0 {
		t.Errorf("Transactions() len = %d, want 0", len(b.Transactions()))
	}
}

func TestFindByID_Success_WithSummaryAndTransactions(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow().withSummary(1, 1, 0, 1000, 1000, 0)
	row.state = "CLOSED"

	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(oneBatchTxRow(row.id))

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindByID(context.Background(), row.id)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
	if b.Summary() == nil {
		t.Fatal("Summary() is nil, want it populated from the summary columns")
	}
	if b.Summary().TotalCount() != 1 {
		t.Errorf("Summary.TotalCount() = %d, want 1", b.Summary().TotalCount())
	}
	if b.Summary().TotalAmount().Cents() != 1000 {
		t.Errorf("Summary.TotalAmount().Cents() = %d, want 1000", b.Summary().TotalAmount().Cents())
	}
	if len(b.Transactions()) != 1 {
		t.Fatalf("Transactions() len = %d, want 1", len(b.Transactions()))
	}
}

// TestFindByID_PartialSummary_DerefsNilBreakdownAsZero cubre el caso donde
// total_count/total_amount están seteados pero el desglose de compra/reversión
// es nil (fila inconsistente/parcial) — derefInt/derefInt64 deben devolver 0.
func TestFindByID_PartialSummary_DerefsNilBreakdownAsZero(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()
	totalCount := 0
	totalAmount := int64(0)
	row.totalCount = &totalCount
	row.totalAmount = &totalAmount
	row.state = "CLOSED"

	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindByID(context.Background(), row.id)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if b.Summary() == nil {
		t.Fatal("Summary() is nil, want it populated (total_count/total_amount were set)")
	}
	if b.Summary().PurchaseCount() != 0 {
		t.Errorf("Summary.PurchaseCount() = %d, want 0 (nil breakdown defaults to zero)", b.Summary().PurchaseCount())
	}
	if b.Summary().ReversalAmount().Cents() != 0 {
		t.Errorf("Summary.ReversalAmount().Cents() = %d, want 0 (nil breakdown defaults to zero)", b.Summary().ReversalAmount().Cents())
	}
}

func TestFindByID_NotFound(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("WHERE id =").
		WithArgs("batch-999").
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindByID(context.Background(), "batch-999")
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestFindByID_ScanError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("WHERE id =").
		WithArgs("batch-1").
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindByID(context.Background(), "batch-1")
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanBatch")
	}
}

func TestFindByID_LoadTransactionsError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindByID(context.Background(), row.id)
	if err == nil || !strings.Contains(err.Error(), "loadTransactions") {
		t.Fatalf("error = %v, want it to contain %q", err, "loadTransactions")
	}
}

// ─── FindOpenByTerminal ─────────────────────────────────────────────────────────

func TestFindOpenByTerminal_Success(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()
	terminalID, err := domain.ParseTerminalID(row.terminalID)
	if err != nil {
		t.Fatalf("ParseTerminalID() error = %v", err)
	}
	date := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)

	pool.ExpectQuery("LIMIT 1").
		WithArgs(row.terminalID, "2026-01-15", "ARS").
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOpenByTerminal(context.Background(), terminalID, date, "ARS")
	if err != nil {
		t.Fatalf("FindOpenByTerminal() error = %v", err)
	}
	if b.ID() != row.id {
		t.Errorf("ID() = %q, want %q", b.ID(), row.id)
	}
}

func TestFindOpenByTerminal_NotFound_ReturnsNilNil(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("LIMIT 1").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("FindOpenByTerminal() error = %v, want nil", err)
	}
	if b != nil {
		t.Errorf("batch = %+v, want nil", b)
	}
}

func TestFindOpenByTerminal_ScanError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("LIMIT 1").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanBatch")
	}
}

func TestFindOpenByTerminal_LoadTransactionsError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectQuery("LIMIT 1").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "loadTransactions") {
		t.Fatalf("error = %v, want it to contain %q", err, "loadTransactions")
	}
}

// ─── FindOrCreateOpen ───────────────────────────────────────────────────────────

func TestFindOrCreateOpen_ExistingBatch(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(row.id))
	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("FindOrCreateOpen() error = %v", err)
	}
	if b.ID() != row.id {
		t.Errorf("ID() = %q, want %q", b.ID(), row.id)
	}
}

func TestFindOrCreateOpen_ExistingBatch_FindByIDError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(row.id))
	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to propagate the inner FindByID error", err)
	}
}

func TestFindOrCreateOpen_CreatesNewBatch(t *testing.T) {
	pool := newMockPool(t)

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	terminalID := domain.NewTerminalID()
	merchantID := domain.NewMerchantID()
	b, err := repo.FindOrCreateOpen(context.Background(), terminalID, merchantID, time.Now(), "ARS")
	if err != nil {
		t.Fatalf("FindOrCreateOpen() error = %v", err)
	}
	if !b.TerminalID().Equals(terminalID) {
		t.Errorf("TerminalID() = %v, want %v", b.TerminalID(), terminalID)
	}
	if b.State() != valueobject.BatchStateOpen {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateOpen)
	}
}

func TestFindOrCreateOpen_ConcurrentConflict_ReloadsWinner(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0)) // conflicto — otro proceso ya insertó
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(row.id))
	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())
	pool.ExpectCommit()
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("FindOrCreateOpen() error = %v", err)
	}
	if b.ID() != row.id {
		t.Errorf("ID() = %q, want %q (el batch creado concurrentemente por el otro proceso)", b.ID(), row.id)
	}
}

func TestFindOrCreateOpen_ConcurrentConflict_FindByIDError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(row.id))
	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to propagate the inner FindByID error", err)
	}
}

func TestFindOrCreateOpen_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable}).WillReturnError(errors.New("connection refused"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "begin transaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "begin transaction")
	}
}

func TestFindOrCreateOpen_CheckExistingError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "FindOrCreateOpen: check existing") {
		t.Fatalf("error = %v, want it to contain %q", err, "FindOrCreateOpen: check existing")
	}
}

func TestFindOrCreateOpen_InvalidTerminalIDPropagatesCreateError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.TerminalID{}, domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "FindOrCreateOpen: create batch") {
		t.Fatalf("error = %v, want it to contain %q", err, "FindOrCreateOpen: create batch")
	}
}

func TestFindOrCreateOpen_InsertBatchError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "FindOrCreateOpen: insert batch") {
		t.Fatalf("error = %v, want it to contain %q", err, "FindOrCreateOpen: insert batch")
	}
}

func TestFindOrCreateOpen_ReloadAfterConflictError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err == nil || !strings.Contains(err.Error(), "FindOrCreateOpen: reload after conflict") {
		t.Fatalf("error = %v, want it to contain %q", err, "FindOrCreateOpen: reload after conflict")
	}
}

// ─── ListByMerchantDate ─────────────────────────────────────────────────────────

func TestListByMerchantDate_Success(t *testing.T) {
	pool := newMockPool(t)
	row1 := newBatchRow()
	row2 := newBatchRow()
	row2.id = "batch-2"

	pool.ExpectQuery("WHERE merchant_id =").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows(batchColumns).
			AddRow(row1.id, row1.terminalID, row1.merchantID, row1.batchDate, row1.state, row1.currency,
				row1.totalCount, row1.totalAmount, row1.purchaseCount, row1.purchaseAmount,
				row1.reversalCount, row1.reversalAmount, row1.discrepancies,
				row1.createdAt, row1.closedAt, row1.submittedAt, row1.settledAt, row1.version, row1.reconciledWith, row1.terminalCount, row1.terminalAmount).
			AddRow(row2.id, row2.terminalID, row2.merchantID, row2.batchDate, row2.state, row2.currency,
				row2.totalCount, row2.totalAmount, row2.purchaseCount, row2.purchaseAmount,
				row2.reversalCount, row2.reversalAmount, row2.discrepancies,
				row2.createdAt, row2.closedAt, row2.submittedAt, row2.settledAt, row2.version, row2.reconciledWith, row2.terminalCount, row2.terminalAmount))
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row1.id).
		WillReturnRows(emptyBatchTxRows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row2.id).
		WillReturnRows(emptyBatchTxRows())

	repo := postgres.NewSettlementBatchRepo(pool)
	batches, err := repo.ListByMerchantDate(context.Background(), domain.NewMerchantID(), time.Now())
	if err != nil {
		t.Fatalf("ListByMerchantDate() error = %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("len(batches) = %d, want 2", len(batches))
	}
	if batches[0].ID() != row1.id || batches[1].ID() != row2.id {
		t.Errorf("results not mapped in order: [0].ID=%q [1].ID=%q", batches[0].ID(), batches[1].ID())
	}
}

func TestListByMerchantDate_Empty(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("WHERE merchant_id =").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows(batchColumns))

	repo := postgres.NewSettlementBatchRepo(pool)
	batches, err := repo.ListByMerchantDate(context.Background(), domain.NewMerchantID(), time.Now())
	if err != nil {
		t.Fatalf("ListByMerchantDate() error = %v", err)
	}
	if len(batches) != 0 {
		t.Errorf("len(batches) = %d, want 0", len(batches))
	}
}

func TestListByMerchantDate_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("WHERE merchant_id =").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.ListByMerchantDate(context.Background(), domain.NewMerchantID(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "ListByMerchantDate: query") {
		t.Fatalf("error = %v, want it to contain %q", err, "ListByMerchantDate: query")
	}
}

func TestListByMerchantDate_LoadTransactionsError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectQuery("WHERE merchant_id =").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.ListByMerchantDate(context.Background(), domain.NewMerchantID(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "loadTransactions") {
		t.Fatalf("error = %v, want it to contain %q", err, "loadTransactions")
	}
}

func TestListByMerchantDate_ScanErrorMidLoop(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	rows := row.rows().RowError(0, errors.New("row 0 scan error"))
	pool.ExpectQuery("WHERE merchant_id =").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(rows)

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.ListByMerchantDate(context.Background(), domain.NewMerchantID(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanBatch")
	}
}

func TestLoadTransactions_ScanErrorMidLoop(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	txRows := oneBatchTxRow(row.id).RowError(0, errors.New("row 0 scan error"))
	pool.ExpectQuery("WHERE id =").
		WithArgs(row.id).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(txRows)

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindByID(context.Background(), row.id)
	if err == nil || !strings.Contains(err.Error(), "loadTransactions: scan") {
		t.Fatalf("error = %v, want it to contain %q", err, "loadTransactions: scan")
	}
}

// ─── FindStaleOpenIDs ─────────────────────────────────────────────────────────

func TestFindStaleOpenIDs_Success(t *testing.T) {
	pool := newMockPool(t)
	now := time.Date(2026, 1, 15, 3, 0, 0, 0, time.UTC)
	pool.ExpectQuery("merchant_timezones").
		WithArgs(now, "UTC").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("batch-1").AddRow("batch-2"))

	ids, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), now, "UTC")
	if err != nil {
		t.Fatalf("FindStaleOpenIDs() error = %v", err)
	}
	if len(ids) != 2 || ids[0] != "batch-1" || ids[1] != "batch-2" {
		t.Errorf("ids = %v, want [batch-1 batch-2]", ids)
	}
}

func TestFindStaleOpenIDs_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("merchant_timezones").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	_, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), time.Now(), "UTC")
	if err == nil || !strings.Contains(err.Error(), "FindStaleOpenIDs: query") {
		t.Fatalf("error = %v, want it to mention FindStaleOpenIDs: query", err)
	}
}

func TestFindStaleOpenIDs_RowsError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("merchant_timezones").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("batch-1").RowError(0, errors.New("broken")))

	if _, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), time.Now(), "UTC"); err == nil {
		t.Fatal("FindStaleOpenIDs() error = nil, want the rows error")
	}
}

// ─── Bloqueo optimista y reversas ─────────────────────────────────────────────

func TestSave_StaleVersionReturnsErrStaleBatch(t *testing.T) {
	pool := newMockPool(t)
	b := newOpenBatch(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("WHERE settlement.settlement_batches.version = EXCLUDED.version - 1").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectRollback()

	err := postgres.NewSettlementBatchRepo(pool).Save(context.Background(), b)
	if !errors.Is(err, repository.ErrStaleBatch) {
		t.Fatalf("error = %v, want ErrStaleBatch", err)
	}
	if b.Version() != 0 {
		t.Errorf("Version() = %d, want 0 (nada se guardó)", b.Version())
	}
}

func TestSave_IncrementsVersionOnEachSave(t *testing.T) {
	pool := newMockPool(t)
	b := newOpenBatch(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	for want := 1; want <= 2; want++ {
		args := anyArgs(21)
		args[17] = want // la versión nueva es el último argumento
		pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		pool.ExpectExec("INSERT INTO settlement.settlement_batches").
			WithArgs(args...).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		pool.ExpectCommit()
		pool.ExpectRollback()

		if err := repo.Save(context.Background(), b); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		if b.Version() != want {
			t.Errorf("Version() = %d, want %d", b.Version(), want)
		}
	}
}

func TestSave_ReversalUsesConflictOnTransactionAndType(t *testing.T) {
	pool := newMockPool(t)
	b := newOpenBatch(t)
	if err := b.RemoveTransaction(domain.NewTransactionID(), 750); err != nil {
		t.Fatalf("RemoveTransaction() error = %v", err)
	}

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec(`ON CONFLICT \(transaction_id, tx_type\) DO NOTHING`).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), int64(750), "ARS", "REVERSAL", pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	if err := postgres.NewSettlementBatchRepo(pool).Save(context.Background(), b); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestFindOrCreateOpen_SerializationFailureIsStale(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.Serializable})
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(&pgconn.PgError{Code: "40001"})
	pool.ExpectRollback()

	_, err := postgres.NewSettlementBatchRepo(pool).FindOrCreateOpen(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if !errors.Is(err, repository.ErrStaleBatch) {
		t.Fatalf("error = %v, want ErrStaleBatch", err)
	}
}

func TestCountBatchesNeedingAttention(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	now := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	closedBefore := now.Add(-time.Hour)
	outboxBefore := now.Add(-time.Minute)
	reviewBefore := now.Add(-15 * time.Minute)

	pool.ExpectQuery("FROM settlement.settlement_batches").
		WithArgs(closedBefore, now, "UTC", outboxBefore, reviewBefore, "reversal-recovery").
		WillReturnRows(pgxmock.NewRows([]string{"disputed", "closed", "open", "outbox", "reviews", "recovery"}).AddRow(int64(3), int64(2), int64(1), int64(4), int64(5), true))

	got, err := repo.CountBatchesNeedingAttention(context.Background(), now, "UTC", closedBefore, outboxBefore, reviewBefore)
	if err != nil {
		t.Fatalf("CountBatchesNeedingAttention() error = %v", err)
	}
	if got.Disputed != 3 || got.ClosedUnsent != 2 || got.OpenOverdue != 1 || got.OutboxStuck != 4 || got.ReversalsInReview != 5 || !got.RecoveryPending {
		t.Errorf("counts = %+v, want {3 2 1 4 5 true}", got)
	}
}

func TestCountBatchesNeedingAttention_QueryError(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectQuery("FROM settlement.settlement_batches").
		WithArgs(anyArgs(6)...).
		WillReturnError(errors.New("db down"))

	if _, err := repo.CountBatchesNeedingAttention(context.Background(), time.Now(), "UTC", time.Now(), time.Now(), time.Now()); err == nil {
		t.Fatal("CountBatchesNeedingAttention() error = nil, want error")
	}
}

func TestSaveTx_UsesCallerTransaction(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateOpen, Currency: "ARS", Version: 3,
	})

	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(21)...).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	pool.ExpectCommit()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	// Sin BeginTx esperado: si SaveTx abriera su propia transacción, pgxmock fallaría.
	if err := repo.SaveTx(context.Background(), tx, b); err != nil {
		t.Fatalf("SaveTx() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if b.Version() != 4 {
		t.Errorf("Version() = %d, want 4", b.Version())
	}
}

func TestMerchantDayBatchesTx_LocksBeforeReading(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	merchantID := domain.NewMerchantID()
	date := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)

	pool.ExpectBegin()
	pool.ExpectExec("pg_advisory_xact_lock").
		WithArgs("settlement-completed:" + merchantID.String() + ":2026-11-05").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	pool.ExpectQuery("FROM settlement.settlement_batches").
		WithArgs(merchantID.String(), "2026-11-05").
		WillReturnRows(pgxmock.NewRows([]string{"currency", "settled", "total"}).
			AddRow("ARS", true, int64(1000)).
			AddRow("USD", false, int64(0)))
	pool.ExpectRollback()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	got, err := repo.MerchantDayBatchesTx(context.Background(), tx, merchantID, date)
	if err != nil {
		t.Fatalf("MerchantDayBatchesTx() error = %v", err)
	}
	want := []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 1000},
		{Currency: "USD", Settled: false, TotalAmount: 0},
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("batches = %+v, want %+v", got, want)
	}
}

func TestMerchantDayBatchesTx_LockError(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectBegin()
	pool.ExpectExec("pg_advisory_xact_lock").WithArgs(pgxmock.AnyArg()).WillReturnError(errors.New("db down"))
	pool.ExpectRollback()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := repo.MerchantDayBatchesTx(context.Background(), tx, domain.NewMerchantID(), time.Now()); err == nil {
		t.Fatal("MerchantDayBatchesTx() error = nil, want error")
	}
}

func TestInsertAuditTx(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	total := int64(1000)
	entry := repository.BatchAuditEntry{
		BatchID: "batch-1", Action: repository.AuditActionResubmit, OperatorID: "op-1",
		FromState: "CLOSED", ToState: "SUBMITTED", PreviousDiscrepancies: 2,
		TotalAmount: &total, ConfirmationID: "conf-1",
	}

	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO settlement.batch_audit_log").
		WithArgs("batch-1", repository.AuditActionResubmit, "op-1", "", "CLOSED", "SUBMITTED", 2, &total, "conf-1").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := repo.InsertAuditTx(context.Background(), tx, entry); err != nil {
		t.Fatalf("InsertAuditTx() error = %v", err)
	}
}

func TestInsertAuditTx_Error(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO settlement.batch_audit_log").
		WithArgs(anyArgs(9)...).
		WillReturnError(errors.New("db down"))
	pool.ExpectRollback()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := repo.InsertAuditTx(context.Background(), tx, repository.BatchAuditEntry{}); err == nil {
		t.Fatal("InsertAuditTx() error = nil, want error")
	}
}

// reviewTx abre una transacción simulada para los métodos de revisión de reversas.
func reviewTx(t *testing.T, pool pgxmock.PgxPoolIface) pgx.Tx {
	t.Helper()
	pool.ExpectBegin()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	return tx
}

func TestFindPurchaseTx(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectReversalLock(pool, "tx-1")
	pool.ExpectQuery("FROM settlement.batch_transactions").WithArgs("tx-1").
		WillReturnRows(pgxmock.NewRows([]string{"merchant", "amount", "currency"}).AddRow("m-1", int64(1000), "ARS"))

	got, err := repo.FindPurchaseTx(context.Background(), tx, "tx-1")
	if err != nil {
		t.Fatalf("FindPurchaseTx() error = %v", err)
	}
	want := repository.ReversalPurchase{MerchantID: "m-1", AmountCents: 1000, Currency: "ARS"}
	if got == nil || *got != want {
		t.Errorf("purchase = %+v, want %+v", got, want)
	}
}

func TestFindPurchaseTx_NotFound(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectReversalLock(pool, "tx-1")
	pool.ExpectQuery("FROM settlement.batch_transactions").WithArgs("tx-1").WillReturnError(pgx.ErrNoRows)

	got, err := repo.FindPurchaseTx(context.Background(), tx, "tx-1")
	if err != nil || got != nil {
		t.Errorf("FindPurchaseTx() = %+v, %v, want nil, nil", got, err)
	}
}

func TestInsertReversalReviewTx_WithoutPurchase(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	pool.ExpectExec("INSERT INTO settlement.reversal_reviews").
		WithArgs("tx-1", "b-1", repository.ReversalReviewPurchaseNotFound, "m-1", int64(1000), "ARS", (*string)(nil), (*int64)(nil), (*string)(nil)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	err := repo.InsertReversalReviewTx(context.Background(), tx, repository.ReversalReview{
		TransactionID: "tx-1", BatchID: "b-1", Reason: repository.ReversalReviewPurchaseNotFound,
		MerchantID: "m-1", AmountCents: 1000, Currency: "ARS",
	})
	if err != nil {
		t.Fatalf("InsertReversalReviewTx() error = %v", err)
	}
}

func TestReconcileReversalReviewTx(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectReversalLock(pool, "tx-1")
	pool.ExpectQuery("UPDATE settlement.reversal_reviews").WithArgs("tx-1", "m-1", int64(1000), "ARS").
		WillReturnRows(pgxmock.NewRows([]string{"reason", "resolved", "batch"}).AddRow(repository.ReversalReviewPurchaseNotFound, true, "b-rev"))

	got, err := repo.ReconcileReversalReviewTx(context.Background(), tx, "tx-1", "b-purchase",
		repository.ReversalPurchase{MerchantID: "m-1", AmountCents: 1000, Currency: "ARS"})
	want := repository.ReviewReconciliation{Resolved: true, BatchID: "b-rev"}
	if err != nil || got != want {
		t.Errorf("ReconcileReversalReviewTx() = %+v, %v, want %+v", got, err, want)
	}
}

// expectNoOpenReview simula que no había una revisión abierta esperando la compra.
func expectNoOpenReview(pool pgxmock.PgxPoolIface) {
	expectReversalLock(pool, "tx-1")
	pool.ExpectQuery("UPDATE settlement.reversal_reviews").WithArgs(anyArgs(4)...).WillReturnError(pgx.ErrNoRows)
}

func TestReconcileReversalReviewTx_NoReview(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectNoOpenReview(pool)
	pool.ExpectQuery("action = ").WithArgs("tx-1", "EXCLUDE").WillReturnError(pgx.ErrNoRows)

	got, err := repo.ReconcileReversalReviewTx(context.Background(), tx, "tx-1", "b-purchase", repository.ReversalPurchase{})
	if err != nil || got != (repository.ReviewReconciliation{}) {
		t.Errorf("ReconcileReversalReviewTx() = %+v, %v, want empty", got, err)
	}
}

// Una reversa excluida sin la compra vuelve, en el lote de la compra, si la compra coincide.
func TestReconcileReversalReviewTx_RestoresExcludedReversal(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectNoOpenReview(pool)
	pool.ExpectQuery("action = ").WithArgs("tx-1", "EXCLUDE").
		WillReturnRows(pgxmock.NewRows([]string{"merchant", "amount", "currency"}).AddRow("m-1", int64(1000), "ARS"))
	pool.ExpectExec("INSERT INTO settlement.batch_transactions").WithArgs("b-purchase", "tx-1", int64(1000), "ARS").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("UPDATE settlement.settlement_batches SET version").WithArgs("b-purchase").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	pool.ExpectExec("UPDATE settlement.reversal_reviews").
		WithArgs("tx-1", "b-purchase", "m-1", int64(1000), "ARS", "RESTORE").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	got, err := repo.ReconcileReversalReviewTx(context.Background(), tx, "tx-1", "b-purchase",
		repository.ReversalPurchase{MerchantID: "m-1", AmountCents: 1000, Currency: "ARS"})
	want := repository.ReviewReconciliation{Restored: true, BatchID: "b-purchase"}
	if err != nil || got != want {
		t.Errorf("ReconcileReversalReviewTx() = %+v, %v, want %+v", got, err, want)
	}
}

// Si la compra no coincide con la reversa excluida, la revisión se reabre en el lote de la compra.
func TestReconcileReversalReviewTx_ReopensExcludedReversal(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	expectNoOpenReview(pool)
	pool.ExpectQuery("action = ").WithArgs("tx-1", "EXCLUDE").
		WillReturnRows(pgxmock.NewRows([]string{"merchant", "amount", "currency"}).AddRow("m-1", int64(1000), "ARS"))
	pool.ExpectExec("resolved_at = NULL").
		WithArgs("tx-1", "b-purchase", repository.ReversalReviewAmountMismatch, "m-1", int64(1500), "ARS").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	got, err := repo.ReconcileReversalReviewTx(context.Background(), tx, "tx-1", "b-purchase",
		repository.ReversalPurchase{MerchantID: "m-1", AmountCents: 1500, Currency: "ARS"})
	want := repository.ReviewReconciliation{Reason: repository.ReversalReviewAmountMismatch, Reopened: true, BatchID: "b-purchase"}
	if err != nil || got != want {
		t.Errorf("ReconcileReversalReviewTx() = %+v, %v, want %+v", got, err, want)
	}
}

// expectReversalLock espera el bloqueo que serializa la compra y la reversa de una transacción.
func expectReversalLock(pool pgxmock.PgxPoolIface, transactionID string) {
	pool.ExpectExec("pg_advisory_xact_lock").WithArgs("settlement-reversal:" + transactionID).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
}

func TestFindPurchaseTx_LockError(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	pool.ExpectExec("pg_advisory_xact_lock").WithArgs("settlement-reversal:tx-1").WillReturnError(errors.New("db down"))

	if _, err := repo.FindPurchaseTx(context.Background(), tx, "tx-1"); err == nil {
		t.Fatal("FindPurchaseTx() error = nil, want the lock error")
	}
}

func reviewRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{"tx", "batch", "reason", "merchant", "amount", "currency", "pm", "pa", "pc", "state", "disc", "resolved", "created", "action"})
}

func TestListOpenReversalReviews(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	created := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	merchant, amount, currency := "m-2", int64(900), "ARS"

	pool.ExpectQuery("FROM settlement.reversal_reviews r").WithArgs(10).
		WillReturnRows(reviewRows().
			AddRow("tx-1", "b-1", repository.ReversalReviewPurchaseNotFound, "m-1", int64(1000), "ARS", (*string)(nil), (*int64)(nil), (*string)(nil), "OPEN", 0, false, created, "").
			AddRow("tx-2", "b-1", repository.ReversalReviewAmountMismatch, "m-1", int64(1000), "ARS", &merchant, &amount, &currency, "DISPUTED", 1, false, created, ""))

	got, err := repo.ListOpenReversalReviews(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListOpenReversalReviews() error = %v", err)
	}
	if len(got) != 2 || got[0].Purchase != nil || got[1].Purchase == nil || got[1].Purchase.AmountCents != 900 || got[1].BatchState != "DISPUTED" {
		t.Errorf("reviews = %+v", got)
	}
}

func TestListOpenReversalReviews_NoneIsEmptyNotNil(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("FROM settlement.reversal_reviews r").WithArgs(10).WillReturnRows(reviewRows())

	got, err := postgres.NewSettlementBatchRepo(pool).ListOpenReversalReviews(context.Background(), 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("ListOpenReversalReviews() = %#v, %v, want empty, nil", got, err)
	}
}

func TestLockReversalReviewTx_LocksBatchBeforeReview(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	pool.ExpectQuery("SELECT batch_id::text").WithArgs("tx-1").
		WillReturnRows(pgxmock.NewRows([]string{"batch"}).AddRow("b-1"))
	pool.ExpectExec("FROM settlement.settlement_batches WHERE id = .* FOR UPDATE").WithArgs("b-1").
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	pool.ExpectQuery("FOR UPDATE OF r").WithArgs("tx-1").
		WillReturnRows(reviewRows().AddRow("tx-1", "b-1", repository.ReversalReviewPurchaseNotFound, "m-1", int64(1000), "ARS", (*string)(nil), (*int64)(nil), (*string)(nil), "OPEN", 0, false, time.Now(), ""))

	got, err := repo.LockReversalReviewTx(context.Background(), tx, "tx-1")
	if err != nil || got == nil || got.BatchID != "b-1" {
		t.Errorf("LockReversalReviewTx() = %+v, %v", got, err)
	}
}

func TestLockReversalReviewTx_NotFound(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	pool.ExpectQuery("SELECT batch_id::text").WithArgs("tx-1").WillReturnError(pgx.ErrNoRows)

	got, err := repo.LockReversalReviewTx(context.Background(), tx, "tx-1")
	if err != nil || got != nil {
		t.Errorf("LockReversalReviewTx() = %+v, %v, want nil, nil", got, err)
	}
}

// Corregir una reversa debe avanzar la versión: si no, quien cargó el lote antes la restaura.
func TestCorrectReversal_BumpsBatchVersion(t *testing.T) {
	cases := map[string]struct {
		expect string
		args   []any
		call   func(*postgres.SettlementBatchRepo, pgx.Tx) error
	}{
		"update": {
			expect: "UPDATE settlement.batch_transactions", args: []any{"b-1", "tx-1", int64(900)},
			call: func(r *postgres.SettlementBatchRepo, tx pgx.Tx) error {
				return r.UpdateReversalAmountTx(context.Background(), tx, "b-1", "tx-1", 900)
			},
		},
		"delete": {
			expect: "DELETE FROM settlement.batch_transactions", args: []any{"b-1", "tx-1"},
			call: func(r *postgres.SettlementBatchRepo, tx pgx.Tx) error {
				return r.DeleteReversalTx(context.Background(), tx, "b-1", "tx-1")
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pool := newMockPool(t)
			repo := postgres.NewSettlementBatchRepo(pool)
			tx := reviewTx(t, pool)

			pool.ExpectExec(tc.expect).WithArgs(tc.args...).WillReturnResult(pgxmock.NewResult("X", 1))
			pool.ExpectExec("SET version = version \\+ 1").WithArgs("b-1").WillReturnResult(pgxmock.NewResult("UPDATE", 1))

			if err := tc.call(repo, tx); err != nil {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCorrectReversal_MissingRowIsAnError(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	tx := reviewTx(t, pool)

	pool.ExpectExec("DELETE FROM settlement.batch_transactions").WithArgs("b-1", "tx-1").WillReturnResult(pgxmock.NewResult("DELETE", 0))

	if err := repo.DeleteReversalTx(context.Background(), tx, "b-1", "tx-1"); err == nil {
		t.Fatal("DeleteReversalTx() error = nil, want an error")
	}
}

func TestMerchantTimezone(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectQuery("FROM settlement.merchant_timezones").WithArgs("m-1").
		WillReturnRows(pgxmock.NewRows([]string{"timezone"}).AddRow("Asia/Tokyo"))
	pool.ExpectQuery("FROM settlement.merchant_timezones").WithArgs("m-2").WillReturnError(pgx.ErrNoRows)

	if got, err := repo.MerchantTimezone(context.Background(), "m-1"); err != nil || got != "Asia/Tokyo" {
		t.Errorf("MerchantTimezone(m-1) = %q, %v", got, err)
	}
	if got, err := repo.MerchantTimezone(context.Background(), "m-2"); err != nil || got != "" {
		t.Errorf("MerchantTimezone(m-2) = %q, %v, want empty", got, err)
	}
}

func TestSetMerchantTimezone_UnknownToDatabase(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectQuery("FROM pg_timezone_names").WithArgs("Mars/Base").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
	pool.ExpectRollback()

	_, err := repo.SetMerchantTimezone(context.Background(), "m-1", "Mars/Base", "UTC", "op-1")
	if !errors.Is(err, repository.ErrUnknownTimezone) {
		t.Fatalf("error = %v, want ErrUnknownTimezone", err)
	}
}

// Mueve el lote de hoy, guarda la zona y audita el movimiento, en una transacción.
func TestSetMerchantTimezone_RedatesTodaysOpenBatch(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectQuery("FROM pg_timezone_names").WithArgs("Asia/Tokyo").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	pool.ExpectQuery("UPDATE settlement.settlement_batches b").
		WithArgs("m-1", "Asia/Tokyo", "America/Argentina/Buenos_Aires").
		WillReturnRows(pgxmock.NewRows([]string{"id", "old_date", "batch_date", "discrepancies"}).
			AddRow("b-1", "2026-11-08", "2026-11-09", 0))
	pool.ExpectExec("INSERT INTO settlement.merchant_timezones").WithArgs("m-1", "Asia/Tokyo", "op-1").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectExec("INSERT INTO settlement.batch_audit_log").
		WithArgs("b-1", "REDATE", "op-1", "merchant timezone changed to Asia/Tokyo: batch_date 2026-11-08 → 2026-11-09",
			"OPEN", "OPEN", 0, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	moved, err := repo.SetMerchantTimezone(context.Background(), "m-1", "Asia/Tokyo", "America/Argentina/Buenos_Aires", "op-1")
	if err != nil {
		t.Fatalf("SetMerchantTimezone() error = %v", err)
	}
	want := repository.RedatedBatch{BatchID: "b-1", FromDate: "2026-11-08", ToDate: "2026-11-09"}
	if len(moved) != 1 || moved[0] != want {
		t.Errorf("moved = %+v, want [%+v]", moved, want)
	}
}

// Si un terminal ya tiene un lote OPEN en la fecha nueva, el índice único rechaza el movimiento
// y no cambia nada: ni los lotes ni la zona.
func TestSetMerchantTimezone_OpenBatchConflict(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)

	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectQuery("FROM pg_timezone_names").WithArgs("Asia/Tokyo").
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
	pool.ExpectQuery("UPDATE settlement.settlement_batches b").
		WithArgs("m-1", "Asia/Tokyo", "UTC").
		WillReturnError(&pgconn.PgError{Code: "23505", ConstraintName: "uq_settlement_batches_open"})
	pool.ExpectRollback()

	_, err := repo.SetMerchantTimezone(context.Background(), "m-1", "Asia/Tokyo", "UTC", "op-1")
	if !errors.Is(err, repository.ErrOpenBatchConflict) {
		t.Fatalf("error = %v, want ErrOpenBatchConflict", err)
	}
}

func TestClosedTotalsForTerminalDay(t *testing.T) {
	pool := newMockPool(t)
	repo := postgres.NewSettlementBatchRepo(pool)
	terminal := domain.NewTerminalID()
	date := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)

	pool.ExpectQuery("state IN \\('CLOSED', 'SUBMITTED', 'SETTLED'\\)").
		WithArgs(terminal.String(), "2026-11-05", "ARS", "b-1").
		WillReturnRows(pgxmock.NewRows([]string{"count", "amount"}).AddRow(3, int64(4500)))

	count, amount, err := repo.ClosedTotalsForTerminalDay(context.Background(), terminal, date, "ARS", "b-1")
	if err != nil || count != 3 || amount != 4500 {
		t.Errorf("ClosedTotalsForTerminalDay() = %d, %d, %v", count, amount, err)
	}
}

func TestMissingTables(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("to_regclass").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"t"}).AddRow("settlement.outbox").AddRow("settlement.merchant_timezones"))

	got, err := postgres.NewSettlementBatchRepo(pool).MissingTables(context.Background())
	if err != nil {
		t.Fatalf("MissingTables() error = %v", err)
	}
	if len(got) != 2 || got[0] != "settlement.outbox" {
		t.Errorf("missing = %v", got)
	}
}

func TestMissingTables_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("to_regclass").WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).WillReturnError(errors.New("db down"))

	if _, err := postgres.NewSettlementBatchRepo(pool).MissingTables(context.Background()); err == nil {
		t.Fatal("MissingTables() error = nil, want error")
	}
}

// La lista debe cubrir toda tabla que crea el schema: si no, una migración olvidada no se detecta.
func TestRequiredTables_MatchSchema(t *testing.T) {
	schema, err := os.ReadFile("st_schema.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	created := map[string]bool{}
	for _, m := range regexp.MustCompile(`CREATE TABLE (?:IF NOT EXISTS )?(settlement\.\w+)`).FindAllSubmatch(schema, -1) {
		created[string(m[1])] = true
	}
	if len(created) == 0 {
		t.Fatal("no tables found in st_schema.sql")
	}
	required := map[string]bool{}
	for _, table := range postgres.RequiredTables() {
		required[table] = true
		if !created[table] {
			t.Errorf("%s is required but not in st_schema.sql", table)
		}
	}
	for table := range created {
		if !required[table] {
			t.Errorf("%s is in st_schema.sql but /readyz does not check it", table)
		}
	}
}

// Toda columna requerida debe estar en st_schema.sql, para que el schema y /readyz no se
// separen.
func TestRequiredColumns_MatchSchema(t *testing.T) {
	schema, err := os.ReadFile("st_schema.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	for _, col := range postgres.RequiredColumns() {
		parts := strings.Split(col, ".")
		table := regexp.MustCompile(`(?s)CREATE TABLE (?:IF NOT EXISTS )?` + regexp.QuoteMeta(parts[0]+"."+parts[1]) + ` \((.*?)\n\);`).FindSubmatch(schema)
		if table == nil || !regexp.MustCompile(`(?m)^\s+`+parts[2]+`\s`).Match(table[1]) {
			t.Errorf("%s is required but not in st_schema.sql", col)
		}
	}
}

func TestWasReopened(t *testing.T) {
	for _, want := range []bool{true, false} {
		pool := newMockPool(t)
		pool.ExpectQuery("SELECT EXISTS \\(SELECT 1 FROM settlement.batch_audit_log").
			WithArgs("batch-1", "REOPEN").
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(want))

		got, err := postgres.NewSettlementBatchRepo(pool).WasReopened(context.Background(), "batch-1")
		if err != nil || got != want {
			t.Errorf("WasReopened() = %v, %v, want %v, nil", got, err, want)
		}
	}
}

// La referencia de conciliación se lee de la base: sin ella, un lote retenido no se libera.
func TestFindByID_ReadsTheReconciliationReference(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()
	with, count, amount := "TERMINAL", 2, int64(1500)
	row.reconciledWith, row.terminalCount, row.terminalAmount = &with, &count, &amount

	pool.ExpectQuery("WHERE id =").WithArgs(row.id).WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").WithArgs(row.id).WillReturnRows(emptyBatchTxRows())

	b, err := postgres.NewSettlementBatchRepo(pool).FindByID(context.Background(), row.id)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	gotCount, gotAmount := b.TerminalTotals()
	if b.ReconciledWith() != "TERMINAL" || gotCount == nil || *gotCount != 2 || gotAmount == nil || *gotAmount != 1500 {
		t.Errorf("reference = %q, %v, %v, want TERMINAL, 2, 1500", b.ReconciledWith(), gotCount, gotAmount)
	}
}

func TestRecordMaintenanceRun(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO settlement.maintenance_runs").
		WithArgs("reversal-recovery", "op-1", "3 reversas registradas").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	if err := postgres.NewSettlementBatchRepo(pool).RecordMaintenanceRun(context.Background(), "reversal-recovery", "op-1", "3 reversas registradas"); err != nil {
		t.Fatalf("RecordMaintenanceRun() error = %v", err)
	}
}
