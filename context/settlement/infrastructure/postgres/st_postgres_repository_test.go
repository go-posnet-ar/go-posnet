package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

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
		WithArgs(anyArgs(17)...).
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
		WithArgs(anyArgs(17)...).
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
		WithArgs(anyArgs(17)...).
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
		WithArgs(anyArgs(17)...).
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
		WithArgs(anyArgs(17)...).
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
		WithArgs(row.terminalID, "2026-01-15").
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnRows(emptyBatchTxRows())

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOpenByTerminal(context.Background(), terminalID, date)
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewSettlementBatchRepo(pool)
	b, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now())
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "scanBatch") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanBatch")
	}
}

func TestFindOpenByTerminal_LoadTransactionsError(t *testing.T) {
	pool := newMockPool(t)
	row := newBatchRow()

	pool.ExpectQuery("LIMIT 1").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(row.rows())
	pool.ExpectQuery("FROM settlement.batch_transactions").
		WithArgs(row.id).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewSettlementBatchRepo(pool)
	_, err := repo.FindOpenByTerminal(context.Background(), domain.NewTerminalID(), time.Now())
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0)) // conflicto — otro proceso ya insertó
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(pgx.ErrNoRows)
	pool.ExpectExec("INSERT INTO settlement.settlement_batches").
		WithArgs(anyArgs(8)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))
	pool.ExpectQuery("SELECT id FROM settlement.settlement_batches").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg()).
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
				row1.createdAt, row1.closedAt, row1.submittedAt, row1.settledAt).
			AddRow(row2.id, row2.terminalID, row2.merchantID, row2.batchDate, row2.state, row2.currency,
				row2.totalCount, row2.totalAmount, row2.purchaseCount, row2.purchaseAmount,
				row2.reversalCount, row2.reversalAmount, row2.discrepancies,
				row2.createdAt, row2.closedAt, row2.submittedAt, row2.settledAt))
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
	pool.ExpectQuery("state = 'OPEN' AND batch_date <").
		WithArgs("2026-01-15").
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("batch-1").AddRow("batch-2"))

	ids, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), time.Date(2026, 1, 15, 3, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("FindStaleOpenIDs() error = %v", err)
	}
	if len(ids) != 2 || ids[0] != "batch-1" || ids[1] != "batch-2" {
		t.Errorf("ids = %v, want [batch-1 batch-2]", ids)
	}
}

func TestFindStaleOpenIDs_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("state = 'OPEN' AND batch_date <").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	_, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "FindStaleOpenIDs: query") {
		t.Fatalf("error = %v, want it to mention FindStaleOpenIDs: query", err)
	}
}

func TestFindStaleOpenIDs_RowsError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("state = 'OPEN' AND batch_date <").
		WithArgs(pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow("batch-1").RowError(0, errors.New("broken")))

	if _, err := postgres.NewSettlementBatchRepo(pool).FindStaleOpenIDs(context.Background(), time.Now()); err == nil {
		t.Fatal("FindStaleOpenIDs() error = nil, want the rows error")
	}
}
