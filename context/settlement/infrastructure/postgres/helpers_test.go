package postgres_test

import (
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// newMockPool crea un pool pgxmock y registra su cierre y la verificación de
// expectations al finalizar el test.
func newMockPool(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := pool.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet pgxmock expectations: %v", err)
		}
	})
	return pool
}

// anyArgs devuelve n comodines pgxmock.AnyArg().
func anyArgs(n int) []any {
	args := make([]any, n)
	for i := range args {
		args[i] = pgxmock.AnyArg()
	}
	return args
}

// ─── builders de dominio ──────────────────────────────────────────────────────

func newOpenBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(
		domain.NewTerminalID(), domain.NewMerchantID(),
		time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), "ARS",
	)
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}

func newClosedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return b
}

// ─── fixtures de filas de Postgres ────────────────────────────────────────────

var batchColumns = []string{
	"id", "terminal_id", "merchant_id", "batch_date", "state", "currency",
	"total_count", "total_amount", "purchase_count", "purchase_amount",
	"reversal_count", "reversal_amount", "discrepancies",
	"created_at", "closed_at", "submitted_at", "settled_at", "version",
}

var batchTxColumns = []string{
	"id", "batch_id", "transaction_id", "amount_cents", "currency", "tx_type", "included_at",
}

// batchRow arma una fila de settlement.settlement_batches. Los tipos deben
// calzar exactamente con los destinos de Scan en rawBatch: pgxmock no
// convierte tipos como pgx real.
type batchRow struct {
	id, terminalID, merchantID string
	batchDate                  time.Time
	state, currency            string

	totalCount, purchaseCount, reversalCount    *int
	totalAmount, purchaseAmount, reversalAmount *int64

	discrepancies int
	createdAt     time.Time
	closedAt      *time.Time
	submittedAt   *time.Time
	settledAt     *time.Time
	version       int
}

func newBatchRow() batchRow {
	return batchRow{
		id:         "batch-1",
		terminalID: domain.NewTerminalID().String(),
		merchantID: domain.NewMerchantID().String(),
		batchDate:  time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		state:      "OPEN",
		currency:   "ARS",
		createdAt:  time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
	}
}

// withSummary agrega los totales del summary (compra+reversión) a la fila,
// simulando un batch CLOSED persistido.
func (r batchRow) withSummary(totalCount, purchaseCount, reversalCount int, totalAmount, purchaseAmount, reversalAmount int64) batchRow {
	r.totalCount = &totalCount
	r.purchaseCount = &purchaseCount
	r.reversalCount = &reversalCount
	r.totalAmount = &totalAmount
	r.purchaseAmount = &purchaseAmount
	r.reversalAmount = &reversalAmount
	return r
}

func (r batchRow) rows() *pgxmock.Rows {
	return pgxmock.NewRows(batchColumns).AddRow(
		r.id, r.terminalID, r.merchantID, r.batchDate, r.state, r.currency,
		r.totalCount, r.totalAmount, r.purchaseCount, r.purchaseAmount,
		r.reversalCount, r.reversalAmount, r.discrepancies,
		r.createdAt, r.closedAt, r.submittedAt, r.settledAt, r.version,
	)
}

func emptyBatchTxRows() *pgxmock.Rows {
	return pgxmock.NewRows(batchTxColumns)
}

func oneBatchTxRow(batchID string) *pgxmock.Rows {
	return pgxmock.NewRows(batchTxColumns).AddRow(
		batchID+"-tx1", batchID, domain.NewTransactionID().String(), int64(1000), "ARS", "PURCHASE",
		time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
	)
}
