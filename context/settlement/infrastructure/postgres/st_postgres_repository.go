// Package postgres contiene el adaptador PostgreSQL del BC Settlement.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// pgSerializationFailure es el SQLSTATE de un conflicto en una transacción SERIALIZABLE.
const pgSerializationFailure = "40001"

// pgxPool es el subconjunto de *pgxpool.Pool que este repositorio necesita:
// pgutil.PgxPool (BeginTx/Ping, usado por WithReadCommitted/WithSerializable)
// más QueryRow/Query, usados directamente en las consultas de lectura.
type pgxPool interface {
	pgutil.PgxPool
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// SettlementBatchRepo implementa repository.SettlementBatchRepository.
type SettlementBatchRepo struct{ pool pgxPool }

func NewSettlementBatchRepo(pool pgxPool) *SettlementBatchRepo {
	return &SettlementBatchRepo{pool: pool}
}

// Save persiste el batch y las transacciones agregadas desde que se cargó, en una sola transacción.
//
// Usa bloqueo optimista: solo actualiza si la versión de la base es la que se cargó.
// Sin esto, una venta concurrente con un cierre devolvía a OPEN el lote cerrado, o el
// cierre calculaba totales sin esa venta.
func (r *SettlementBatchRepo) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	return pgutil.WithReadCommitted(ctx, r.pool, func(tx pgx.Tx) error {
		return r.SaveTx(ctx, tx, b)
	})
}

// SaveTx hace lo mismo que Save dentro de una transacción existente.
func (r *SettlementBatchRepo) SaveTx(ctx context.Context, tx pgx.Tx, b *aggregate.SettlementBatch) error {
	const upsertBatch = `
		INSERT INTO settlement.settlement_batches (
			id, terminal_id, merchant_id, batch_date, state, currency,
			total_count, total_amount, purchase_count, purchase_amount,
			reversal_count, reversal_amount, discrepancies,
			created_at, closed_at, submitted_at, settled_at, version,
			reconciled_with, terminal_count, terminal_amount,
			adjustment_count, adjustment_amount
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,NULLIF($19,''),$20,$21,$22,$23)
		ON CONFLICT (id) DO UPDATE SET
			state           = EXCLUDED.state,
			total_count     = EXCLUDED.total_count,
			total_amount    = EXCLUDED.total_amount,
			purchase_count  = EXCLUDED.purchase_count,
			purchase_amount = EXCLUDED.purchase_amount,
			reversal_count  = EXCLUDED.reversal_count,
			reversal_amount = EXCLUDED.reversal_amount,
			discrepancies   = EXCLUDED.discrepancies,
			closed_at       = EXCLUDED.closed_at,
			submitted_at    = EXCLUDED.submitted_at,
			settled_at      = EXCLUDED.settled_at,
			version         = EXCLUDED.version,
			reconciled_with = EXCLUDED.reconciled_with,
			terminal_count  = EXCLUDED.terminal_count,
			terminal_amount = EXCLUDED.terminal_amount,
			adjustment_count  = EXCLUDED.adjustment_count,
			adjustment_amount = EXCLUDED.adjustment_amount
		WHERE settlement.settlement_batches.version = EXCLUDED.version - 1
	`

	var (
		totalCount, purchaseCount, reversalCount    *int
		totalAmount, purchaseAmount, reversalAmount *int64
	)

	terminalCount, terminalAmount := b.TerminalTotals()
	var (
		adjustmentCount  *int
		adjustmentAmount *int64
	)
	if s := b.Summary(); s != nil {
		ac, aa := s.AdjustmentCount(), s.AdjustmentAmount()
		adjustmentCount, adjustmentAmount = &ac, &aa
		tc := s.TotalCount()
		pc := s.PurchaseCount()
		rc := s.ReversalCount()
		ta := s.TotalAmount().Cents()
		pa := s.PurchaseAmount().Cents()
		ra := s.ReversalAmount().Cents()
		totalCount = &tc
		purchaseCount = &pc
		reversalCount = &rc
		totalAmount = &ta
		purchaseAmount = &pa
		reversalAmount = &ra
	}

	tag, err := tx.Exec(ctx, upsertBatch,
		b.ID(), b.TerminalID().String(), b.MerchantID().String(),
		b.BatchDate(), b.State().String(), b.Currency(),
		totalCount, totalAmount,
		purchaseCount, purchaseAmount,
		reversalCount, reversalAmount,
		b.Discrepancies(),
		b.CreatedAt(), b.ClosedAt(), b.SubmittedAt(), b.SettledAt(), b.Version()+1,
		b.ReconciledWith(), terminalCount, terminalAmount,
		adjustmentCount, adjustmentAmount,
	)
	if err != nil {
		return fmt.Errorf("SettlementBatchRepo.Save: upsert batch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("SettlementBatchRepo.Save: batch %s: %w", b.ID(), repository.ErrStaleBatch)
	}

	// Solo las transacciones agregadas desde la carga: las demás ya están. ON CONFLICT descarta
	// una transacción que ya está en otro lote.
	const insertTx = `
		INSERT INTO settlement.batch_transactions
			(id, batch_id, transaction_id, amount_cents, currency, tx_type, included_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (transaction_id, tx_type) DO NOTHING
	`
	for _, t := range b.NewTransactions() {
		if _, err := tx.Exec(ctx, insertTx,
			t.ID(), t.BatchID(), t.TransactionID().String(),
			t.AmountCents(), t.Currency(), t.Type().String(), t.IncludedAt(),
		); err != nil {
			return fmt.Errorf("SettlementBatchRepo.Save: insert batch_transaction: %w", err)
		}
	}

	b.MarkPersisted()
	return nil
}

// requiredTables son las tablas sin las que el servicio no puede operar. Una tabla nueva
// se agrega acá: las migraciones se aplican a mano, y olvidarlas no da otro aviso.
var requiredTables = []string{
	"settlement.settlement_batches",
	"settlement.batch_transactions",
	"settlement.processed_events",
	"settlement.outbox",
	"settlement.batch_audit_log",
	"settlement.reversal_reviews",
	"settlement.merchant_timezones",
	"settlement.maintenance_runs",
}

// requiredColumns son las columnas agregadas por migraciones sobre tablas existentes, como
// "schema.tabla.columna". Sin ellas, cada guardado de un lote falla.
var requiredColumns = []string{
	"settlement.settlement_batches.reconciled_with",
	"settlement.settlement_batches.terminal_count",
	"settlement.settlement_batches.terminal_amount",
	"settlement.reversal_reviews.action",
	"settlement.settlement_batches.adjustment_count",
	"settlement.settlement_batches.adjustment_amount",
}

// MissingTables devuelve las tablas y columnas requeridas que no existen en la base.
func (r *SettlementBatchRepo) MissingTables(ctx context.Context) ([]string, error) {
	const q = `
		SELECT t FROM unnest($1::text[]) AS t WHERE to_regclass(t) IS NULL
		UNION ALL
		SELECT c FROM unnest($2::text[]) AS c
		WHERE NOT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = split_part(c, '.', 1) AND table_name = split_part(c, '.', 2)
			  AND column_name = split_part(c, '.', 3))
		ORDER BY 1`
	rows, err := r.pool.Query(ctx, q, requiredTables, requiredColumns)
	if err != nil {
		return nil, fmt.Errorf("MissingTables: query: %w", err)
	}
	defer rows.Close()

	var missing []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("MissingTables: scan: %w", err)
		}
		missing = append(missing, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MissingTables: rows: %w", err)
	}
	return missing, nil
}

// WasReopened informa si el lote tiene un reopen en la auditoría.
func (r *SettlementBatchRepo) WasReopened(ctx context.Context, batchID string) (bool, error) {
	var reopened bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM settlement.batch_audit_log WHERE batch_id = $1 AND action = $2)`,
		batchID, repository.AuditActionReopen).Scan(&reopened)
	if err != nil {
		return false, fmt.Errorf("WasReopened: %w", err)
	}
	return reopened, nil
}

// InsertAuditTx guarda un registro de auditoría dentro de tx.
func (r *SettlementBatchRepo) InsertAuditTx(ctx context.Context, tx pgx.Tx, e repository.BatchAuditEntry) error {
	const q = `
		INSERT INTO settlement.batch_audit_log (
			batch_id, action, operator_id, reason, from_state, to_state,
			previous_discrepancies, total_amount, confirmation_id
		) VALUES ($1,$2,$3,NULLIF($4,''),$5,$6,$7,$8,NULLIF($9,''))
	`
	if _, err := tx.Exec(ctx, q,
		e.BatchID, e.Action, e.OperatorID, e.Reason, e.FromState, e.ToState,
		e.PreviousDiscrepancies, e.TotalAmount, e.ConfirmationID,
	); err != nil {
		return fmt.Errorf("InsertAuditTx: %w", err)
	}
	return nil
}

// lockReversalPair serializa la compra y la reversa de una misma transacción. Sin esto,
// si se procesan a la vez, ninguna ve a la otra y la revisión queda abierta como
// PURCHASE_NOT_FOUND. Debe llamarse después de guardar el batch: la consulta siguiente,
// en READ COMMITTED, ve lo que confirmó la otra.
func lockReversalPair(ctx context.Context, tx pgx.Tx, transactionID string) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "settlement-reversal:"+transactionID); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	return nil
}

// FindPurchaseTx busca la compra de una transacción, en cualquier lote.
func (r *SettlementBatchRepo) FindPurchaseTx(ctx context.Context, tx pgx.Tx, transactionID string) (*repository.ReversalPurchase, error) {
	if err := lockReversalPair(ctx, tx, transactionID); err != nil {
		return nil, fmt.Errorf("FindPurchaseTx: %w", err)
	}
	const q = `
		SELECT b.merchant_id::text, t.amount_cents, t.currency
		FROM settlement.batch_transactions t
		JOIN settlement.settlement_batches b ON b.id = t.batch_id
		WHERE t.transaction_id = $1 AND t.tx_type = 'PURCHASE'
	`
	var p repository.ReversalPurchase
	err := tx.QueryRow(ctx, q, transactionID).Scan(&p.MerchantID, &p.AmountCents, &p.Currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FindPurchaseTx: %w", err)
	}
	return &p, nil
}

// InsertReversalReviewTx abre la revisión. Si la transacción ya tiene una, no hace nada.
func (r *SettlementBatchRepo) InsertReversalReviewTx(ctx context.Context, tx pgx.Tx, v repository.ReversalReview) error {
	const q = `
		INSERT INTO settlement.reversal_reviews (
			transaction_id, batch_id, reason, merchant_id, reversal_amount, reversal_currency,
			purchase_merchant_id, purchase_amount, purchase_currency
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (transaction_id) DO NOTHING
	`
	var (
		purchaseMerchant, purchaseCurrency *string
		purchaseAmount                     *int64
	)
	if v.Purchase != nil {
		purchaseMerchant, purchaseAmount, purchaseCurrency = &v.Purchase.MerchantID, &v.Purchase.AmountCents, &v.Purchase.Currency
	}
	if _, err := tx.Exec(ctx, q,
		v.TransactionID, v.BatchID, v.Reason, v.MerchantID, v.AmountCents, v.Currency,
		purchaseMerchant, purchaseAmount, purchaseCurrency,
	); err != nil {
		return fmt.Errorf("InsertReversalReviewTx: %w", err)
	}
	return nil
}

// ReconcileReversalReviewTx compara la compra recién llegada con la reversa que la esperaba, o
// con la reversa que un operador excluyó sin tener la compra.
func (r *SettlementBatchRepo) ReconcileReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID, purchaseBatchID string, p repository.ReversalPurchase) (repository.ReviewReconciliation, error) {
	if err := lockReversalPair(ctx, tx, transactionID); err != nil {
		return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: %w", err)
	}
	const q = `
		WITH checked AS (
			SELECT transaction_id,
			       CASE WHEN merchant_id <> $2::uuid THEN 'MERCHANT_MISMATCH'
			            WHEN reversal_currency <> $4 THEN 'CURRENCY_MISMATCH'
			            WHEN reversal_amount <> $3 THEN 'AMOUNT_MISMATCH'
			       END AS mismatch
			FROM settlement.reversal_reviews
			WHERE transaction_id = $1 AND reason = 'PURCHASE_NOT_FOUND' AND resolved_at IS NULL
		)
		UPDATE settlement.reversal_reviews r
		SET purchase_merchant_id = $2::uuid, purchase_amount = $3, purchase_currency = $4,
		    reason      = COALESCE(c.mismatch, r.reason),
		    resolved_at = CASE WHEN c.mismatch IS NULL THEN now() END,
		    resolved_by = CASE WHEN c.mismatch IS NULL THEN 'system' END,
		    resolution  = CASE WHEN c.mismatch IS NULL THEN 'La compra llegó después y coincide' END
		FROM checked c
		WHERE r.transaction_id = c.transaction_id
		RETURNING r.reason, r.resolved_at IS NOT NULL, r.batch_id::text
	`
	var out repository.ReviewReconciliation
	err := tx.QueryRow(ctx, q, transactionID, p.MerchantID, p.AmountCents, p.Currency).Scan(&out.Reason, &out.Resolved, &out.BatchID)
	if err == nil {
		if out.Resolved {
			out.Reason = ""
		}
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: %w", err)
	}
	return r.reconcileExcludedReversalTx(ctx, tx, transactionID, purchaseBatchID, p)
}

// reconcileExcludedReversalTx trata la compra de una reversa que un operador excluyó porque la
// compra no estaba. Si coinciden, la reversa vuelve, en el lote de la compra: sin ella, la venta
// sumaría como si no se hubiera anulado. Si no coinciden, la revisión se reabre en ese lote.
func (r *SettlementBatchRepo) reconcileExcludedReversalTx(ctx context.Context, tx pgx.Tx, transactionID, purchaseBatchID string, p repository.ReversalPurchase) (repository.ReviewReconciliation, error) {
	var (
		merchantID, currency string
		amount               int64
	)
	err := tx.QueryRow(ctx, `
		SELECT merchant_id::text, reversal_amount, reversal_currency
		FROM settlement.reversal_reviews
		WHERE transaction_id = $1 AND reason = 'PURCHASE_NOT_FOUND' AND action = $2 AND resolved_at IS NOT NULL
		FOR UPDATE`, transactionID, repository.ReviewActionExclude).Scan(&merchantID, &amount, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return repository.ReviewReconciliation{}, nil
	}
	if err != nil {
		return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: find excluded reversal: %w", err)
	}

	var mismatch string
	switch {
	case merchantID != p.MerchantID:
		mismatch = repository.ReversalReviewMerchantMismatch
	case currency != p.Currency:
		mismatch = repository.ReversalReviewCurrencyMismatch
	case amount != p.AmountCents:
		mismatch = repository.ReversalReviewAmountMismatch
	}

	if mismatch != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE settlement.reversal_reviews
			SET batch_id = $2, reason = $3, purchase_merchant_id = $4::uuid, purchase_amount = $5, purchase_currency = $6,
			    resolved_at = NULL, resolved_by = NULL, resolution = NULL
			WHERE transaction_id = $1`,
			transactionID, purchaseBatchID, mismatch, p.MerchantID, p.AmountCents, p.Currency); err != nil {
			return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: reopen review: %w", err)
		}
		return repository.ReviewReconciliation{Reason: mismatch, Reopened: true, BatchID: purchaseBatchID}, nil
	}

	if err := r.InsertReversalTx(ctx, tx, purchaseBatchID, transactionID, amount, currency); err != nil {
		return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE settlement.reversal_reviews
		SET batch_id = $2, purchase_merchant_id = $3::uuid, purchase_amount = $4, purchase_currency = $5,
		    resolved_at = now(), resolved_by = 'system', action = $6,
		    resolution = 'La compra llegó después y coincide: la reversa excluida se restituyó'
		WHERE transaction_id = $1`,
		transactionID, purchaseBatchID, p.MerchantID, p.AmountCents, p.Currency, repository.ReviewActionRestore); err != nil {
		return repository.ReviewReconciliation{}, fmt.Errorf("ReconcileReversalReviewTx: close review: %w", err)
	}
	return repository.ReviewReconciliation{Restored: true, BatchID: purchaseBatchID}, nil
}

// InsertReversalTx vuelve a agregar una reversa excluida a un lote.
func (r *SettlementBatchRepo) InsertReversalTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string, amountCents int64, currency string) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO settlement.batch_transactions (id, batch_id, transaction_id, amount_cents, currency, tx_type, included_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, 'REVERSAL', now())
		ON CONFLICT (transaction_id, tx_type) DO NOTHING`,
		batchID, transactionID, amountCents, currency)
	if err != nil {
		return fmt.Errorf("InsertReversalTx: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("InsertReversalTx: reversal %s is already in a batch", transactionID)
	}
	return bumpBatchVersion(ctx, tx, batchID)
}

// CountOpenReversalReviews cuenta las revisiones abiertas de las reversas de un lote.
func (r *SettlementBatchRepo) CountOpenReversalReviews(ctx context.Context, batchID string) (int, error) {
	const q = `SELECT COUNT(*) FROM settlement.reversal_reviews WHERE batch_id = $1 AND resolved_at IS NULL`
	var n int
	if err := r.pool.QueryRow(ctx, q, batchID).Scan(&n); err != nil {
		return 0, fmt.Errorf("CountOpenReversalReviews: %w", err)
	}
	return n, nil
}

// FindLateSaleBatchTx busca el lote en disputa al que le falta una venta que llegó tarde. El
// total_count guardado es el del último cálculo del lote; AddLateSale lo vuelve a comprobar.
func (r *SettlementBatchRepo) FindLateSaleBatchTx(ctx context.Context, tx pgx.Tx, terminalID domain.TerminalID, date time.Time, currency string, authorizedAt time.Time) (string, error) {
	const q = `
		SELECT id::text FROM settlement.settlement_batches
		WHERE terminal_id = $1 AND batch_date = $2::date AND currency = $3
		  AND state = 'DISPUTED' AND reconciled_with = 'TERMINAL'
		  AND closed_at >= $4 AND terminal_count > COALESCE(total_count, 0)
		ORDER BY closed_at ASC
		LIMIT 1
	`
	var id string
	err := tx.QueryRow(ctx, q, terminalID.String(), date.Format("2006-01-02"), currency, authorizedAt).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("FindLateSaleBatchTx: %w", err)
	}
	return id, nil
}

// CountTerminalDayBatches cuenta los lotes del terminal en esa fecha y moneda.
func (r *SettlementBatchRepo) CountTerminalDayBatches(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency string) (int, error) {
	const q = `
		SELECT COUNT(*) FROM settlement.settlement_batches
		WHERE terminal_id = $1 AND batch_date = $2::date AND currency = $3
	`
	var n int
	if err := r.pool.QueryRow(ctx, q, terminalID.String(), date.Format("2006-01-02"), currency).Scan(&n); err != nil {
		return 0, fmt.Errorf("CountTerminalDayBatches: %w", err)
	}
	return n, nil
}

// FindReleasableHeldBatchIDs lista los lotes retenidos cuyas revisiones ya se cerraron. Un lote
// que se volvió a conciliar y no coincide ya tiene discrepancias: no vuelve a aparecer.
func (r *SettlementBatchRepo) FindReleasableHeldBatchIDs(ctx context.Context) ([]string, error) {
	const q = `
		SELECT b.id::text FROM settlement.settlement_batches b
		WHERE b.state = 'DISPUTED' AND b.discrepancies = 0 AND b.reconciled_with IS NOT NULL
		  AND EXISTS (SELECT 1 FROM settlement.reversal_reviews r WHERE r.batch_id = b.id)
		  AND NOT EXISTS (SELECT 1 FROM settlement.reversal_reviews r WHERE r.batch_id = b.id AND r.resolved_at IS NULL)
		ORDER BY b.batch_date ASC
	`
	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("FindReleasableHeldBatchIDs: query: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("FindReleasableHeldBatchIDs: %w", err)
	}
	return ids, nil
}

// reversalReviewColumns son las columnas que lee scanReversalReview. Requiere los alias
// r (reversal_reviews) y b (settlement_batches).
const reversalReviewColumns = `
	r.transaction_id::text, r.batch_id::text, r.reason, r.merchant_id::text,
	r.reversal_amount, r.reversal_currency,
	r.purchase_merchant_id::text, r.purchase_amount, r.purchase_currency,
	b.state, b.discrepancies, r.resolved_at IS NOT NULL, r.created_at, COALESCE(r.action, '')`

func scanReversalReview(row scanner) (*repository.StoredReversalReview, error) {
	var (
		v                                  repository.StoredReversalReview
		purchaseMerchant, purchaseCurrency *string
		purchaseAmount                     *int64
	)
	if err := row.Scan(
		&v.TransactionID, &v.BatchID, &v.Reason, &v.MerchantID,
		&v.AmountCents, &v.Currency,
		&purchaseMerchant, &purchaseAmount, &purchaseCurrency,
		&v.BatchState, &v.Discrepancies, &v.Resolved, &v.CreatedAt, &v.Action,
	); err != nil {
		return nil, err
	}
	if purchaseMerchant != nil && purchaseAmount != nil && purchaseCurrency != nil {
		v.Purchase = &repository.ReversalPurchase{
			MerchantID: *purchaseMerchant, AmountCents: *purchaseAmount, Currency: *purchaseCurrency,
		}
	}
	return &v, nil
}

// ListOpenReversalReviews lista las revisiones abiertas, de la más antigua a la más nueva.
func (r *SettlementBatchRepo) ListOpenReversalReviews(ctx context.Context, limit int) ([]repository.StoredReversalReview, error) {
	q := `SELECT ` + reversalReviewColumns + `
		FROM settlement.reversal_reviews r
		JOIN settlement.settlement_batches b ON b.id = r.batch_id
		WHERE r.resolved_at IS NULL
		ORDER BY r.created_at
		LIMIT $1`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("ListOpenReversalReviews: query: %w", err)
	}
	defer rows.Close()

	reviews := []repository.StoredReversalReview{}
	for rows.Next() {
		v, err := scanReversalReview(rows)
		if err != nil {
			return nil, fmt.Errorf("ListOpenReversalReviews: scan: %w", err)
		}
		reviews = append(reviews, *v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListOpenReversalReviews: rows: %w", err)
	}
	return reviews, nil
}

// LockReversalReviewTx bloquea primero el lote y después la revisión, el mismo orden en que
// lo hace quien registra una compra: bloquear al revés podría generar un deadlock.
func (r *SettlementBatchRepo) LockReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID string) (*repository.StoredReversalReview, error) {
	var batchID string
	err := tx.QueryRow(ctx, `SELECT batch_id::text FROM settlement.reversal_reviews WHERE transaction_id = $1`, transactionID).Scan(&batchID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("LockReversalReviewTx: find batch: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM settlement.settlement_batches WHERE id = $1 FOR UPDATE`, batchID); err != nil {
		return nil, fmt.Errorf("LockReversalReviewTx: lock batch: %w", err)
	}
	q := `SELECT ` + reversalReviewColumns + `
		FROM settlement.reversal_reviews r
		JOIN settlement.settlement_batches b ON b.id = r.batch_id
		WHERE r.transaction_id = $1
		FOR UPDATE OF r`
	v, err := scanReversalReview(tx.QueryRow(ctx, q, transactionID))
	if err != nil {
		return nil, fmt.Errorf("LockReversalReviewTx: %w", err)
	}
	return v, nil
}

// bumpBatchVersion hace que quien cargó el lote antes de un cambio en sus transacciones
// reciba ErrStaleBatch. Si no, su guardado volvería a insertar una reversa excluida.
func bumpBatchVersion(ctx context.Context, tx pgx.Tx, batchID string) error {
	if _, err := tx.Exec(ctx, `UPDATE settlement.settlement_batches SET version = version + 1 WHERE id = $1`, batchID); err != nil {
		return fmt.Errorf("bump batch version: %w", err)
	}
	return nil
}

// UpdateReversalAmountTx cambia el monto de la reversa en su lote.
func (r *SettlementBatchRepo) UpdateReversalAmountTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string, amountCents int64) error {
	const q = `
		UPDATE settlement.batch_transactions SET amount_cents = $3
		WHERE batch_id = $1 AND transaction_id = $2 AND tx_type = 'REVERSAL'
	`
	tag, err := tx.Exec(ctx, q, batchID, transactionID, amountCents)
	if err != nil {
		return fmt.Errorf("UpdateReversalAmountTx: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("UpdateReversalAmountTx: reversal %s not found in batch %s", transactionID, batchID)
	}
	return bumpBatchVersion(ctx, tx, batchID)
}

// DeleteReversalTx quita la reversa de su lote.
func (r *SettlementBatchRepo) DeleteReversalTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string) error {
	const q = `
		DELETE FROM settlement.batch_transactions
		WHERE batch_id = $1 AND transaction_id = $2 AND tx_type = 'REVERSAL'
	`
	tag, err := tx.Exec(ctx, q, batchID, transactionID)
	if err != nil {
		return fmt.Errorf("DeleteReversalTx: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("DeleteReversalTx: reversal %s not found in batch %s", transactionID, batchID)
	}
	return bumpBatchVersion(ctx, tx, batchID)
}

// CloseReversalReviewTx cierra la revisión con el operador, la resolución y la acción indicados.
// También corrige una ya cerrada: quien llama decide si corresponde, con la revisión bloqueada.
func (r *SettlementBatchRepo) CloseReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID, operatorID, resolution, action string) error {
	const q = `
		UPDATE settlement.reversal_reviews
		SET resolved_at = now(), resolved_by = $2, resolution = $3, action = $4
		WHERE transaction_id = $1
	`
	if _, err := tx.Exec(ctx, q, transactionID, operatorID, resolution, action); err != nil {
		return fmt.Errorf("CloseReversalReviewTx: %w", err)
	}
	return nil
}

// MerchantTimezone devuelve la zona propia del comercio, o vacío si no tiene.
func (r *SettlementBatchRepo) MerchantTimezone(ctx context.Context, merchantID string) (string, error) {
	var tz string
	err := r.pool.QueryRow(ctx, `SELECT timezone FROM settlement.merchant_timezones WHERE merchant_id = $1`, merchantID).Scan(&tz)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("MerchantTimezone: %w", err)
	}
	return tz, nil
}

// merchantTimezoneLock es la clave del lock entre las ventas y reversas de un comercio, que lo
// toman compartido para leer la zona, y el cambio de zona, que lo toma exclusivo.
const merchantTimezoneLock = `hashtextextended('settlement-merchant-timezone:' || $1::text, 0)`

// MerchantTimezoneTx toma el lock compartido del comercio y lee su zona. El lock dura hasta el
// fin de tx.
func (r *SettlementBatchRepo) MerchantTimezoneTx(ctx context.Context, tx pgx.Tx, merchantID string) (string, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(`+merchantTimezoneLock+`)`, merchantID); err != nil {
		return "", fmt.Errorf("MerchantTimezoneTx: lock: %w", err)
	}
	var tz string
	err := tx.QueryRow(ctx, `SELECT timezone FROM settlement.merchant_timezones WHERE merchant_id = $1`, merchantID).Scan(&tz)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("MerchantTimezoneTx: %w", err)
	}
	return tz, nil
}

// SetMerchantTimezone guarda la zona, solo si Postgres la reconoce, y mueve los lotes OPEN del
// día a la fecha de la zona nueva. Todo en una transacción: si un lote choca con otro OPEN, no
// cambia nada. Antes espera el lock exclusivo del comercio: las ventas y reversas que ya leyeron
// la zona anterior terminan de guardarse, y sus lotes se mueven con los demás.
func (r *SettlementBatchRepo) SetMerchantTimezone(ctx context.Context, merchantID, timezone, defaultTimezone, operatorID string) ([]repository.RedatedBatch, error) {
	// La zona anterior es la propia del comercio o, si no tenía, la general. Solo se mueve el lote
	// de hoy: los de días anteriores los cierra el job. now() es el inicio de la transacción.
	const redateQ = `
		WITH target AS (
			SELECT b.id, b.batch_date AS old_date, b.discrepancies
			FROM settlement.settlement_batches b
			WHERE b.merchant_id = $1::uuid AND b.state = 'OPEN'
			  AND b.batch_date = (now() AT TIME ZONE COALESCE(
			        (SELECT timezone FROM settlement.merchant_timezones WHERE merchant_id = $1::uuid), $3::text))::date
			  AND b.batch_date <> (now() AT TIME ZONE $2::text)::date
			FOR UPDATE OF b
		)
		UPDATE settlement.settlement_batches b
		SET batch_date = (now() AT TIME ZONE $2::text)::date, version = b.version + 1
		FROM target t
		WHERE b.id = t.id
		RETURNING b.id::text, t.old_date::text, b.batch_date::text, t.discrepancies
	`
	const upsertQ = `
		INSERT INTO settlement.merchant_timezones (merchant_id, timezone, updated_by)
		VALUES ($1::uuid, $2::text, $3::text)
		ON CONFLICT (merchant_id) DO UPDATE
		SET timezone = EXCLUDED.timezone, updated_by = EXCLUDED.updated_by, updated_at = now()
	`
	var moved []repository.RedatedBatch
	err := pgutil.WithReadCommitted(ctx, r.pool, func(tx pgx.Tx) error {
		moved = nil
		// Antes de usarla en una consulta: con una zona desconocida, AT TIME ZONE falla.
		var known bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_timezone_names WHERE name = $1)`, timezone).Scan(&known); err != nil {
			return fmt.Errorf("SetMerchantTimezone: check timezone: %w", err)
		}
		if !known {
			return repository.ErrUnknownTimezone
		}
		// Cada consulta posterior ve lo que guardaron las ventas que tenían el lock.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(`+merchantTimezoneLock+`)`, merchantID); err != nil {
			return fmt.Errorf("SetMerchantTimezone: lock: %w", err)
		}

		rows, err := tx.Query(ctx, redateQ, merchantID, timezone, defaultTimezone)
		if err != nil {
			return redateError(err)
		}
		var discrepancies []int
		for rows.Next() {
			var (
				b repository.RedatedBatch
				d int
			)
			if err := rows.Scan(&b.BatchID, &b.FromDate, &b.ToDate, &d); err != nil {
				rows.Close()
				return fmt.Errorf("SetMerchantTimezone: scan: %w", err)
			}
			moved, discrepancies = append(moved, b), append(discrepancies, d)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return redateError(err)
		}

		if _, err := tx.Exec(ctx, upsertQ, merchantID, timezone, operatorID); err != nil {
			return fmt.Errorf("SetMerchantTimezone: %w", err)
		}
		for i, b := range moved {
			if err := r.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
				BatchID: b.BatchID, Action: repository.AuditActionRedate, OperatorID: operatorID,
				Reason:    fmt.Sprintf("merchant timezone changed to %s: batch_date %s → %s", timezone, b.FromDate, b.ToDate),
				FromState: "OPEN", ToState: "OPEN", PreviousDiscrepancies: discrepancies[i],
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return moved, nil
}

// redateError traduce el choque con el índice de un solo lote OPEN por terminal, fecha y moneda.
func redateError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return repository.ErrOpenBatchConflict
	}
	return fmt.Errorf("SetMerchantTimezone: redate open batches: %w", err)
}

// ClosedTotalsForTerminalDay suma los totales de los lotes ya cerrados del terminal en la fecha,
// sin los ajustes: se comparan con lo que informa el terminal, que no los conoce.
func (r *SettlementBatchRepo) ClosedTotalsForTerminalDay(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency, excludeBatchID string) (int, int64, error) {
	const q = `
		SELECT COALESCE(SUM(total_count), 0)::bigint,
		       COALESCE(SUM(total_amount - COALESCE(adjustment_amount, 0)), 0)::bigint
		FROM settlement.settlement_batches
		WHERE terminal_id = $1 AND batch_date = $2::date AND currency = $3 AND id <> $4
		  AND state IN ('CLOSED', 'SUBMITTED', 'SETTLED')
	`
	var (
		count  int
		amount int64
	)
	err := r.pool.QueryRow(ctx, q, terminalID.String(), date.Format("2006-01-02"), currency, excludeBatchID).Scan(&count, &amount)
	if err != nil {
		return 0, 0, fmt.Errorf("ClosedTotalsForTerminalDay: %w", err)
	}
	return count, amount, nil
}

// MerchantDayBatchesTx lista los batches con transacciones del comercio en la fecha.
//
// El advisory lock serializa a quienes liquidan el mismo comercio y fecha. Sin él, los
// dos últimos lotes liquidados en paralelo no se verían entre sí, y nadie publicaría
// SettlementCompleted. Se toma después de guardar el batch: la lectura incluye ese
// cambio, sin confirmar, y lo que confirmó la transacción anterior.
func (r *SettlementBatchRepo) MerchantDayBatchesTx(ctx context.Context, tx pgx.Tx, merchantID domain.MerchantID, date time.Time) ([]repository.MerchantDayBatch, error) {
	day := date.Format("2006-01-02")
	lockKey := "settlement-completed:" + merchantID.String() + ":" + day
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return nil, fmt.Errorf("MerchantDayBatchesTx: lock: %w", err)
	}

	const q = `
		SELECT b.currency, b.state = 'SETTLED', COALESCE(b.total_amount, 0), COALESCE(b.adjustment_amount, 0)
		FROM settlement.settlement_batches b
		WHERE b.merchant_id = $1 AND b.batch_date = $2::date
		  AND EXISTS (SELECT 1 FROM settlement.batch_transactions t WHERE t.batch_id = b.id)
	`
	rows, err := tx.Query(ctx, q, merchantID.String(), day)
	if err != nil {
		return nil, fmt.Errorf("MerchantDayBatchesTx: query: %w", err)
	}
	defer rows.Close()

	var batches []repository.MerchantDayBatch
	for rows.Next() {
		var d repository.MerchantDayBatch
		if err := rows.Scan(&d.Currency, &d.Settled, &d.TotalAmount, &d.AdjustmentAmount); err != nil {
			return nil, fmt.Errorf("MerchantDayBatchesTx: scan: %w", err)
		}
		batches = append(batches, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("MerchantDayBatchesTx: rows: %w", err)
	}
	return batches, nil
}

const findBatchByIDQuery = `
	SELECT id, terminal_id, merchant_id, batch_date, state, currency,
	       total_count, total_amount, purchase_count, purchase_amount,
	       reversal_count, reversal_amount, discrepancies,
	       created_at, closed_at, submitted_at, settled_at, version,
	       reconciled_with, terminal_count, terminal_amount,
	       adjustment_count, adjustment_amount
	FROM settlement.settlement_batches WHERE id = $1
`

// findForAppend recupera un batch sin sus transacciones, para agregarle una: cargarlas haría
// crecer el costo de cada venta con el tamaño del lote. El lote no puede calcular totales.
func (r *SettlementBatchRepo) findForAppend(ctx context.Context, id string) (*aggregate.SettlementBatch, error) {
	batch, err := scanBatch(r.pool.QueryRow(ctx, findBatchByIDQuery, id))
	if err != nil {
		return nil, err
	}
	params := toBatchParams(batch, nil)
	params.TransactionsNotLoaded = true
	return aggregate.Reconstitute(params), nil
}

// FindByID recupera un batch completo con sus transacciones.
func (r *SettlementBatchRepo) FindByID(ctx context.Context, id string) (*aggregate.SettlementBatch, error) {
	batch, err := scanBatch(r.pool.QueryRow(ctx, findBatchByIDQuery, id))
	if err != nil {
		return nil, err
	}

	txs, err := r.loadTransactions(ctx, id)
	if err != nil {
		return nil, err
	}

	return aggregate.Reconstitute(toBatchParams(batch, txs)), nil
}

// FindOpenByTerminal recupera el batch OPEN de un terminal en una fecha y moneda.
func (r *SettlementBatchRepo) FindOpenByTerminal(
	ctx context.Context,
	terminalID domain.TerminalID,
	date time.Time,
	currency string,
) (*aggregate.SettlementBatch, error) {
	const q = `
		SELECT id, terminal_id, merchant_id, batch_date, state, currency,
		       total_count, total_amount, purchase_count, purchase_amount,
		       reversal_count, reversal_amount, discrepancies,
		       created_at, closed_at, submitted_at, settled_at, version,
		       reconciled_with, terminal_count, terminal_amount,
		       adjustment_count, adjustment_amount
		FROM settlement.settlement_batches
		WHERE terminal_id = $1
		  AND batch_date = $2::date
		  AND currency = $3
		  AND state = 'OPEN'
		LIMIT 1
	`
	row := r.pool.QueryRow(ctx, q, terminalID.String(), date.Format("2006-01-02"), currency)
	batch, err := scanBatch(row)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, err
	}

	txs, err := r.loadTransactions(ctx, batch.id)
	if err != nil {
		return nil, err
	}

	return aggregate.Reconstitute(toBatchParams(batch, txs)), nil
}

// FindOrCreateOpen recupera o crea el batch OPEN del terminal para ese día, sin sus
// transacciones: es para agregarle una. Usa SERIALIZABLE para evitar condiciones de carrera.
func (r *SettlementBatchRepo) FindOrCreateOpen(
	ctx context.Context,
	terminalID domain.TerminalID,
	merchantID domain.MerchantID,
	date time.Time,
	currency string,
) (*aggregate.SettlementBatch, error) {
	var result *aggregate.SettlementBatch

	err := pgutil.WithSerializable(ctx, r.pool, func(tx pgx.Tx) error {
		// Intentar encontrar uno existente primero
		const findQ = `
			SELECT id FROM settlement.settlement_batches
			WHERE terminal_id = $1 AND batch_date = $2::date AND currency = $3 AND state = 'OPEN'
			FOR UPDATE
		`
		var existingID string
		err := tx.QueryRow(ctx, findQ, terminalID.String(), date.Format("2006-01-02"), currency).Scan(&existingID)

		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("FindOrCreateOpen: check existing: %w", err)
		}

		if existingID != "" {
			batch, err := r.findForAppend(ctx, existingID)
			if err != nil {
				return err
			}
			result = batch
			return nil
		}

		// No existe — crear uno nuevo
		newBatch, err := aggregate.NewSettlementBatch(terminalID, merchantID, date, currency)
		if err != nil {
			return fmt.Errorf("FindOrCreateOpen: create batch: %w", err)
		}

		const insertQ = `
			INSERT INTO settlement.settlement_batches
				(id, terminal_id, merchant_id, batch_date, state, currency, discrepancies, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (terminal_id, batch_date, currency) WHERE state = 'OPEN' DO NOTHING
		`
		tag, err := tx.Exec(ctx, insertQ,
			newBatch.ID(), terminalID.String(), merchantID.String(),
			date.Format("2006-01-02"), "OPEN", currency, 0, newBatch.CreatedAt(),
		)
		if err != nil {
			return fmt.Errorf("FindOrCreateOpen: insert batch: %w", err)
		}

		if tag.RowsAffected() == 0 {
			// Conflicto — otro proceso creó el batch concurrentemente
			// Reintentar la búsqueda
			var concurrentID string
			if err := tx.QueryRow(ctx, findQ,
				terminalID.String(), date.Format("2006-01-02"), currency,
			).Scan(&concurrentID); err != nil {
				return fmt.Errorf("FindOrCreateOpen: reload after conflict: %w", err)
			}
			batch, err := r.findForAppend(ctx, concurrentID)
			if err != nil {
				return err
			}
			result = batch
			return nil
		}

		result = newBatch
		return nil
	})

	// Una falla de serialización es el mismo conflicto que una versión vieja.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgSerializationFailure {
		return nil, fmt.Errorf("FindOrCreateOpen: %w: %w", repository.ErrStaleBatch, err)
	}
	return result, err
}

// ListByMerchantDate lista todos los batches de un comercio en una fecha.
func (r *SettlementBatchRepo) ListByMerchantDate(
	ctx context.Context,
	merchantID domain.MerchantID,
	date time.Time,
) ([]*aggregate.SettlementBatch, error) {
	const q = `
		SELECT id, terminal_id, merchant_id, batch_date, state, currency,
		       total_count, total_amount, purchase_count, purchase_amount,
		       reversal_count, reversal_amount, discrepancies,
		       created_at, closed_at, submitted_at, settled_at, version,
		       reconciled_with, terminal_count, terminal_amount,
		       adjustment_count, adjustment_amount
		FROM settlement.settlement_batches
		WHERE merchant_id = $1 AND batch_date = $2::date
		ORDER BY created_at ASC
	`
	rows, err := r.pool.Query(ctx, q, merchantID.String(), date.Format("2006-01-02"))
	if err != nil {
		return nil, fmt.Errorf("ListByMerchantDate: query: %w", err)
	}
	defer rows.Close()

	var batches []*aggregate.SettlementBatch
	for rows.Next() {
		raw, err := scanBatch(rows)
		if err != nil {
			return nil, err
		}
		txs, err := r.loadTransactions(ctx, raw.id)
		if err != nil {
			return nil, err
		}
		batches = append(batches, aggregate.Reconstitute(toBatchParams(raw, txs)))
	}

	return batches, nil
}

// CountBatchesNeedingAttention cuenta los lotes en disputa, los CLOSED con transacciones
// sin enviar desde antes de closedBefore, los OPEN de días de negocio anteriores al de now y
// los eventos en el outbox desde antes de outboxBefore y las revisiones de reversas abiertas
// desde antes de reviewBefore. Los lotes vacíos en CLOSED no cuentan: no hay nada que enviar.
func (r *SettlementBatchRepo) CountBatchesNeedingAttention(ctx context.Context, now time.Time, defaultTimezone string, closedBefore, outboxBefore, reviewBefore time.Time) (repository.BatchAttentionCounts, error) {
	const q = `
		SELECT
			COUNT(*) FILTER (WHERE b.state = 'DISPUTED'),
			COUNT(*) FILTER (WHERE b.state = 'CLOSED' AND (COALESCE(b.total_count, 0) > 0 OR COALESCE(b.adjustment_count, 0) > 0) AND b.closed_at < $1),
			COUNT(*) FILTER (WHERE b.state = 'OPEN' AND b.batch_date < ($2::timestamptz AT TIME ZONE COALESCE(m.timezone, $3))::date),
			(SELECT COUNT(*) FROM settlement.outbox WHERE created_at < $4),
			(SELECT COUNT(*) FROM settlement.reversal_reviews WHERE resolved_at IS NULL AND created_at < $5),
			NOT EXISTS (SELECT 1 FROM settlement.maintenance_runs WHERE name = $6)
		FROM settlement.settlement_batches b
		LEFT JOIN settlement.merchant_timezones m ON m.merchant_id = b.merchant_id
		WHERE b.state IN ('DISPUTED', 'CLOSED', 'OPEN')
	`
	var c repository.BatchAttentionCounts
	err := r.pool.QueryRow(ctx, q, closedBefore, now, defaultTimezone, outboxBefore, reviewBefore, repository.MaintenanceReversalRecovery).
		Scan(&c.Disputed, &c.ClosedUnsent, &c.OpenOverdue, &c.OutboxStuck, &c.ReversalsInReview, &c.RecoveryPending)
	if err != nil {
		return repository.BatchAttentionCounts{}, fmt.Errorf("CountBatchesNeedingAttention: %w", err)
	}
	return c, nil
}

// FindStaleOpenIDs lista los batches OPEN de días de negocio anteriores al de now.
func (r *SettlementBatchRepo) FindStaleOpenIDs(ctx context.Context, now time.Time, defaultTimezone string) ([]string, error) {
	const q = `
		SELECT b.id FROM settlement.settlement_batches b
		LEFT JOIN settlement.merchant_timezones m ON m.merchant_id = b.merchant_id
		WHERE b.state = 'OPEN'
		  AND b.batch_date < ($1::timestamptz AT TIME ZONE COALESCE(m.timezone, $2))::date
		ORDER BY b.batch_date ASC
	`
	rows, err := r.pool.Query(ctx, q, now, defaultTimezone)
	if err != nil {
		return nil, fmt.Errorf("FindStaleOpenIDs: query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("FindStaleOpenIDs: scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("FindStaleOpenIDs: rows: %w", err)
	}
	return ids, nil
}

// ─── Helpers internos ─────────────────────────────────────────────────────────

// rawBatch almacena la fila escaneada antes de reconstruir el aggregate.
type rawBatch struct {
	id, terminalID, merchantID                  string
	batchDate                                   time.Time
	state, currency                             string
	totalCount, purchaseCount, reversalCount    *int
	totalAmount, purchaseAmount, reversalAmount *int64
	discrepancies                               int
	createdAt                                   time.Time
	closedAt, submittedAt, settledAt            *time.Time
	version                                     int
	reconciledWith                              *string
	terminalCount                               *int
	terminalAmount                              *int64
	adjustmentCount                             *int
	adjustmentAmount                            *int64
}

type scanner interface {
	Scan(dest ...any) error
}

func scanBatch(row scanner) (*rawBatch, error) {
	var b rawBatch
	err := row.Scan(
		&b.id, &b.terminalID, &b.merchantID, &b.batchDate,
		&b.state, &b.currency,
		&b.totalCount, &b.totalAmount,
		&b.purchaseCount, &b.purchaseAmount,
		&b.reversalCount, &b.reversalAmount,
		&b.discrepancies,
		&b.createdAt, &b.closedAt, &b.submittedAt, &b.settledAt, &b.version,
		&b.reconciledWith, &b.terminalCount, &b.terminalAmount,
		&b.adjustmentCount, &b.adjustmentAmount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pkgerrors.NewNotFoundError("SettlementBatch", "")
		}
		return nil, fmt.Errorf("scanBatch: %w", err)
	}
	return &b, nil
}

func (r *SettlementBatchRepo) loadTransactions(ctx context.Context, batchID string) ([]*entity.BatchTransaction, error) {
	const q = `
		SELECT id, batch_id, transaction_id, amount_cents, currency, tx_type, included_at
		FROM settlement.batch_transactions WHERE batch_id = $1
		ORDER BY included_at ASC
	`
	rows, err := r.pool.Query(ctx, q, batchID)
	if err != nil {
		return nil, fmt.Errorf("loadTransactions: %w", err)
	}
	defer rows.Close()

	var txs []*entity.BatchTransaction
	for rows.Next() {
		var id, bid, txID, currency, txType string
		var amountCents int64
		var includedAt time.Time

		if err := rows.Scan(&id, &bid, &txID, &amountCents, &currency, &txType, &includedAt); err != nil {
			return nil, fmt.Errorf("loadTransactions: scan: %w", err)
		}

		parsedTxID, _ := domain.ParseTransactionID(txID)
		// Un tipo desconocido no entra en ningún total: cargar el lote sin él lo cerraría con
		// totales equivocados.
		parsedType, err := valueobject.ParseBatchTransactionType(txType)
		if err != nil {
			return nil, fmt.Errorf("loadTransactions: transaction %s: %w", id, err)
		}

		txs = append(txs, entity.ReconstituteBatchTransaction(id, bid, parsedTxID, amountCents, currency, parsedType, includedAt))
	}
	// Sin esto, un corte a mitad de la lectura devuelve el lote incompleto, y sus totales mal.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loadTransactions: %w", err)
	}
	return txs, nil
}

func toBatchParams(b *rawBatch, txs []*entity.BatchTransaction) aggregate.ReconstituteParams {
	tid, _ := domain.ParseTerminalID(b.terminalID)
	mid, _ := domain.ParseMerchantID(b.merchantID)
	state, _ := valueobject.ParseBatchState(b.state)

	params := aggregate.ReconstituteParams{
		ID:            b.id,
		TerminalID:    tid,
		MerchantID:    mid,
		BatchDate:     b.batchDate,
		State:         state,
		Currency:      b.currency,
		Transactions:  txs,
		Discrepancies: b.discrepancies,
		Version:       b.version,
		CreatedAt:     b.createdAt,
		ClosedAt:      b.closedAt,
		SubmittedAt:   b.submittedAt,
		SettledAt:     b.settledAt,

		TerminalCount:  b.terminalCount,
		TerminalAmount: b.terminalAmount,
	}
	if b.reconciledWith != nil {
		params.ReconciledWith = *b.reconciledWith
	}

	if b.totalCount != nil && b.totalAmount != nil {
		cur, _ := domain.ParseCurrency(b.currency)
		totalMoney, _ := domain.NewMoney(*b.totalAmount, cur)
		purchaseMoney, _ := domain.NewMoney(derefInt64(b.purchaseAmount), cur)
		reversalMoney, _ := domain.NewMoney(derefInt64(b.reversalAmount), cur)
		summary, err := valueobject.NewBatchSummary(
			*b.totalCount, totalMoney,
			derefInt(b.purchaseCount), purchaseMoney,
			derefInt(b.reversalCount), reversalMoney,
		)
		if err == nil {
			summary = summary.WithAdjustments(derefInt(b.adjustmentCount), derefInt64(b.adjustmentAmount))
			params.Summary = &summary
		}
	}

	return params
}

func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
