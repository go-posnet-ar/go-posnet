package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// UnrecordedReversal es una venta revertida en Authorization, sin su reversa en Settlement.
type UnrecordedReversal struct {
	TransactionID string
	TerminalID    string
	MerchantID    string
	AmountCents   int64
	Currency      string
	AuthorizedAt  time.Time
	BatchID       string // Lote de la compra
	BatchState    string
}

// FindUnrecordedReversals lista las ventas revertidas sin fila REVERSAL. Es la única consulta
// de Settlement que lee otro schema, y solo la usa el comando de recuperación: el servicio
// no depende de que tenga permiso.
func (r *SettlementBatchRepo) FindUnrecordedReversals(ctx context.Context, limit int) ([]UnrecordedReversal, error) {
	const q = `
		SELECT p.transaction_id::text, t.terminal_id::text, t.merchant_id::text,
		       t.amount_cents, t.currency, COALESCE(t.authorized_at, t.created_at),
		       p.batch_id::text, b.state
		FROM settlement.batch_transactions p
		JOIN settlement.settlement_batches b ON b.id = p.batch_id
		JOIN pn_authorization.transactions t ON t.id = p.transaction_id
		LEFT JOIN settlement.batch_transactions rv
		       ON rv.transaction_id = p.transaction_id AND rv.tx_type = 'REVERSAL'
		WHERE t.state = 'REVERSED' AND p.tx_type = 'PURCHASE' AND rv.transaction_id IS NULL
		ORDER BY b.batch_date, p.transaction_id
		LIMIT $1
	`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("FindUnrecordedReversals: query: %w", err)
	}
	defer rows.Close()

	var found []UnrecordedReversal
	for rows.Next() {
		var u UnrecordedReversal
		if err := rows.Scan(&u.TransactionID, &u.TerminalID, &u.MerchantID, &u.AmountCents, &u.Currency, &u.AuthorizedAt, &u.BatchID, &u.BatchState); err != nil {
			return nil, fmt.Errorf("FindUnrecordedReversals: scan: %w", err)
		}
		found = append(found, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("FindUnrecordedReversals: rows: %w", err)
	}
	return found, nil
}

// RecordMaintenanceRun registra que una tarea de mantenimiento quedó hecha en este entorno. Si ya
// estaba, actualiza quién y cuándo.
func (r *SettlementBatchRepo) RecordMaintenanceRun(ctx context.Context, name, completedBy, details string) error {
	return pgutil.WithReadCommitted(ctx, r.pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO settlement.maintenance_runs (name, completed_by, details) VALUES ($1, $2, NULLIF($3, ''))
			ON CONFLICT (name) DO UPDATE
			SET completed_at = now(), completed_by = EXCLUDED.completed_by, details = EXCLUDED.details`,
			name, completedBy, details)
		if err != nil {
			return fmt.Errorf("RecordMaintenanceRun: %w", err)
		}
		return nil
	})
}

// FindReversalBatchTx devuelve el lote en el que quedó registrada una reversa y su estado.
func (r *SettlementBatchRepo) FindReversalBatchTx(ctx context.Context, tx pgx.Tx, transactionID string) (batchID, state string, err error) {
	const q = `
		SELECT b.id::text, b.state
		FROM settlement.batch_transactions t
		JOIN settlement.settlement_batches b ON b.id = t.batch_id
		WHERE t.transaction_id = $1 AND t.tx_type = 'REVERSAL'
	`
	err = tx.QueryRow(ctx, q, transactionID).Scan(&batchID, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("FindReversalBatchTx: reversal %s not found", transactionID)
	}
	if err != nil {
		return "", "", fmt.Errorf("FindReversalBatchTx: %w", err)
	}
	return batchID, state, nil
}
