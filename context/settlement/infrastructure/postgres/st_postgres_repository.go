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

// Save persiste el batch y sus transacciones nuevas en una sola transacción.
//
// Usa bloqueo optimista: solo actualiza si la versión de la base es la que se cargó.
// Sin esto, una venta concurrente con un cierre devolvía a OPEN el lote cerrado, o el
// cierre calculaba totales sin esa venta.
func (r *SettlementBatchRepo) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	err := pgutil.WithReadCommitted(ctx, r.pool, func(tx pgx.Tx) error {
		const upsertBatch = `
			INSERT INTO settlement.settlement_batches (
				id, terminal_id, merchant_id, batch_date, state, currency,
				total_count, total_amount, purchase_count, purchase_amount,
				reversal_count, reversal_amount, discrepancies,
				created_at, closed_at, submitted_at, settled_at, version
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
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
				version         = EXCLUDED.version
			WHERE settlement.settlement_batches.version = EXCLUDED.version - 1
		`

		var (
			totalCount, purchaseCount, reversalCount    *int
			totalAmount, purchaseAmount, reversalAmount *int64
		)

		if s := b.Summary(); s != nil {
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
		)
		if err != nil {
			return fmt.Errorf("SettlementBatchRepo.Save: upsert batch: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("SettlementBatchRepo.Save: batch %s: %w", b.ID(), repository.ErrStaleBatch)
		}

		// Insertar solo las transacciones nuevas (ON CONFLICT DO NOTHING por UNIQUE)
		const insertTx = `
			INSERT INTO settlement.batch_transactions
				(id, batch_id, transaction_id, amount_cents, currency, tx_type, included_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (transaction_id, tx_type) DO NOTHING
		`
		for _, t := range b.Transactions() {
			if _, err := tx.Exec(ctx, insertTx,
				t.ID(), t.BatchID(), t.TransactionID().String(),
				t.AmountCents(), t.Currency(), t.Type().String(), t.IncludedAt(),
			); err != nil {
				return fmt.Errorf("SettlementBatchRepo.Save: insert batch_transaction: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}
	b.MarkPersisted()
	return nil
}

// FindByID recupera un batch completo con sus transacciones.
func (r *SettlementBatchRepo) FindByID(ctx context.Context, id string) (*aggregate.SettlementBatch, error) {
	const q = `
		SELECT id, terminal_id, merchant_id, batch_date, state, currency,
		       total_count, total_amount, purchase_count, purchase_amount,
		       reversal_count, reversal_amount, discrepancies,
		       created_at, closed_at, submitted_at, settled_at, version
		FROM settlement.settlement_batches WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, q, id)
	batch, err := scanBatch(row)
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
		       created_at, closed_at, submitted_at, settled_at, version
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

// FindOrCreateOpen recupera o crea el batch OPEN del terminal para ese día.
// Usa SERIALIZABLE para evitar condiciones de carrera.
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
			// Ya existe — cargar completo
			batch, err := r.FindByID(ctx, existingID)
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
			batch, err := r.FindByID(ctx, concurrentID)
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
		       created_at, closed_at, submitted_at, settled_at, version
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

// FindStaleOpenIDs lista los batches OPEN de fechas anteriores a before.
func (r *SettlementBatchRepo) FindStaleOpenIDs(ctx context.Context, before time.Time) ([]string, error) {
	const q = `
		SELECT id FROM settlement.settlement_batches
		WHERE state = 'OPEN' AND batch_date < $1::date
		ORDER BY batch_date ASC
	`
	rows, err := r.pool.Query(ctx, q, before.Format("2006-01-02"))
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
		parsedType, _ := valueobject.ParseBatchTransactionType(txType)

		txs = append(txs, entity.ReconstituteBatchTransaction(id, bid, parsedTxID, amountCents, currency, parsedType, includedAt))
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
