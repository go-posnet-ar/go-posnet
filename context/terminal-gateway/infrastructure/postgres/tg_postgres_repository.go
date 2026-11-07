// Package postgres contiene el adaptador PostgreSQL del BC Terminal Gateway.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/entity"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	valueobject "github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// pgxPool es el subconjunto de *pgxpool.Pool que estos repositorios necesitan.
type pgxPool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ─── PaymentSessionRepo ───────────────────────────────────────────────────────

type PaymentSessionRepo struct{ pool pgxPool }

func NewPaymentSessionRepo(pool pgxPool) *PaymentSessionRepo {
	return &PaymentSessionRepo{pool: pool}
}

func (r *PaymentSessionRepo) Save(ctx context.Context, s *aggregate.PaymentSession) error {
	const q = `
		INSERT INTO terminal_gateway.payment_sessions (
			id, terminal_id, merchant_id,
			state, channel,
			amount_cents, currency, stan,
			auth_code, rejection_code, rejection_reason,
			expires_at, created_at, closed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO UPDATE SET
			state            = EXCLUDED.state,
			auth_code        = EXCLUDED.auth_code,
			rejection_code   = EXCLUDED.rejection_code,
			rejection_reason = EXCLUDED.rejection_reason,
			closed_at        = EXCLUDED.closed_at
	`
	_, err := r.pool.Exec(ctx, q,
		s.ID().String(), s.TerminalID().String(), s.MerchantID().String(),
		s.State().String(), s.Channel().String(),
		s.Amount().Cents(), s.Amount().Currency().String(), s.STAN().Value(),
		nullStr(s.AuthCode()), nullStr(s.RejectionCode()), nullStr(s.RejectionReason()),
		s.ExpiresAt(), s.CreatedAt(), s.ClosedAt(),
	)
	if err != nil {
		return fmt.Errorf("PaymentSessionRepo.Save: %w", err)
	}
	return nil
}

// SaveTx persiste la sesión dentro de una transacción Postgres existente.
// Usar cuando la sesión debe guardarse atómicamente junto a otra operación (ej: outbox).
func (r *PaymentSessionRepo) SaveTx(ctx context.Context, tx pgx.Tx, s *aggregate.PaymentSession) error {
	const q = `
		INSERT INTO terminal_gateway.payment_sessions (
			id, terminal_id, merchant_id,
			state, channel,
			amount_cents, currency, stan,
			auth_code, rejection_code, rejection_reason,
			expires_at, created_at, closed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO UPDATE SET
			state            = EXCLUDED.state,
			auth_code        = EXCLUDED.auth_code,
			rejection_code   = EXCLUDED.rejection_code,
			rejection_reason = EXCLUDED.rejection_reason,
			closed_at        = EXCLUDED.closed_at
	`
	_, err := tx.Exec(ctx, q,
		s.ID().String(), s.TerminalID().String(), s.MerchantID().String(),
		s.State().String(), s.Channel().String(),
		s.Amount().Cents(), s.Amount().Currency().String(), s.STAN().Value(),
		nullStr(s.AuthCode()), nullStr(s.RejectionCode()), nullStr(s.RejectionReason()),
		s.ExpiresAt(), s.CreatedAt(), s.ClosedAt(),
	)
	if err != nil {
		return fmt.Errorf("PaymentSessionRepo.SaveTx: %w", err)
	}
	return nil
}

func (r *PaymentSessionRepo) FindByID(ctx context.Context, id domain.TransactionID) (*aggregate.PaymentSession, error) {
	const q = `
		SELECT id, terminal_id, merchant_id,
		       state, channel,
		       amount_cents, currency, stan,
		       auth_code, rejection_code, rejection_reason,
		       expires_at, created_at, closed_at
		FROM terminal_gateway.payment_sessions WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, q, id.String())
	return scanSession(row)
}

func (r *PaymentSessionRepo) FindActiveByTerminal(ctx context.Context, terminalID domain.TerminalID) (*aggregate.PaymentSession, error) {
	const q = `
		SELECT id, terminal_id, merchant_id,
		       state, channel,
		       amount_cents, currency, stan,
		       auth_code, rejection_code, rejection_reason,
		       expires_at, created_at, closed_at
		FROM terminal_gateway.payment_sessions
		WHERE terminal_id = $1
		  AND state IN ('AWAITING_PAYMENT', 'PROCESSING')
		LIMIT 1
	`
	row := r.pool.QueryRow(ctx, q, terminalID.String())
	s, err := scanSession(row)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, err
	}
	return s, nil
}

func (r *PaymentSessionRepo) ExpireAwaiting(ctx context.Context, limit int) ([]repository.ExpiredSession, error) {
	// SKIP LOCKED: con varias instancias, cada sesión la vence y la notifica una sola.
	const q = `
		UPDATE terminal_gateway.payment_sessions
		SET state = 'EXPIRED', closed_at = NOW()
		WHERE id IN (
			SELECT id FROM terminal_gateway.payment_sessions
			WHERE state = 'AWAITING_PAYMENT' AND expires_at < NOW()
			ORDER BY expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, terminal_id::text
	`
	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("PaymentSessionRepo.ExpireAwaiting: %w", err)
	}
	defer rows.Close()

	var expired []repository.ExpiredSession
	for rows.Next() {
		var id, terminalID string
		if err := rows.Scan(&id, &terminalID); err != nil {
			return nil, fmt.Errorf("PaymentSessionRepo.ExpireAwaiting: scan: %w", err)
		}
		txID, err := domain.ParseTransactionID(id)
		if err != nil {
			return nil, fmt.Errorf("PaymentSessionRepo.ExpireAwaiting: %w", err)
		}
		tID, err := domain.ParseTerminalID(terminalID)
		if err != nil {
			return nil, fmt.Errorf("PaymentSessionRepo.ExpireAwaiting: %w", err)
		}
		expired = append(expired, repository.ExpiredSession{TransactionID: txID, TerminalID: tID})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("PaymentSessionRepo.ExpireAwaiting: rows: %w", err)
	}
	return expired, nil
}

// CountProcessing retorna la cantidad de sesiones actualmente en PROCESSING.
// Alimenta el gauge posnet_sessions_processing_current.
func (r *PaymentSessionRepo) CountProcessing(ctx context.Context) (int64, error) {
	const q = `SELECT COUNT(*) FROM terminal_gateway.payment_sessions WHERE state = 'PROCESSING'`
	var n int64
	if err := r.pool.QueryRow(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("PaymentSessionRepo.CountProcessing: %w", err)
	}
	return n, nil
}

func scanSession(row pgx.Row) (*aggregate.PaymentSession, error) {
	var (
		id, terminalID, merchantID   string
		state, channel               string
		amountCents                  int64
		currency                     string
		stan                         int
		authCode, rejCode, rejReason *string
		expiresAt, createdAt         time.Time
		closedAt                     *time.Time
	)
	err := row.Scan(
		&id, &terminalID, &merchantID,
		&state, &channel,
		&amountCents, &currency, &stan,
		&authCode, &rejCode, &rejReason,
		&expiresAt, &createdAt, &closedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pkgerrors.NewNotFoundError("PaymentSession", "")
		}
		return nil, fmt.Errorf("scanSession: %w", err)
	}

	txID, _ := domain.ParseTransactionID(id)
	tID, _ := domain.ParseTerminalID(terminalID)
	mID, _ := domain.ParseMerchantID(merchantID)
	cur, _ := domain.ParseCurrency(currency)
	amount, _ := domain.NewMoney(amountCents, cur)
	stanVO, _ := domain.NewSTAN(stan)
	ch, _ := valueobject.ParsePaymentChannel(channel)

	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:              txID,
		TerminalID:      tID,
		MerchantID:      mID,
		Amount:          amount,
		STAN:            stanVO,
		Channel:         ch,
		State:           valueobject.SessionState(state),
		AuthCode:        derefStr(authCode),
		RejectionCode:   derefStr(rejCode),
		RejectionReason: derefStr(rejReason),
		ExpiresAt:       expiresAt,
		CreatedAt:       createdAt,
		ClosedAt:        closedAt,
	}), nil
}

// ─── TerminalRepo ─────────────────────────────────────────────────────────────

type TerminalRepo struct{ pool pgxPool }

func NewTerminalRepo(pool pgxPool) *TerminalRepo {
	return &TerminalRepo{pool: pool}
}

func (r *TerminalRepo) FindByID(ctx context.Context, id domain.TerminalID) (*entity.Terminal, error) {
	const q = `
		SELECT id, merchant_id, terminal_code, certificate_cn, status, created_at, updated_at
		FROM terminal_gateway.terminals WHERE id = $1
	`
	return scanTerminal(r.pool.QueryRow(ctx, q, id.String()))
}

func (r *TerminalRepo) FindByCertificateCN(ctx context.Context, cn string) (*entity.Terminal, error) {
	const q = `
		SELECT id, merchant_id, terminal_code, certificate_cn, status, created_at, updated_at
		FROM terminal_gateway.terminals WHERE certificate_cn = $1
	`
	return scanTerminal(r.pool.QueryRow(ctx, q, cn))
}

func (r *TerminalRepo) Save(ctx context.Context, t *entity.Terminal) error {
	const q = `
		INSERT INTO terminal_gateway.terminals (id, merchant_id, terminal_code, certificate_cn, status, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
	`
	_, err := r.pool.Exec(ctx, q,
		t.ID().String(), t.MerchantID().String(), t.TerminalCode(),
		t.CertificateCN(), string(t.Status()), t.CreatedAt(), t.UpdatedAt(),
	)
	return err
}

func scanTerminal(row pgx.Row) (*entity.Terminal, error) {
	var id, merchantID, code, cn, status string
	var createdAt, updatedAt time.Time
	if err := row.Scan(&id, &merchantID, &code, &cn, &status, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pkgerrors.NewNotFoundError("Terminal", "")
		}
		return nil, fmt.Errorf("scanTerminal: %w", err)
	}
	tid, _ := domain.ParseTerminalID(id)
	mid, _ := domain.ParseMerchantID(merchantID)
	return entity.ReconstitueTerminal(tid, mid, code, cn, entity.TerminalStatus(status), createdAt, updatedAt), nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
