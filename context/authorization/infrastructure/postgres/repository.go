// Package postgres contiene el adaptador de PostgreSQL para el BC Authorization.
// Implementa las interfaces de repositorio definidas en domain/repository.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// pgxPool es el subconjunto de *pgxpool.Pool que este repositorio necesita.
// Permite testear las queries con un pool falso (ej: pgxmock) sin depender
// del tipo concreto de pgx.
type pgxPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// execer es lo que comparten el pool y una transacción para ejecutar un comando.
type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// TransactionRepo implementa repository.TransactionRepository usando pgx/v5.
// Usa sqlc para las queries (código generado en infrastructure/postgres/sqlc/).
// Este archivo contiene solo las queries complejas que sqlc no genera bien.
type TransactionRepo struct {
	pool pgxPool
}

// NewTransactionRepo construye el repositorio con el pool de conexiones.
func NewTransactionRepo(pool pgxPool) *TransactionRepo {
	return &TransactionRepo{pool: pool}
}

// InsertTx da de alta una Transaction dentro de dbTx, junto con la marca de idempotencia. Si ya
// existe ese ID, no cambia nada y devuelve false: las transiciones siguientes pasan por
// SaveIfState, así que un alta no puede devolver una transacción a un estado anterior.
func (r *TransactionRepo) InsertTx(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) (bool, error) {
	const query = `
		INSERT INTO pn_authorization.transactions (
			id, terminal_id, merchant_id,
			state, amount_cents, currency,
			pan_last4, card_network, entry_mode,
			stan, auth_code, rejection_code, rejection_source,
			fraud_score, fraud_decision, fraud_rules_hit,
			emv_data_b64, iso8583_raw,
			created_at, authorized_at, rejected_at,
			card_token
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7, $8, $9,
			$10, $11, $12, $13,
			$14, $15, $16,
			$17, $18,
			$19, $20, $21,
			$22
		)
		ON CONFLICT (id) DO NOTHING
	`

	var authCode *string
	if tx.AuthCode() != nil {
		s := tx.AuthCode().String()
		authCode = &s
	}

	var rejCode, rejSource *string
	if tx.RejectionCode() != nil {
		c := tx.RejectionCode().Code()
		s := string(tx.RejectionCode().Source())
		rejCode = &c
		rejSource = &s
	}

	var fraudScore *int
	var fraudDecision *string
	if !tx.FraudDecision().IsZero() {
		fs := tx.FraudDecision().Score
		fd := tx.FraudDecision().Decision
		fraudScore = &fs
		fraudDecision = &fd
	}
	rulesHit, err := fraudRulesJSON(tx)
	if err != nil {
		return false, fmt.Errorf("TransactionRepo.InsertTx: %w", err)
	}

	// El token va como NULL cuando el terminal no lo emite: así la columna
	// distingue "sin token" de "token vacío" y el índice único no agrupa
	// todas las transacciones sin token bajo una misma clave.
	var cardToken *string
	if !tx.CardToken().IsZero() {
		ct := tx.CardToken().String()
		cardToken = &ct
	}

	tag, err := dbTx.Exec(ctx, query,
		tx.ID().String(),
		tx.TerminalID().String(),
		tx.MerchantID().String(),
		tx.State().String(),
		tx.Amount().Cents(),
		tx.Amount().Currency().String(),
		tx.PAN().Last4(),
		string(tx.PAN().Network()),
		tx.EntryMode().String(),
		tx.STAN().Value(),
		authCode,
		rejCode,
		rejSource,
		fraudScore,
		fraudDecision,
		rulesHit,
		tx.EMVDataBase64(),
		tx.ISO8583Raw(),
		tx.ReceivedAt(),
		tx.AuthorizedAt(),
		tx.RejectedAt(),
		cardToken,
	)
	if err != nil {
		return false, fmt.Errorf("TransactionRepo.InsertTx: exec insert: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// SaveInvalidRejection registra el rechazo de una transacción con datos inválidos. Un reintento
// del mismo mensaje no lo duplica.
func (r *TransactionRepo) SaveInvalidRejectionTx(ctx context.Context, dbTx pgx.Tx, rejection valueobject.InvalidTransactionRejection) error {
	const query = `
		INSERT INTO pn_authorization.invalid_transactions (
			transaction_id, terminal_id, merchant_id,
			amount_cents, currency,
			rejection_code, validation_error, rejected_at
		) VALUES (
			$1, NULLIF($2, '')::uuid, NULLIF($3, '')::uuid,
			$4, NULLIF($5, ''),
			$6, $7, $8
		)
		ON CONFLICT (transaction_id) DO NOTHING
	`
	_, err := dbTx.Exec(ctx, query,
		rejection.TransactionID.String(), rejection.TerminalID, rejection.MerchantID,
		rejection.AmountCents, rejection.Currency,
		rejection.Rejection.Code(), rejection.ValidationError, rejection.RejectedAt,
	)
	if err != nil {
		return fmt.Errorf("TransactionRepo.SaveInvalidRejectionTx: %w", err)
	}
	return nil
}

// FindInvalidRejection recupera el rechazo de una transacción con datos inválidos, o nil.
func (r *TransactionRepo) FindInvalidRejection(ctx context.Context, id domain.TransactionID) (*valueobject.InvalidTransactionRejection, error) {
	const query = `
		SELECT
			COALESCE(terminal_id::text, ''), COALESCE(merchant_id::text, ''),
			amount_cents, COALESCE(currency, ''),
			rejection_code, validation_error, rejected_at
		FROM pn_authorization.invalid_transactions
		WHERE transaction_id = $1
	`
	rejection := valueobject.InvalidTransactionRejection{TransactionID: id}
	var code string
	err := r.pool.QueryRow(ctx, query, id.String()).Scan(
		&rejection.TerminalID, &rejection.MerchantID,
		&rejection.AmountCents, &rejection.Currency,
		&code, &rejection.ValidationError, &rejection.RejectedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("TransactionRepo.FindInvalidRejection: %w", err)
	}
	rejection.Rejection = valueobject.NewRejectionFromValidation(code)
	return &rejection, nil
}

// DeleteInvalidRejectionsBefore purga los rechazos por datos inválidos anteriores a cutoff. Varias
// instancias pueden correrlo a la vez: lo que una borra, la otra no lo encuentra.
func (r *TransactionRepo) DeleteInvalidRejectionsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	const query = `DELETE FROM pn_authorization.invalid_transactions WHERE rejected_at < $1`
	tag, err := r.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("TransactionRepo.DeleteInvalidRejectionsBefore: %w", err)
	}
	return tag.RowsAffected(), nil
}

// fraudRulesJSON serializa las reglas activadas para la columna JSONB. Sin decisión de fraude
// devuelve nil, que se guarda como NULL.
func fraudRulesJSON(tx *aggregate.Transaction) ([]byte, error) {
	rules := tx.FraudDecision().RulesHit
	if rules == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(rules)
	if err != nil {
		return nil, fmt.Errorf("encode fraud rules: %w", err)
	}
	return encoded, nil
}

// SaveIfState guarda el estado y el resultado solo si en la base sigue en expected.
func (r *TransactionRepo) SaveIfState(ctx context.Context, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error) {
	return saveIfState(ctx, r.pool, tx, expected)
}

// SaveIfStateTx hace lo mismo que SaveIfState dentro de una transacción existente.
func (r *TransactionRepo) SaveIfStateTx(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error) {
	return saveIfState(ctx, dbTx, tx, expected)
}

// saveIfState es la única forma segura de avanzar la Saga: la condición sobre el estado la
// evalúa Postgres, así que de dos procesos que intentan la misma transición gana uno solo.
func saveIfState(ctx context.Context, db execer, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error) {
	const query = `
		UPDATE pn_authorization.transactions SET
			state            = $3::text,
			auth_code        = $4,
			rejection_code   = $5,
			rejection_source = $6,
			fraud_score      = $7,
			fraud_decision   = $8,
			authorized_at    = $9,
			rejected_at      = $10,
			fraud_rules_hit  = $11,
			processing_started_at = CASE WHEN $3::text = 'PROCESSING' THEN now() ELSE processing_started_at END
		WHERE id = $1 AND state = $2
	`
	var authCode *string
	if tx.AuthCode() != nil {
		s := tx.AuthCode().String()
		authCode = &s
	}
	var rejCode, rejSource *string
	if tx.RejectionCode() != nil {
		c := tx.RejectionCode().Code()
		s := string(tx.RejectionCode().Source())
		rejCode = &c
		rejSource = &s
	}
	var fraudScore *int
	var fraudDecision *string
	if !tx.FraudDecision().IsZero() {
		fs := tx.FraudDecision().Score
		fd := tx.FraudDecision().Decision
		fraudScore = &fs
		fraudDecision = &fd
	}
	rulesHit, err := fraudRulesJSON(tx)
	if err != nil {
		return false, fmt.Errorf("TransactionRepo.SaveIfState: %w", err)
	}

	tag, err := db.Exec(ctx, query,
		tx.ID().String(), expected.String(), tx.State().String(),
		authCode, rejCode, rejSource, fraudScore, fraudDecision,
		tx.AuthorizedAt(), tx.RejectedAt(), rulesHit,
	)
	if err != nil {
		return false, fmt.Errorf("TransactionRepo.SaveIfState: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// FindUnresolved devuelve las transacciones que el conciliador debe resolver. COALESCE cubre
// una fila en PROCESSING sin fecha: no debería existir, pero no puede quedar sin resolver.
//
// Lista los IDs y carga cada una por separado, porque un error de Scan cierra el cursor de pgx:
// si no, una fila ilegible frenaría a todas. CountUnresolved la sigue contando, así que la
// alerta avisa de que no se resolvió.
func (r *TransactionRepo) FindUnresolved(ctx context.Context, processingBefore time.Time, limit int) ([]*aggregate.Transaction, error) {
	const query = `
		SELECT id::text
		FROM pn_authorization.transactions
		WHERE state = 'INDETERMINATE'
		   OR (state = 'PROCESSING' AND COALESCE(processing_started_at, created_at) < $1)
		ORDER BY created_at
		LIMIT $2
	`
	return r.loadByIDs(ctx, query, processingBefore, limit)
}

// FindPendingReversals devuelve las transacciones con un pedido de reversa sin completar.
func (r *TransactionRepo) FindPendingReversals(ctx context.Context, requestedBefore time.Time, limit int) ([]*aggregate.Transaction, error) {
	const query = `
		SELECT id::text
		FROM pn_authorization.transactions
		WHERE state = 'APPROVED' AND reversal_requested_at < $1
		ORDER BY reversal_requested_at
		LIMIT $2
	`
	return r.loadByIDs(ctx, query, requestedBefore, limit)
}

// loadByIDs ejecuta una consulta de IDs y carga cada transacción. Ver FindUnresolved.
func (r *TransactionRepo) loadByIDs(ctx context.Context, query string, args ...any) ([]*aggregate.Transaction, error) {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("TransactionRepo.loadByIDs: query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("TransactionRepo.loadByIDs: scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("TransactionRepo.loadByIDs: rows: %w", err)
	}
	rows.Close()

	txs := make([]*aggregate.Transaction, 0, len(ids))
	for _, id := range ids {
		txID, err := domain.ParseTransactionID(id)
		if err != nil {
			slog.ErrorContext(ctx, "unreadable transaction skipped by the reconciler", slog.String("transaction_id", id), slog.String("error", err.Error()))
			continue
		}
		tx, err := r.FindByID(ctx, txID)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("TransactionRepo.loadByIDs: %w", ctx.Err())
			}
			slog.ErrorContext(ctx, "unreadable transaction skipped by the reconciler", slog.String("transaction_id", id), slog.String("error", err.Error()))
			continue
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// MarkReversalRequestedTx guarda el pedido de reversa, conservando la fecha del primero.
func (r *TransactionRepo) MarkReversalRequestedTx(ctx context.Context, dbTx pgx.Tx, id domain.TransactionID) error {
	const query = `
		UPDATE pn_authorization.transactions
		SET reversal_requested_at = COALESCE(reversal_requested_at, now())
		WHERE id = $1
	`
	if _, err := dbTx.Exec(ctx, query, id.String()); err != nil {
		return fmt.Errorf("TransactionRepo.MarkReversalRequestedTx: %w", err)
	}
	return nil
}

// CountUnresolved cuenta las transacciones sin resolver, por motivo, y los eventos trabados
// en el outbox.
func (r *TransactionRepo) CountUnresolved(ctx context.Context, processingBefore, reversalsBefore, outboxBefore time.Time) (repository.UnresolvedCounts, error) {
	const query = `
		SELECT
			COUNT(*) FILTER (WHERE state = 'INDETERMINATE'),
			COUNT(*) FILTER (WHERE state = 'PROCESSING' AND COALESCE(processing_started_at, created_at) < $1),
			COUNT(*) FILTER (WHERE state = 'APPROVED' AND reversal_requested_at < $2),
			(SELECT COUNT(*) FROM pn_authorization.outbox WHERE created_at < $3)
		FROM pn_authorization.transactions
		WHERE state IN ('INDETERMINATE', 'PROCESSING')
		   OR (state = 'APPROVED' AND reversal_requested_at IS NOT NULL)
	`
	var c repository.UnresolvedCounts
	if err := r.pool.QueryRow(ctx, query, processingBefore, reversalsBefore, outboxBefore).
		Scan(&c.Indeterminate, &c.Processing, &c.ReversalPending, &c.OutboxStuck); err != nil {
		return repository.UnresolvedCounts{}, fmt.Errorf("TransactionRepo.CountUnresolved: %w", err)
	}
	return c, nil
}

// requiredTables y requiredColumns son lo que el servicio necesita para operar. Las
// migraciones se aplican a mano, y olvidar una no da otro aviso: /readyz lo informa.
var (
	requiredTables = []string{
		"pn_authorization.transactions",
		"pn_authorization.processed_events",
		"pn_authorization.blocked_cards",
		"pn_authorization.invalid_transactions",
		"pn_authorization.outbox",
	}
	requiredColumns = []string{"card_token", "processing_started_at", "reversal_requested_at"}
)

// MissingSchema devuelve las tablas y columnas requeridas que no existen.
func (r *TransactionRepo) MissingSchema(ctx context.Context) ([]string, error) {
	const query = `
		SELECT t FROM unnest($1::text[]) AS t WHERE to_regclass(t) IS NULL
		UNION ALL
		SELECT 'pn_authorization.transactions.' || c FROM unnest($2::text[]) AS c
		WHERE NOT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'pn_authorization' AND table_name = 'transactions' AND column_name = c
		)
	`
	rows, err := r.pool.Query(ctx, query, requiredTables, requiredColumns)
	if err != nil {
		return nil, fmt.Errorf("TransactionRepo.MissingSchema: %w", err)
	}
	defer rows.Close()

	var missing []string
	for rows.Next() {
		var item string
		if err := rows.Scan(&item); err != nil {
			return nil, fmt.Errorf("TransactionRepo.MissingSchema: scan: %w", err)
		}
		missing = append(missing, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("TransactionRepo.MissingSchema: rows: %w", err)
	}
	return missing, nil
}

// FindByID recupera una Transaction por su ID.
func (r *TransactionRepo) FindByID(ctx context.Context, id domain.TransactionID) (*aggregate.Transaction, error) {
	const query = `
		SELECT
			id, terminal_id, merchant_id,
			state, amount_cents, currency,
			pan_last4, card_network, entry_mode,
			stan, auth_code, rejection_code, rejection_source,
			fraud_score, fraud_decision, fraud_rules_hit,
			emv_data_b64, iso8583_raw,
			created_at, authorized_at, rejected_at,
			card_token, reversal_requested_at
		FROM pn_authorization.transactions
		WHERE id = $1
	`

	row := r.pool.QueryRow(ctx, query, id.String())
	return scanTransaction(row)
}

// FindByIDForUpdateTx usa la conexión de dbTx: pedir otra al pool con una transacción abierta
// bloquea el servicio cuando todas las conexiones están esperando una más.
func (r *TransactionRepo) FindByIDForUpdateTx(ctx context.Context, dbTx pgx.Tx, id domain.TransactionID) (*aggregate.Transaction, error) {
	const query = `
		SELECT
			id, terminal_id, merchant_id,
			state, amount_cents, currency,
			pan_last4, card_network, entry_mode,
			stan, auth_code, rejection_code, rejection_source,
			fraud_score, fraud_decision, fraud_rules_hit,
			emv_data_b64, iso8583_raw,
			created_at, authorized_at, rejected_at,
			card_token, reversal_requested_at
		FROM pn_authorization.transactions
		WHERE id = $1
		FOR UPDATE
	`
	return scanTransaction(dbTx.QueryRow(ctx, query, id.String()))
}

// ExistsByID verifica si una transacción con ese ID ya existe.
func (r *TransactionRepo) ExistsByID(ctx context.Context, id domain.TransactionID) (bool, error) {
	const query = `SELECT EXISTS(SELECT 1 FROM pn_authorization.transactions WHERE id = $1)`
	var exists bool
	err := r.pool.QueryRow(ctx, query, id.String()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("TransactionRepo.ExistsByID: %w", err)
	}
	return exists, nil
}

// ─── Scanner ──────────────────────────────────────────────────────────────────

// txRow es la estructura que mapea la fila de Postgres al aggregate.
type txRow struct {
	ID              string
	TerminalID      string
	MerchantID      string
	State           string
	AmountCents     int64
	Currency        string
	PanLast4        string
	CardNetwork     string
	EntryMode       string
	Stan            int
	AuthCode        *string
	RejectionCode   *string
	RejectionSource *string
	FraudScore      *int
	FraudDecision   *string
	FraudRulesHit   []byte // JSONB
	EMVDataB64      string
	ISO8583Raw      []byte
	CreatedAt       time.Time
	AuthorizedAt    *time.Time
	RejectedAt      *time.Time
	CardToken       *string

	ReversalRequestedAt *time.Time
}

func scanTransaction(row pgx.Row) (*aggregate.Transaction, error) {
	var r txRow
	err := row.Scan(
		&r.ID, &r.TerminalID, &r.MerchantID,
		&r.State, &r.AmountCents, &r.Currency,
		&r.PanLast4, &r.CardNetwork, &r.EntryMode,
		&r.Stan, &r.AuthCode, &r.RejectionCode, &r.RejectionSource,
		&r.FraudScore, &r.FraudDecision, &r.FraudRulesHit,
		&r.EMVDataB64, &r.ISO8583Raw,
		&r.CreatedAt, &r.AuthorizedAt, &r.RejectedAt,
		&r.CardToken, &r.ReversalRequestedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pkgerrors.NewNotFoundError("Transaction", "")
		}
		return nil, fmt.Errorf("scanTransaction: %w", err)
	}

	// Reconstruir Value Objects
	currency, err := domain.ParseCurrency(r.Currency)
	if err != nil {
		return nil, fmt.Errorf("scanTransaction: parse currency: %w", err)
	}
	amount, err := domain.NewMoney(r.AmountCents, currency)
	if err != nil {
		return nil, fmt.Errorf("scanTransaction: parse money: %w", err)
	}
	txID, _ := domain.ParseTransactionID(r.ID)
	terminalID, _ := domain.ParseTerminalID(r.TerminalID)
	merchantID, _ := domain.ParseMerchantID(r.MerchantID)
	stan, _ := domain.NewSTAN(r.Stan)
	network, _ := domain.ParseCardNetwork(r.CardNetwork)
	pan, _ := domain.NewPAN(r.PanLast4, network)
	entryMode, _ := valueobject.ParseEntryMode(r.EntryMode)

	// Un token malformado en la base se trata como ausente: degradar a "no
	// bloqueable" es preferible a que FindByID falle y frene la Saga entera.
	var cardToken domain.CardToken
	if r.CardToken != nil {
		cardToken, _ = domain.ParseOptionalCardToken(*r.CardToken)
	}

	// Reconstruir FraudDecision si está disponible
	var fraudDecision valueobject.FraudDecision
	if r.FraudScore != nil && r.FraudDecision != nil {
		var rulesHit []string
		if r.FraudRulesHit != nil {
			_ = json.Unmarshal(r.FraudRulesHit, &rulesHit)
		}
		fraudDecision, _ = valueobject.NewFraudDecision(*r.FraudScore, *r.FraudDecision, rulesHit)
	}

	// Reconstruir el aggregate usando el constructor de reconstitución
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:              txID,
		TerminalID:      terminalID,
		MerchantID:      merchantID,
		Amount:          amount,
		STAN:            stan,
		PAN:             pan,
		EntryMode:       entryMode,
		CardToken:       cardToken,
		State:           valueobject.TransactionState(r.State),
		FraudDecision:   fraudDecision,
		AuthCode:        r.AuthCode,
		RejectionCode:   r.RejectionCode,
		RejectionSource: r.RejectionSource,
		EMVDataBase64:   r.EMVDataB64,
		ISO8583Raw:      r.ISO8583Raw,
		ReceivedAt:      r.CreatedAt,
		AuthorizedAt:    r.AuthorizedAt,
		RejectedAt:      r.RejectedAt,

		ReversalRequestedAt: r.ReversalRequestedAt,
	}), nil
}
