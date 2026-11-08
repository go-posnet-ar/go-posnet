package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

// ─── InsertTx ───────────────────────────────────────────────────────────────

// beginMockTx abre la transacción de base que InsertTx recibe del handler. Las expectations
// se verifican sobre el mismo mock: InsertTx no debe pedir otra conexión al pool.
func beginMockTx(t *testing.T, pool pgxmock.PgxPoolIface) pgx.Tx {
	t.Helper()
	pool.ExpectBegin()
	dbTx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	return dbTx
}

func TestInsertTx_NewTransaction(t *testing.T) {
	pool := newMockPool(t)
	dbTx := beginMockTx(t, pool)
	pool.ExpectExec("INSERT INTO pn_authorization.transactions").
		WithArgs(anyArgs(22)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewTransactionRepo(pool)
	created, err := repo.InsertTx(context.Background(), dbTx, newValidTransaction(t))
	if err != nil || !created {
		t.Fatalf("InsertTx() = %v, %v, want true, nil", created, err)
	}
}

func TestInsertTx_ExistingTransactionIsNotOverwritten(t *testing.T) {
	pool := newMockPool(t)
	dbTx := beginMockTx(t, pool)
	pool.ExpectExec(`INSERT INTO pn_authorization.transactions[\s\S]*ON CONFLICT \(id\) DO NOTHING`).
		WithArgs(anyArgs(22)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 0))

	repo := postgres.NewTransactionRepo(pool)
	created, err := repo.InsertTx(context.Background(), dbTx, newValidTransaction(t))
	if err != nil || created {
		t.Fatalf("InsertTx() = %v, %v, want false, nil", created, err)
	}
}

func TestInsertTx_ExecError(t *testing.T) {
	pool := newMockPool(t)
	dbTx := beginMockTx(t, pool)
	pool.ExpectExec("INSERT INTO pn_authorization.transactions").
		WithArgs(anyArgs(22)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.InsertTx(context.Background(), dbTx, newValidTransaction(t))
	if err == nil || !strings.Contains(err.Error(), "exec insert") {
		t.Fatalf("error = %v, want it to contain %q", err, "exec insert")
	}
}

func TestInsertTx_ApprovedTransactionArgs(t *testing.T) {
	pool := newMockPool(t)
	dbTx := beginMockTx(t, pool)
	tx := newApprovedTransaction(t)

	pool.ExpectExec("INSERT INTO pn_authorization.transactions").
		WithArgs(
			tx.ID().String(), tx.TerminalID().String(), tx.MerchantID().String(),
			"APPROVED", tx.Amount().Cents(), tx.Amount().Currency().String(),
			tx.PAN().Last4(), string(tx.PAN().Network()), tx.EntryMode().String(),
			tx.STAN().Value(),
			strPtr(tx.AuthCode().String()), (*string)(nil), (*string)(nil),
			intPtr(tx.FraudDecision().Score), strPtr(tx.FraudDecision().Decision), pgxmock.AnyArg(),
			tx.EMVDataBase64(), tx.ISO8583Raw(),
			tx.ReceivedAt(), tx.AuthorizedAt(), (*time.Time)(nil),
			(*string)(nil), // card_token — la transacción de prueba no trae token
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewTransactionRepo(pool)
	if _, err := repo.InsertTx(context.Background(), dbTx, tx); err != nil {
		t.Fatalf("InsertTx() error = %v", err)
	}
}

func TestInsertTx_RejectedTransactionArgs(t *testing.T) {
	pool := newMockPool(t)
	dbTx := beginMockTx(t, pool)
	tx := newRejectedTransaction(t)
	rc := tx.RejectionCode()

	pool.ExpectExec("INSERT INTO pn_authorization.transactions").
		WithArgs(
			tx.ID().String(), tx.TerminalID().String(), tx.MerchantID().String(),
			"REJECTED", tx.Amount().Cents(), tx.Amount().Currency().String(),
			tx.PAN().Last4(), string(tx.PAN().Network()), tx.EntryMode().String(),
			tx.STAN().Value(),
			(*string)(nil), strPtr(rc.Code()), strPtr(string(rc.Source())),
			(*int)(nil), (*string)(nil), []byte(nil), // sin fraud decision — Reject() directo desde RECEIVED
			tx.EMVDataBase64(), tx.ISO8583Raw(),
			tx.ReceivedAt(), (*time.Time)(nil), tx.RejectedAt(),
			(*string)(nil), // card_token — la transacción de prueba no trae token
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewTransactionRepo(pool)
	if _, err := repo.InsertTx(context.Background(), dbTx, tx); err != nil {
		t.Fatalf("InsertTx() error = %v", err)
	}
}

// ─── Rechazos por datos inválidos ───────────────────────────────────────────

func TestSaveInvalidRejection(t *testing.T) {
	pool := newMockPool(t)
	r := valueobject.InvalidTransactionRejection{
		TransactionID:   domain.NewTransactionID(),
		AmountCents:     5000,
		Currency:        "ARS",
		CardLast4:       "1234",
		Rejection:       valueobject.NewRejectionFromValidation(valueobject.CodeInvalidData),
		ValidationError: "invalid card_network: unknown value",
		RejectedAt:      time.Date(2026, 11, 6, 15, 0, 4, 0, time.UTC),
	}
	pool.ExpectExec(`INSERT INTO pn_authorization.invalid_transactions[\s\S]*ON CONFLICT \(transaction_id\) DO NOTHING`).
		WithArgs(r.TransactionID.String(), "", "", int64(5000), "ARS", valueobject.CodeInvalidData, r.ValidationError, r.RejectedAt).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	if err := postgres.NewTransactionRepo(pool).SaveInvalidRejection(context.Background(), r); err != nil {
		t.Fatalf("SaveInvalidRejection() error = %v", err)
	}
}

func TestSaveInvalidRejection_ExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("invalid_transactions").WithArgs(anyArgs(8)...).WillReturnError(errors.New("db down"))

	err := postgres.NewTransactionRepo(pool).SaveInvalidRejection(context.Background(), valueobject.InvalidTransactionRejection{TransactionID: domain.NewTransactionID()})
	if err == nil {
		t.Fatal("SaveInvalidRejection() error = nil, want error")
	}
}

func TestDeleteInvalidRejectionsBefore(t *testing.T) {
	pool := newMockPool(t)
	cutoff := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	pool.ExpectExec("DELETE FROM pn_authorization.invalid_transactions WHERE rejected_at < ").
		WithArgs(cutoff).
		WillReturnResult(pgxmock.NewResult("DELETE", 2))

	got, err := postgres.NewTransactionRepo(pool).DeleteInvalidRejectionsBefore(context.Background(), cutoff)
	if err != nil || got != 2 {
		t.Fatalf("DeleteInvalidRejectionsBefore() = %d, %v, want 2, nil", got, err)
	}
}

func TestDeleteInvalidRejectionsBefore_ExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("DELETE FROM").WithArgs(anyArgs(1)...).WillReturnError(errors.New("db down"))

	if _, err := postgres.NewTransactionRepo(pool).DeleteInvalidRejectionsBefore(context.Background(), time.Now()); err == nil {
		t.Fatal("DeleteInvalidRejectionsBefore() error = nil, want error")
	}
}

func TestFindInvalidRejection(t *testing.T) {
	columns := []string{"terminal_id", "merchant_id", "amount_cents", "currency", "rejection_code", "validation_error", "rejected_at"}
	rejectedAt := time.Date(2026, 11, 6, 15, 0, 4, 0, time.UTC)
	id := domain.NewTransactionID()

	t.Run("existe", func(t *testing.T) {
		pool := newMockPool(t)
		pool.ExpectQuery("FROM pn_authorization.invalid_transactions").WithArgs(id.String()).
			WillReturnRows(pgxmock.NewRows(columns).AddRow("", "", int64(5000), "ARS", valueobject.CodeInvalidData, "motivo", rejectedAt))

		got, err := postgres.NewTransactionRepo(pool).FindInvalidRejection(context.Background(), id)
		if err != nil || got == nil {
			t.Fatalf("FindInvalidRejection() = %v, %v", got, err)
		}
		if got.TransactionID != id || got.Rejection.Code() != valueobject.CodeInvalidData ||
			got.Rejection.Source() != valueobject.SourceValidation || got.ValidationError != "motivo" {
			t.Errorf("got = %+v", got)
		}
	})

	t.Run("no existe", func(t *testing.T) {
		pool := newMockPool(t)
		pool.ExpectQuery("FROM pn_authorization.invalid_transactions").WithArgs(id.String()).
			WillReturnRows(pgxmock.NewRows(columns))

		got, err := postgres.NewTransactionRepo(pool).FindInvalidRejection(context.Background(), id)
		if err != nil || got != nil {
			t.Fatalf("FindInvalidRejection() = %v, %v, want nil, nil", got, err)
		}
	})

	t.Run("error de base", func(t *testing.T) {
		pool := newMockPool(t)
		pool.ExpectQuery("FROM pn_authorization.invalid_transactions").WithArgs(id.String()).
			WillReturnError(errors.New("db down"))

		if _, err := postgres.NewTransactionRepo(pool).FindInvalidRejection(context.Background(), id); err == nil {
			t.Fatal("FindInvalidRejection() error = nil, want error")
		}
	})
}

// ─── FindByID ───────────────────────────────────────────────────────────────

func TestFindByID_Success(t *testing.T) {
	pool := newMockPool(t)
	f := newRowFixture(t)
	f.state = "APPROVED"
	f.authCode = strPtr("AB1234")
	authorizedAt := f.createdAt.Add(time.Minute)
	f.authorizedAt = &authorizedAt
	f.fraudScore = intPtr(10)
	f.fraudDecision = strPtr(valueobject.FraudDecisionApprove)
	rulesJSON, err := json.Marshal([]string{"R1", "R2"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	f.fraudRulesHit = rulesJSON

	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(f.id).
		WillReturnRows(f.rows())

	repo := postgres.NewTransactionRepo(pool)
	id, err := domain.ParseTransactionID(f.id)
	if err != nil {
		t.Fatalf("ParseTransactionID() error = %v", err)
	}

	tx, err := repo.FindByID(context.Background(), id)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if tx.ID().String() != f.id {
		t.Errorf("ID() = %q, want %q", tx.ID().String(), f.id)
	}
	if tx.State() != valueobject.StateApproved {
		t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateApproved)
	}
	if tx.AuthCode() == nil || tx.AuthCode().String() != "AB1234" {
		t.Errorf("AuthCode() = %v, want AB1234", tx.AuthCode())
	}
	if tx.AuthorizedAt() == nil || !tx.AuthorizedAt().Equal(authorizedAt) {
		t.Errorf("AuthorizedAt() = %v, want %v", tx.AuthorizedAt(), authorizedAt)
	}
	if tx.FraudDecision().Score != 10 || tx.FraudDecision().Decision != valueobject.FraudDecisionApprove {
		t.Errorf("FraudDecision() = %+v, want Score=10 Decision=APPROVE", tx.FraudDecision())
	}
	if len(tx.FraudDecision().RulesHit) != 2 || tx.FraudDecision().RulesHit[0] != "R1" {
		t.Errorf("FraudDecision().RulesHit = %v, want [R1 R2]", tx.FraudDecision().RulesHit)
	}
	if tx.RejectionCode() != nil {
		t.Errorf("RejectionCode() = %v, want nil", tx.RejectionCode())
	}
}

func TestFindByID_NotFound(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(anyArgs(1)...).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.FindByID(context.Background(), domain.NewTransactionID())
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestFindByID_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(anyArgs(1)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.FindByID(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "scanTransaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanTransaction")
	}
}

func TestFindByID_InvalidCurrencyInRow(t *testing.T) {
	pool := newMockPool(t)
	f := newRowFixture(t)
	f.currency = "XXX"
	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(anyArgs(1)...).
		WillReturnRows(f.rows())

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.FindByID(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "parse currency") {
		t.Fatalf("error = %v, want it to contain %q", err, "parse currency")
	}
}

func TestFindByID_InvalidAmountInRow(t *testing.T) {
	pool := newMockPool(t)
	f := newRowFixture(t)
	f.amountCents = 0 // domain.NewMoney rechaza monto cero
	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(anyArgs(1)...).
		WillReturnRows(f.rows())

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.FindByID(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "parse money") {
		t.Fatalf("error = %v, want it to contain %q", err, "parse money")
	}
}

func TestFindByID_NoFraudDecisionWhenScoreMissing(t *testing.T) {
	pool := newMockPool(t)
	f := newRowFixture(t) // fraudScore/fraudDecision quedan nil
	pool.ExpectQuery("FROM pn_authorization.transactions").
		WithArgs(anyArgs(1)...).
		WillReturnRows(f.rows())

	repo := postgres.NewTransactionRepo(pool)
	tx, err := repo.FindByID(context.Background(), domain.NewTransactionID())
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if !tx.FraudDecision().IsZero() {
		t.Errorf("FraudDecision() = %+v, want zero value", tx.FraudDecision())
	}
}

// ─── ExistsByID ─────────────────────────────────────────────────────────────

func TestExistsByID_True(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("SELECT EXISTS").
		WithArgs(anyArgs(1)...).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))

	repo := postgres.NewTransactionRepo(pool)
	exists, err := repo.ExistsByID(context.Background(), domain.NewTransactionID())
	if err != nil {
		t.Fatalf("ExistsByID() error = %v", err)
	}
	if !exists {
		t.Error("ExistsByID() = false, want true")
	}
}

func TestExistsByID_False(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("SELECT EXISTS").
		WithArgs(anyArgs(1)...).
		WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))

	repo := postgres.NewTransactionRepo(pool)
	exists, err := repo.ExistsByID(context.Background(), domain.NewTransactionID())
	if err != nil {
		t.Fatalf("ExistsByID() error = %v", err)
	}
	if exists {
		t.Error("ExistsByID() = true, want false")
	}
}

func TestExistsByID_Error(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("SELECT EXISTS").
		WithArgs(anyArgs(1)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewTransactionRepo(pool)
	_, err := repo.ExistsByID(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "TransactionRepo.ExistsByID") {
		t.Fatalf("error = %v, want it to contain %q", err, "TransactionRepo.ExistsByID")
	}
}

// ─── SaveIfState ──────────────────────────────────────────────────────────────

func TestSaveIfState(t *testing.T) {
	cases := map[string]struct {
		rows int64
		want bool
	}{
		"sigue en el estado esperado": {1, true},
		"otro proceso la movió":       {0, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pool := newMockPool(t)
			tx := newApprovedTransaction(t)
			pool.ExpectExec("WHERE id = .* AND state = ").
				WithArgs(append([]any{tx.ID().String(), "PROCESSING", "APPROVED"}, anyArgs(8)...)...).
				WillReturnResult(pgxmock.NewResult("UPDATE", tc.rows))

			got, err := postgres.NewTransactionRepo(pool).SaveIfState(context.Background(), tx, valueobject.StateProcessing)
			if err != nil || got != tc.want {
				t.Errorf("SaveIfState() = %v, %v, want %v", got, err, tc.want)
			}
		})
	}
}

func TestSaveIfState_SavesFraudRules(t *testing.T) {
	pool := newMockPool(t)
	tx := newValidTransaction(t)
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	decision, err := valueobject.NewFraudDecision(55, valueobject.FraudDecisionReview, []string{"R1", "R2"})
	if err != nil {
		t.Fatalf("NewFraudDecision() error = %v", err)
	}
	if err := tx.ApplyFraudDecision(decision); err != nil {
		t.Fatalf("ApplyFraudDecision() error = %v", err)
	}
	pool.ExpectExec("fraud_rules_hit  = \\$11").
		WithArgs(append(anyArgs(10), []byte(`["R1","R2"]`))...).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	if _, err := postgres.NewTransactionRepo(pool).SaveIfState(context.Background(), tx, valueobject.StateFraudChecking); err != nil {
		t.Fatalf("SaveIfState() error = %v", err)
	}
}

func TestSaveIfState_ExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("WHERE id = ").WithArgs(anyArgs(11)...).WillReturnError(errors.New("db down"))

	if _, err := postgres.NewTransactionRepo(pool).SaveIfState(context.Background(), newApprovedTransaction(t), valueobject.StateProcessing); err == nil {
		t.Fatal("SaveIfState() error = nil, want error")
	}
}

func TestSaveIfStateTx_UsesCallerTransaction(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	dbTx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	pool.ExpectExec("WHERE id = ").WithArgs(anyArgs(11)...).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	got, err := postgres.NewTransactionRepo(pool).SaveIfStateTx(context.Background(), dbTx, newApprovedTransaction(t), valueobject.StateFraudChecking)
	if err != nil || !got {
		t.Errorf("SaveIfStateTx() = %v, %v", got, err)
	}
}

func TestFindUnresolved(t *testing.T) {
	pool := newMockPool(t)
	before := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	f := newRowFixture(t)
	f.state = "INDETERMINATE"
	pool.ExpectQuery("state = 'INDETERMINATE'").WithArgs(before, 10).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(f.id))
	pool.ExpectQuery("WHERE id = ").WithArgs(f.id).WillReturnRows(f.rows())

	got, err := postgres.NewTransactionRepo(pool).FindUnresolved(context.Background(), before, 10)
	if err != nil {
		t.Fatalf("FindUnresolved() error = %v", err)
	}
	if len(got) != 1 || got[0].State() != valueobject.StateIndeterminate {
		t.Errorf("got = %+v", got)
	}
}

func TestFindUnresolved_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("state = 'INDETERMINATE'").WithArgs(anyArgs(2)...).WillReturnError(errors.New("db down"))

	if _, err := postgres.NewTransactionRepo(pool).FindUnresolved(context.Background(), time.Now(), 10); err == nil {
		t.Fatal("FindUnresolved() error = nil, want error")
	}
}

func TestCountUnresolved(t *testing.T) {
	pool := newMockPool(t)
	before := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	reversals := before.Add(time.Minute)
	pool.ExpectQuery("COUNT").WithArgs(before, reversals).
		WillReturnRows(pgxmock.NewRows([]string{"i", "p", "r"}).AddRow(int64(2), int64(3), int64(4)))

	got, err := postgres.NewTransactionRepo(pool).CountUnresolved(context.Background(), before, reversals)
	want := repository.UnresolvedCounts{Indeterminate: 2, Processing: 3, ReversalPending: 4}
	if err != nil || got != want {
		t.Errorf("CountUnresolved() = %+v, %v, want %+v", got, err, want)
	}
}

func TestFindPendingReversals(t *testing.T) {
	pool := newMockPool(t)
	before := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	f := newRowFixture(t)
	f.state = "APPROVED"
	requested := before.Add(-time.Minute)
	f.reversalRequestedAt = &requested
	pool.ExpectQuery("reversal_requested_at < ").WithArgs(before, 10).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(f.id))
	pool.ExpectQuery("WHERE id = ").WithArgs(f.id).WillReturnRows(f.rows())

	got, err := postgres.NewTransactionRepo(pool).FindPendingReversals(context.Background(), before, 10)
	if err != nil {
		t.Fatalf("FindPendingReversals() error = %v", err)
	}
	if len(got) != 1 || got[0].ReversalRequestedAt() == nil || !got[0].ReversalRequestedAt().Equal(requested) {
		t.Errorf("got = %+v", got)
	}
}

func TestMarkReversalRequestedTx_KeepsFirstRequest(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	dbTx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	id := domain.NewTransactionID()
	pool.ExpectExec("COALESCE\\(reversal_requested_at, now\\(\\)\\)").WithArgs(id.String()).WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	if err := postgres.NewTransactionRepo(pool).MarkReversalRequestedTx(context.Background(), dbTx, id); err != nil {
		t.Fatalf("MarkReversalRequestedTx() error = %v", err)
	}
}

func TestMissingSchema(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("information_schema.columns").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"item"}).AddRow("pn_authorization.transactions.reversal_requested_at"))

	got, err := postgres.NewTransactionRepo(pool).MissingSchema(context.Background())
	if err != nil || len(got) != 1 {
		t.Errorf("MissingSchema() = %v, %v", got, err)
	}
}

func TestMissingSchema_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("information_schema.columns").WithArgs(anyArgs(2)...).WillReturnError(errors.New("db down"))

	if _, err := postgres.NewTransactionRepo(pool).MissingSchema(context.Background()); err == nil {
		t.Fatal("MissingSchema() error = nil, want error")
	}
}

// Sin escenarios, el mock aprueba cualquier monto, también los reservados.
func TestMockAcquirer_ScenariosAreOptIn(t *testing.T) {
	tx := mockTx(t, postgres.MockAmountError)
	resp, err := postgres.NewMockAcquirerGateway(false).Authorize(context.Background(), tx)
	if err != nil || !resp.IsApproved() {
		t.Errorf("Authorize() = %+v, %v, want approved", resp, err)
	}
}

func TestMockAcquirer_Scenarios(t *testing.T) {
	m := postgres.NewMockAcquirerGateway(true)
	ctx := context.Background()

	if resp, err := m.Authorize(ctx, mockTx(t, 12345)); err != nil || !resp.IsApproved() {
		t.Errorf("normal amount = %+v, %v", resp, err)
	}
	if resp, err := m.Authorize(ctx, mockTx(t, postgres.MockAmountDecline)); err != nil || resp.ResponseCode != valueobject.ISO_INSUFFICIENT_FUNDS {
		t.Errorf("decline = %+v, %v", resp, err)
	}
	if resp, err := m.Authorize(ctx, mockTx(t, postgres.MockAmountCaptureCard)); err != nil || resp.ResponseCode != valueobject.ISO_LOST_CARD {
		t.Errorf("capture = %+v, %v", resp, err)
	}
	if _, err := m.Authorize(ctx, mockTx(t, postgres.MockAmountError)); err == nil {
		t.Error("error scenario returned no error")
	}

	hangCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := m.Authorize(hangCtx, mockTx(t, postgres.MockAmountHang)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("hang = %v, want it to end with the context", err)
	}

	tx := mockTx(t, postgres.MockAmountReverseFails)
	for i := range 2 {
		if err := m.Reverse(ctx, tx); err == nil {
			t.Errorf("reverse %d should fail", i)
		}
	}
	if err := m.Reverse(ctx, tx); err != nil {
		t.Errorf("third reverse = %v, want success", err)
	}
}

func mockTx(t *testing.T, cents int64) *aggregate.Transaction {
	t.Helper()
	tx, err := aggregate.NewTransaction(
		domain.NewTransactionID(), domain.NewTerminalID(), domain.NewMerchantID(),
		mustMoney(t, cents), mustSTAN(t, 1), mustPAN(t), valueobject.EntryModeChip, domain.CardToken{}, "", nil,
	)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	return tx
}

// La lectura usa la conexión de la transacción y bloquea la fila.
func TestFindByIDForUpdateTx(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	dbTx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	f := newRowFixture(t)
	f.state = "FRAUD_CHECKING"
	pool.ExpectQuery("FOR UPDATE").WithArgs(f.id).WillReturnRows(f.rows())

	id, err := domain.ParseTransactionID(f.id)
	if err != nil {
		t.Fatalf("ParseTransactionID() error = %v", err)
	}
	got, err := postgres.NewTransactionRepo(pool).FindByIDForUpdateTx(context.Background(), dbTx, id)
	if err != nil || got.State() != valueobject.StateFraudChecking {
		t.Errorf("FindByIDForUpdateTx() = %v, %v", got, err)
	}
}

// Una fila ilegible no impide resolver las demás.
func TestFindUnresolved_SkipsUnreadableRow(t *testing.T) {
	pool := newMockPool(t)
	bad, good := newRowFixture(t), newRowFixture(t)
	bad.state, good.state = "INDETERMINATE", "INDETERMINATE"
	pool.ExpectQuery("state = 'INDETERMINATE'").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bad.id).AddRow(good.id))
	pool.ExpectQuery("WHERE id = ").WithArgs(bad.id).WillReturnError(errors.New("cannot scan NULL"))
	pool.ExpectQuery("WHERE id = ").WithArgs(good.id).WillReturnRows(good.rows())

	got, err := postgres.NewTransactionRepo(pool).FindUnresolved(context.Background(), time.Now(), 10)
	if err != nil {
		t.Fatalf("FindUnresolved() error = %v", err)
	}
	if len(got) != 1 || got[0].ID().String() != good.id {
		t.Errorf("got = %+v, want only the readable row", got)
	}
}
