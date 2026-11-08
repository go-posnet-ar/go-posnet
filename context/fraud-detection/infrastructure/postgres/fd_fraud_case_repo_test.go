package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/context/fraud-detection/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── Save ───────────────────────────────────────────────────────────────────

// beginTx abre una transacción del pool mock. Las expectations de la transacción se declaran
// antes: ExpectBegin, lo que ejecuta, y ExpectRollback.
func beginTx(t *testing.T, pool pgxmock.PgxPoolIface) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

func TestFraudCaseRepo_Save_Success(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO fraud_detection.fraud_cases").
		WithArgs(anyArgs(14)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()

	repo := postgres.NewFraudCaseRepo(pool)
	if err := repo.Save(context.Background(), beginTx(t, pool), newFraudCase(t, domain.NewTransactionID())); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestFraudCaseRepo_Save_ExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO fraud_detection.fraud_cases").
		WithArgs(anyArgs(14)...).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	repo := postgres.NewFraudCaseRepo(pool)
	err := repo.Save(context.Background(), beginTx(t, pool), newFraudCase(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "FraudCaseRepo.Save") {
		t.Fatalf("error = %v, want it to contain %q", err, "FraudCaseRepo.Save")
	}
}

func TestFraudCaseRepo_Save_SerializesRulesHitAndEvaluations(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()
	fc := newFraudCase(t, txID)

	wantRulesHit := []string{"RULE-001"}
	wantEvals := []map[string]any{
		{"rule_id": "RULE-001", "activated": true, "score_contribution": 30, "reason": "high velocity"},
	}

	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO fraud_detection.fraud_cases").
		WithArgs(
			fc.ID(), fc.TransactionID().String(),
			fc.TerminalID().String(), fc.MerchantID().String(), fc.AmountCents(), fc.Currency(), fc.CardNetwork(), fc.EntryMode(), fc.OccurredAt(),
			fc.Score().Score(), fc.Score().Decision().String(),
			jsonArg{want: wantRulesHit}, jsonArg{want: wantEvals},
			fc.EvaluatedAt(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()

	repo := postgres.NewFraudCaseRepo(pool)
	if err := repo.Save(context.Background(), beginTx(t, pool), fc); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

// El caso de un pedido inválido puede no tener terminal ni comercio: se guardan como UUID nulo.
func TestFraudCaseRepo_Save_InvalidEventUsesNilUUIDs(t *testing.T) {
	pool := newMockPool(t)
	fc, err := aggregate.NewInvalidEventCase(aggregate.InvalidEventParams{
		TransactionID: domain.NewTransactionID(),
		OccurredAt:    time.Now().UTC(),
		Reason:        "invalid terminal_id; invalid merchant_id",
	})
	if err != nil {
		t.Fatalf("NewInvalidEventCase() error = %v", err)
	}
	const nilUUID = "00000000-0000-0000-0000-000000000000"

	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO fraud_detection.fraud_cases").
		WithArgs(
			fc.ID(), fc.TransactionID().String(),
			nilUUID, nilUUID, int64(0), "", "", "", fc.OccurredAt(),
			100, "REJECT",
			pgxmock.AnyArg(), pgxmock.AnyArg(),
			fc.EvaluatedAt(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()

	if err := postgres.NewFraudCaseRepo(pool).Save(context.Background(), beginTx(t, pool), fc); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

// ─── CountOutboxStuck / MissingSchema ────────────────────────────────────────

func TestFraudCaseRepo_CountOutboxStuck(t *testing.T) {
	pool := newMockPool(t)
	before := time.Now()
	pool.ExpectQuery("SELECT COUNT\\(\\*\\) FROM fraud_detection.outbox").
		WithArgs(before).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(3)))

	n, err := postgres.NewFraudCaseRepo(pool).CountOutboxStuck(context.Background(), before)
	if err != nil || n != 3 {
		t.Fatalf("CountOutboxStuck() = %d, %v, want 3, nil", n, err)
	}
}

func TestFraudCaseRepo_MissingSchema(t *testing.T) {
	tests := []struct {
		name  string
		found *string
		want  int
	}{
		{"outbox present", ptr("fraud_detection.outbox"), 0},
		{"outbox missing", nil, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			pool.ExpectQuery("SELECT to_regclass").
				WithArgs("fraud_detection.outbox").
				WillReturnRows(pgxmock.NewRows([]string{"to_regclass"}).AddRow(tc.found))

			missing, err := postgres.NewFraudCaseRepo(pool).MissingSchema(context.Background())
			if err != nil || len(missing) != tc.want {
				t.Fatalf("MissingSchema() = %v, %v, want %d missing", missing, err, tc.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// ─── FindByTransactionID ─────────────────────────────────────────────────────

var fraudCaseColumns = []string{
	"id", "transaction_id",
	"terminal_id", "merchant_id", "amount_cents", "currency", "card_network", "entry_mode", "occurred_at",
	"score", "decision", "rules_hit", "evaluations", "evaluated_at",
}

func fraudCaseRow(t *testing.T, txID string) *pgxmock.Rows {
	t.Helper()
	rulesHit := []byte(`["RULE-001"]`)
	evals := []byte(`[{"rule_id":"RULE-001","rule_name":"Velocity","activated":true,"score_contribution":30,"reason":"high velocity"}]`)
	return pgxmock.NewRows(fraudCaseColumns).AddRow(
		"fraud-case-1", txID,
		domain.NewTerminalID().String(), domain.NewMerchantID().String(), int64(5000), "ARS", "VISA", "CHIP",
		time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		30, "APPROVE", rulesHit, evals,
		time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC),
	)
}

func TestFraudCaseRepo_FindByTransactionID_Success(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()

	pool.ExpectQuery("FROM fraud_detection.fraud_cases").
		WithArgs(txID.String()).
		WillReturnRows(fraudCaseRow(t, txID.String()))

	repo := postgres.NewFraudCaseRepo(pool)
	fc, err := repo.FindByTransactionID(context.Background(), txID)
	if err != nil {
		t.Fatalf("FindByTransactionID() error = %v", err)
	}
	if fc == nil {
		t.Fatal("FindByTransactionID() = nil, want a FraudCase")
	}
	if fc.ID() != "fraud-case-1" {
		t.Errorf("ID() = %q, want %q", fc.ID(), "fraud-case-1")
	}
	if !fc.TransactionID().Equals(txID) {
		t.Errorf("TransactionID() = %v, want %v", fc.TransactionID(), txID)
	}
	if fc.Score().Score() != 30 {
		t.Errorf("Score().Score() = %d, want 30", fc.Score().Score())
	}
	if fc.Score().Decision() != valueobject.DecisionApprove {
		t.Errorf("Score().Decision() = %v, want %v", fc.Score().Decision(), valueobject.DecisionApprove)
	}
	if len(fc.Score().RulesHit()) != 1 || fc.Score().RulesHit()[0] != "RULE-001" {
		t.Errorf("Score().RulesHit() = %v, want [RULE-001]", fc.Score().RulesHit())
	}
	if len(fc.Evaluations()) != 1 || fc.Evaluations()[0].RuleID() != "RULE-001" {
		t.Errorf("Evaluations() = %v, want 1 item with RuleID=RULE-001", fc.Evaluations())
	}
	if fc.EvaluatedAt() == nil {
		t.Error("EvaluatedAt() = nil, want non-nil")
	}
}

func TestFraudCaseRepo_FindByTransactionID_NotFound(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("FROM fraud_detection.fraud_cases").
		WithArgs(anyArgs(1)...).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewFraudCaseRepo(pool)
	fc, err := repo.FindByTransactionID(context.Background(), domain.NewTransactionID())
	if err != nil {
		t.Fatalf("FindByTransactionID() error = %v, want nil", err)
	}
	if fc != nil {
		t.Errorf("FindByTransactionID() = %v, want nil", fc)
	}
}

func TestFraudCaseRepo_FindByTransactionID_QueryError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("FROM fraud_detection.fraud_cases").
		WithArgs(anyArgs(1)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewFraudCaseRepo(pool)
	_, err := repo.FindByTransactionID(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "FraudCaseRepo.FindByTransactionID") {
		t.Fatalf("error = %v, want it to contain %q", err, "FraudCaseRepo.FindByTransactionID")
	}
}

func TestFraudCaseRepo_FindByTransactionID_InvalidScoreInRow(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()
	rows := pgxmock.NewRows(fraudCaseColumns).AddRow(
		"fraud-case-1", txID.String(),
		domain.NewTerminalID().String(), domain.NewMerchantID().String(), int64(5000), "ARS", "VISA", "CHIP",
		time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		999, "APPROVE", []byte(`[]`), []byte(`[]`), // score fuera de rango [0,100]
		time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC),
	)
	pool.ExpectQuery("FROM fraud_detection.fraud_cases").
		WithArgs(anyArgs(1)...).
		WillReturnRows(rows)

	repo := postgres.NewFraudCaseRepo(pool)
	_, err := repo.FindByTransactionID(context.Background(), txID)
	if err == nil || !strings.Contains(err.Error(), "reconstruct score") {
		t.Fatalf("error = %v, want it to contain %q", err, "reconstruct score")
	}
}

func TestFraudCaseRepo_FindByTransactionID_MalformedJSONColumnsAreIgnored(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()
	rows := pgxmock.NewRows(fraudCaseColumns).AddRow(
		"fraud-case-1", txID.String(),
		domain.NewTerminalID().String(), domain.NewMerchantID().String(), int64(5000), "ARS", "VISA", "CHIP",
		time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		30, "APPROVE", []byte("not-json"), []byte("not-json"),
		time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC),
	)
	pool.ExpectQuery("FROM fraud_detection.fraud_cases").
		WithArgs(anyArgs(1)...).
		WillReturnRows(rows)

	repo := postgres.NewFraudCaseRepo(pool)
	fc, err := repo.FindByTransactionID(context.Background(), txID)
	if err != nil {
		t.Fatalf("FindByTransactionID() error = %v, want nil (JSON inválido se ignora silenciosamente)", err)
	}
	if len(fc.Score().RulesHit()) != 0 {
		t.Errorf("Score().RulesHit() = %v, want empty", fc.Score().RulesHit())
	}
	if len(fc.Evaluations()) != 0 {
		t.Errorf("Evaluations() = %v, want empty", fc.Evaluations())
	}
}
