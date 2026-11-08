package http

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── fakeFraudCaseRepo ────────────────────────────────────────────────────────

type fakeFraudCaseRepo struct {
	saveErr    error
	findResult *aggregate.FraudCase
	findErr    error
}

var _ repository.FraudCaseRepository = (*fakeFraudCaseRepo)(nil)

func (f *fakeFraudCaseRepo) Save(context.Context, pgx.Tx, *aggregate.FraudCase) (bool, error) {
	return f.saveErr == nil, f.saveErr
}

func (f *fakeFraudCaseRepo) FindByTransactionIDTx(context.Context, pgx.Tx, domain.TransactionID) (*aggregate.FraudCase, error) {
	return nil, nil
}

func (f *fakeFraudCaseRepo) FindByTransactionID(context.Context, domain.TransactionID) (*aggregate.FraudCase, error) {
	return f.findResult, f.findErr
}

// panickyFraudCaseRepo simula un bug en la capa de aplicación para probar que
// recoverMiddleware efectivamente protege al proceso HTTP.
type panickyFraudCaseRepo struct{}

var _ repository.FraudCaseRepository = (*panickyFraudCaseRepo)(nil)

func (p *panickyFraudCaseRepo) Save(context.Context, pgx.Tx, *aggregate.FraudCase) (bool, error) {
	return true, nil
}

func (p *panickyFraudCaseRepo) FindByTransactionIDTx(context.Context, pgx.Tx, domain.TransactionID) (*aggregate.FraudCase, error) {
	panic("boom")
}

func (p *panickyFraudCaseRepo) FindByTransactionID(context.Context, domain.TransactionID) (*aggregate.FraudCase, error) {
	panic("boom")
}

// ─── fakeRuleRepo ────────────────────────────────────────────────────────────

type fakeRuleRepo struct {
	rules      []*entity.FraudRule
	findErr    error
	saveErr    error
	savedRules []*entity.FraudRule
}

var _ repository.FraudRuleRepository = (*fakeRuleRepo)(nil)

func (f *fakeRuleRepo) FindAllActive(context.Context) ([]*entity.FraudRule, error) {
	return f.rules, f.findErr
}

func (f *fakeRuleRepo) Save(_ context.Context, rule *entity.FraudRule) error {
	f.savedRules = append(f.savedRules, rule)
	return f.saveErr
}

// ─── fakePool ────────────────────────────────────────────────────────────────

// fakePool implementa pgutil.PgxPool. Solo Ping() se ejercita en este
// paquete — ningún handler HTTP del BC Fraud Detection abre transacciones.
type fakePool struct {
	pingErr error
}

func (f *fakePool) Ping(context.Context) error { return f.pingErr }

func (f *fakePool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("fakePool: BeginTx not implemented")
}

// ─── builders ────────────────────────────────────────────────────────────────

func mustFraudRule(t *testing.T, scoreWeight int, id string) *entity.FraudRule {
	t.Helper()
	r, err := entity.NewFraudRule(id, "Rule "+id, "description", scoreWeight, 0)
	if err != nil {
		t.Fatalf("NewFraudRule(%q) error = %v", id, err)
	}
	return r
}

func evaluatedFraudCase(t *testing.T, txID domain.TransactionID) *aggregate.FraudCase {
	t.Helper()
	score, err := valueobject.NewFraudScore(30, []string{"RULE-001"})
	if err != nil {
		t.Fatalf("NewFraudScore() error = %v", err)
	}
	eval, err := valueobject.NewRuleEvaluation("RULE-001", "Velocity", true, 30, "high velocity")
	if err != nil {
		t.Fatalf("NewRuleEvaluation() error = %v", err)
	}
	evaluatedAt := time.Date(2026, 1, 1, 10, 5, 30, 0, time.UTC)
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:            "fraud-case-1",
		TransactionID: txID,
		TerminalID:    domain.NewTerminalID(),
		MerchantID:    domain.NewMerchantID(),
		AmountCents:   5000,
		Currency:      "ARS",
		CardNetwork:   "VISA",
		EntryMode:     "CHIP",
		OccurredAt:    time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		Score:         score,
		Evaluations:   []valueobject.RuleEvaluation{eval},
		EvaluatedAt:   &evaluatedAt,
	})
}
