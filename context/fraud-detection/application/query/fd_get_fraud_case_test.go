package query_test

import (
	"context"

	"errors"
	"github.com/jackc/pgx/v5"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/application/query"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── fakes ───────────────────────────────────────────────────────────────────

type fakeFraudCaseRepo struct {
	findResult *aggregate.FraudCase
	findErr    error
}

var _ repository.FraudCaseRepository = (*fakeFraudCaseRepo)(nil)

func (f *fakeFraudCaseRepo) Save(context.Context, pgx.Tx, *aggregate.FraudCase) (bool, error) {
	return true, nil
}

func (f *fakeFraudCaseRepo) FindByTransactionIDTx(context.Context, pgx.Tx, domain.TransactionID) (*aggregate.FraudCase, error) {
	return nil, nil
}

func (f *fakeFraudCaseRepo) FindByTransactionID(context.Context, domain.TransactionID) (*aggregate.FraudCase, error) {
	return f.findResult, f.findErr
}

type fakeRuleRepo struct {
	rules []*entity.FraudRule
	err   error
}

var _ repository.FraudRuleRepository = (*fakeRuleRepo)(nil)

func (f *fakeRuleRepo) FindAllActive(context.Context) ([]*entity.FraudRule, error) {
	return f.rules, f.err
}

func (f *fakeRuleRepo) Save(context.Context, *entity.FraudRule) error { return nil }

// ─── builders ────────────────────────────────────────────────────────────────

func mustFraudRule(t *testing.T, id string, scoreWeight int) *entity.FraudRule {
	t.Helper()
	r, err := entity.NewFraudRule(id, "Rule "+id, "description", scoreWeight, 0)
	if err != nil {
		t.Fatalf("NewFraudRule(%q) error = %v", id, err)
	}
	return r
}

func mustRuleEval(t *testing.T, ruleID string, activated bool, score int, reason string) valueobject.RuleEvaluation {
	t.Helper()
	e, err := valueobject.NewRuleEvaluation(ruleID, "Rule "+ruleID, activated, score, reason)
	if err != nil {
		t.Fatalf("NewRuleEvaluation(%q) error = %v", ruleID, err)
	}
	return e
}

func evaluatedFraudCase(t *testing.T, txID domain.TransactionID, evaluatedAt *time.Time) *aggregate.FraudCase {
	t.Helper()
	score, err := valueobject.NewFraudScore(30, []string{"RULE-001"})
	if err != nil {
		t.Fatalf("NewFraudScore() error = %v", err)
	}
	evals := []valueobject.RuleEvaluation{
		mustRuleEval(t, "RULE-001", true, 30, "high velocity"),
		mustRuleEval(t, "RULE-002", false, 0, ""),
	}
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
		Evaluations:   evals,
		EvaluatedAt:   evaluatedAt,
	})
}

// ─── GetFraudCase ────────────────────────────────────────────────────────────

func TestGetFraudCase_InvalidTransactionID(t *testing.T) {
	h := query.NewFraudQueryHandler(&fakeFraudCaseRepo{}, &fakeRuleRepo{})

	_, err := h.GetFraudCase(context.Background(), "not-a-uuid")
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestGetFraudCase_RepoError(t *testing.T) {
	repo := &fakeFraudCaseRepo{findErr: errors.New("connection reset")}
	h := query.NewFraudQueryHandler(repo, &fakeRuleRepo{})

	_, err := h.GetFraudCase(context.Background(), domain.NewTransactionID().String())
	if err == nil || !strings.Contains(err.Error(), "GetFraudCase") {
		t.Fatalf("error = %v, want it to contain %q", err, "GetFraudCase")
	}
}

func TestGetFraudCase_NotFound(t *testing.T) {
	repo := &fakeFraudCaseRepo{findResult: nil, findErr: nil}
	h := query.NewFraudQueryHandler(repo, &fakeRuleRepo{})

	txID := domain.NewTransactionID().String()
	_, err := h.GetFraudCase(context.Background(), txID)
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
	if nf.Entity != "FraudCase" || nf.ID != txID {
		t.Errorf("NotFoundError = %+v, want Entity=FraudCase ID=%s", nf, txID)
	}
}

func TestGetFraudCase_Success(t *testing.T) {
	txID := domain.NewTransactionID()
	evaluatedAt := time.Date(2026, 1, 1, 10, 5, 30, 0, time.UTC)
	fc := evaluatedFraudCase(t, txID, &evaluatedAt)

	repo := &fakeFraudCaseRepo{findResult: fc}
	h := query.NewFraudQueryHandler(repo, &fakeRuleRepo{})

	result, err := h.GetFraudCase(context.Background(), txID.String())
	if err != nil {
		t.Fatalf("GetFraudCase() error = %v", err)
	}
	if result.FraudCaseID != fc.ID() {
		t.Errorf("FraudCaseID = %q, want %q", result.FraudCaseID, fc.ID())
	}
	if result.TransactionID != txID.String() {
		t.Errorf("TransactionID = %q, want %q", result.TransactionID, txID.String())
	}
	if result.Score != 30 {
		t.Errorf("Score = %d, want 30", result.Score)
	}
	if result.Decision != string(valueobject.DecisionApprove) {
		t.Errorf("Decision = %q, want %q", result.Decision, valueobject.DecisionApprove)
	}
	if len(result.RulesHit) != 1 || result.RulesHit[0] != "RULE-001" {
		t.Errorf("RulesHit = %v, want [RULE-001]", result.RulesHit)
	}
	if result.EvaluatedAt != "2026-01-01T10:05:30Z" {
		t.Errorf("EvaluatedAt = %q, want %q", result.EvaluatedAt, "2026-01-01T10:05:30Z")
	}

	if len(result.Evaluations) != 2 {
		t.Fatalf("Evaluations = %v, want 2 items", result.Evaluations)
	}
	if result.Evaluations[0].RuleID != "RULE-001" || !result.Evaluations[0].Activated || result.Evaluations[0].ScoreContribution != 30 {
		t.Errorf("Evaluations[0] = %+v, want activated RULE-001 with contribution 30", result.Evaluations[0])
	}
	if result.Evaluations[1].RuleID != "RULE-002" || result.Evaluations[1].Activated {
		t.Errorf("Evaluations[1] = %+v, want inactive RULE-002", result.Evaluations[1])
	}
}

func TestGetFraudCase_NilEvaluatedAt(t *testing.T) {
	txID := domain.NewTransactionID()
	fc := evaluatedFraudCase(t, txID, nil)
	repo := &fakeFraudCaseRepo{findResult: fc}
	h := query.NewFraudQueryHandler(repo, &fakeRuleRepo{})

	result, err := h.GetFraudCase(context.Background(), txID.String())
	if err != nil {
		t.Fatalf("GetFraudCase() error = %v", err)
	}
	if result.EvaluatedAt != "" {
		t.Errorf("EvaluatedAt = %q, want empty", result.EvaluatedAt)
	}
}

func TestGetFraudCase_NoEvaluations(t *testing.T) {
	txID := domain.NewTransactionID()
	fc := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:            "fraud-case-2",
		TransactionID: txID,
		TerminalID:    domain.NewTerminalID(),
		MerchantID:    domain.NewMerchantID(),
		AmountCents:   5000,
		Currency:      "ARS",
		CardNetwork:   "VISA",
		EntryMode:     "CHIP",
		OccurredAt:    time.Now().UTC(),
	})
	repo := &fakeFraudCaseRepo{findResult: fc}
	h := query.NewFraudQueryHandler(repo, &fakeRuleRepo{})

	result, err := h.GetFraudCase(context.Background(), txID.String())
	if err != nil {
		t.Fatalf("GetFraudCase() error = %v", err)
	}
	if result.Evaluations == nil {
		t.Error("Evaluations = nil, want non-nil empty slice")
	}
	if len(result.Evaluations) != 0 {
		t.Errorf("Evaluations = %v, want empty", result.Evaluations)
	}
}

// ─── ListActiveRules ─────────────────────────────────────────────────────────

func TestListActiveRules_Error(t *testing.T) {
	repo := &fakeRuleRepo{err: errors.New("connection reset")}
	h := query.NewFraudQueryHandler(&fakeFraudCaseRepo{}, repo)

	_, err := h.ListActiveRules(context.Background())
	if err == nil || !strings.Contains(err.Error(), "ListActiveRules") {
		t.Fatalf("error = %v, want it to contain %q", err, "ListActiveRules")
	}
}

func TestListActiveRules_Success(t *testing.T) {
	rules := []*entity.FraudRule{
		mustFraudRule(t, "RULE-001", 10),
		mustFraudRule(t, "RULE-002", 20),
	}
	repo := &fakeRuleRepo{rules: rules}
	h := query.NewFraudQueryHandler(&fakeFraudCaseRepo{}, repo)

	results, err := h.ListActiveRules(context.Background())
	if err != nil {
		t.Fatalf("ListActiveRules() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %v, want 2 items", results)
	}
	if results[0].ID != "RULE-001" || results[0].ScoreWeight != 10 {
		t.Errorf("results[0] = %+v, want ID=RULE-001 ScoreWeight=10", results[0])
	}
	if results[1].ID != "RULE-002" || results[1].ScoreWeight != 20 {
		t.Errorf("results[1] = %+v, want ID=RULE-002 ScoreWeight=20", results[1])
	}
	if !results[0].IsActive {
		t.Error("results[0].IsActive = false, want true")
	}
}

func TestListActiveRules_Empty(t *testing.T) {
	repo := &fakeRuleRepo{rules: nil}
	h := query.NewFraudQueryHandler(&fakeFraudCaseRepo{}, repo)

	results, err := h.ListActiveRules(context.Background())
	if err != nil {
		t.Fatalf("ListActiveRules() error = %v", err)
	}
	if results == nil {
		t.Error("results = nil, want non-nil empty slice")
	}
	if len(results) != 0 {
		t.Errorf("results = %v, want empty", results)
	}
}
