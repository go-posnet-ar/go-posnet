package aggregate_test

import (
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

func TestNewInvalidEventCase_RejectsWithScore100(t *testing.T) {
	txID := domain.NewTransactionID()
	fc, err := aggregate.NewInvalidEventCase(aggregate.InvalidEventParams{
		TransactionID: txID,
		AmountCents:   0,
		OccurredAt:    time.Now().UTC(),
		Reason:        "amount_cents must be positive",
	})
	if err != nil {
		t.Fatalf("NewInvalidEventCase() error = %v", err)
	}
	if fc.TransactionID() != txID || !fc.IsEvaluated() || fc.EvaluatedAt() == nil {
		t.Errorf("case = %s evaluated=%v, want %s evaluated", fc.TransactionID(), fc.IsEvaluated(), txID)
	}
	if fc.Score().Score() != 100 || fc.Score().Decision() != valueobject.DecisionReject {
		t.Errorf("score = %d %s, want 100 REJECT", fc.Score().Score(), fc.Score().Decision())
	}
	evals := fc.Evaluations()
	if len(evals) != 1 || evals[0].RuleID() != aggregate.RuleInvalidEvent || !strings.Contains(evals[0].Reason(), "amount_cents") {
		t.Errorf("evaluations = %+v, want one %s with the reason", evals, aggregate.RuleInvalidEvent)
	}
}

func TestNewInvalidEventCase_RequiresATransactionID(t *testing.T) {
	if _, err := aggregate.NewInvalidEventCase(aggregate.InvalidEventParams{Reason: "x"}); err == nil {
		t.Fatal("NewInvalidEventCase() without transaction_id: error = nil, want an error")
	}
}

func TestApplyNoActiveRules_ApprovesWithScore0(t *testing.T) {
	fc := newValidFraudCase(t)
	if err := fc.ApplyNoActiveRules(); err != nil {
		t.Fatalf("ApplyNoActiveRules() error = %v", err)
	}
	if fc.Score().Score() != 0 || fc.Score().Decision() != valueobject.DecisionApprove || len(fc.Score().RulesHit()) != 0 {
		t.Errorf("score = %d %s %v, want 0 APPROVE with no rules hit", fc.Score().Score(), fc.Score().Decision(), fc.Score().RulesHit())
	}
	if evals := fc.Evaluations(); len(evals) != 1 || evals[0].RuleID() != aggregate.RuleNoActiveRules {
		t.Errorf("evaluations = %+v, want one %s", evals, aggregate.RuleNoActiveRules)
	}
}
