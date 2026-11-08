package service

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
)

func TestEvaluate_Success(t *testing.T) {
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{
		mustFraudRule(t, "RULE-001", 10),
		mustFraudRule(t, "RULE-005", 20),
	}}
	histRepo := &fakeHistRepo{txPerHour: 100} // > 60 → activa RULE-001

	re := NewRuleEngine(ruleRepo, histRepo, time.Second)
	fc := newFraudCase(t, 6_000_000, "MAGSTRIPE") // magstripe + monto alto → activa RULE-005

	if err := re.Evaluate(context.Background(), fc); err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}

	if !fc.IsEvaluated() {
		t.Fatal("IsEvaluated() = false, want true")
	}
	if fc.Score().Score() != 30 {
		t.Errorf("Score().Score() = %d, want 30 (10+20)", fc.Score().Score())
	}

	rulesHit := append([]string{}, fc.Score().RulesHit()...)
	sort.Strings(rulesHit)
	want := []string{"RULE-001", "RULE-005"}
	if len(rulesHit) != len(want) || rulesHit[0] != want[0] || rulesHit[1] != want[1] {
		t.Errorf("RulesHit() = %v, want %v (orden puede variar por las goroutines)", rulesHit, want)
	}
}

func TestEvaluate_LoadRulesError(t *testing.T) {
	ruleRepo := &fakeRuleRepo{err: errors.New("connection reset")}
	re := NewRuleEngine(ruleRepo, &fakeHistRepo{}, time.Second)
	fc := newFraudCase(t, 5000, "CHIP")

	err := re.Evaluate(context.Background(), fc)
	if err == nil || !strings.Contains(err.Error(), "load active rules") {
		t.Fatalf("error = %v, want it to contain %q", err, "load active rules")
	}
}

// Sin reglas activas el control está apagado: aprueba con score 0, y el caso lo registra.
func TestEvaluate_NoActiveRules_Approves(t *testing.T) {
	re := NewRuleEngine(&fakeRuleRepo{rules: nil}, &fakeHistRepo{}, time.Second)
	fc := newFraudCase(t, 5000, "CHIP")

	if err := re.Evaluate(context.Background(), fc); err != nil {
		t.Fatalf("Evaluate() error = %v, want nil", err)
	}
	if fc.Score().Score() != 0 || fc.Score().Decision() != valueobject.DecisionApprove {
		t.Errorf("score = %d %s, want 0 APPROVE", fc.Score().Score(), fc.Score().Decision())
	}
	evals := fc.Evaluations()
	if len(evals) != 1 || evals[0].RuleID() != aggregate.RuleNoActiveRules || evals[0].Activated() {
		t.Errorf("evaluations = %+v, want one non-activated %s", evals, aggregate.RuleNoActiveRules)
	}
}

func TestEvaluate_HistoryFailsCompletely_FallsBackToEmptyContext(t *testing.T) {
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{mustFraudRule(t, "RULE-001", 10)}}
	histRepo := &fakeHistRepo{
		txPerHourErr:        errors.New("db down"),
		avgMerchantAmtErr:   errors.New("db down"),
		recentRejectionsErr: errors.New("db down"),
		sameAmountCountErr:  errors.New("db down"),
	}
	re := NewRuleEngine(ruleRepo, histRepo, time.Second)
	fc := newFraudCase(t, 5000, "CHIP")

	// El historial falla completamente, pero Evaluate no debe fallar por eso
	// — sigue con un RuleContext vacío en vez de bloquear la transacción.
	if err := re.Evaluate(context.Background(), fc); err != nil {
		t.Fatalf("Evaluate() error = %v, want nil", err)
	}
	if !fc.IsEvaluated() {
		t.Fatal("IsEvaluated() = false, want true")
	}
	// TxPerHour quedó en 0 (contexto vacío) → RULE-001 (>60) no debió activar.
	if fc.Score().Score() != 0 {
		t.Errorf("Score().Score() = %d, want 0 (RULE-001 no debería activar sin historial)", fc.Score().Score())
	}
}

func TestEvaluate_UnregisteredRuleDoesNotActivate(t *testing.T) {
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{mustFraudRule(t, "RULE-999", 50)}}
	re := NewRuleEngine(ruleRepo, &fakeHistRepo{}, time.Second)
	fc := newFraudCase(t, 5000, "CHIP")

	if err := re.Evaluate(context.Background(), fc); err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if fc.Score().Score() != 0 {
		t.Errorf("Score().Score() = %d, want 0", fc.Score().Score())
	}
	if len(fc.Evaluations()) != 1 {
		t.Fatalf("Evaluations() = %v, want 1 item", fc.Evaluations())
	}
	if fc.Evaluations()[0].Activated() {
		t.Error("Evaluations()[0].Activated() = true, want false")
	}
	if fc.Evaluations()[0].Reason() != "no eval function registered" {
		t.Errorf("Evaluations()[0].Reason() = %q, want %q", fc.Evaluations()[0].Reason(), "no eval function registered")
	}
}

// ─── evaluateRule (whitebox) ──────────────────────────────────────────────────

func TestEvaluateRule_NoFunctionRegistered(t *testing.T) {
	re := NewRuleEngine(&fakeRuleRepo{}, &fakeHistRepo{}, time.Second)
	rule := mustFraudRule(t, "RULE-999", 10)
	fc := newFraudCase(t, 5000, "CHIP")

	eval := re.evaluateRule(context.Background(), fc, rule, RuleContext{})
	if eval.Activated() {
		t.Error("Activated() = true, want false")
	}
	if eval.ScoreContribution() != 0 {
		t.Errorf("ScoreContribution() = %d, want 0", eval.ScoreContribution())
	}
	if eval.Reason() != "no eval function registered" {
		t.Errorf("Reason() = %q, want %q", eval.Reason(), "no eval function registered")
	}
}

func TestEvaluateRule_RegisteredFunctionActivates(t *testing.T) {
	re := NewRuleEngine(&fakeRuleRepo{}, &fakeHistRepo{}, time.Second)
	rule := mustFraudRule(t, "RULE-001", 25)
	fc := newFraudCase(t, 5000, "CHIP")

	eval := re.evaluateRule(context.Background(), fc, rule, RuleContext{TxPerHour: 61})
	if !eval.Activated() {
		t.Fatal("Activated() = false, want true")
	}
	if eval.ScoreContribution() != 25 {
		t.Errorf("ScoreContribution() = %d, want 25 (rule.ScoreWeight())", eval.ScoreContribution())
	}
	if eval.RuleID() != "RULE-001" {
		t.Errorf("RuleID() = %q, want %q", eval.RuleID(), "RULE-001")
	}
}

func TestEvaluateRule_RegisteredFunctionDoesNotActivate(t *testing.T) {
	re := NewRuleEngine(&fakeRuleRepo{}, &fakeHistRepo{}, time.Second)
	rule := mustFraudRule(t, "RULE-001", 25)
	fc := newFraudCase(t, 5000, "CHIP")

	eval := re.evaluateRule(context.Background(), fc, rule, RuleContext{TxPerHour: 10})
	if eval.Activated() {
		t.Error("Activated() = true, want false")
	}
	if eval.ScoreContribution() != 0 {
		t.Errorf("ScoreContribution() = %d, want 0", eval.ScoreContribution())
	}
}

// ─── parseTerminalID ──────────────────────────────────────────────────────────
// Nota: esta función no se usa en ningún otro lugar del código (verificado con
// grep) — es código muerto, pero se testea igual por completitud ya que forma
// parte del archivo.

func TestParseTerminalID(t *testing.T) {
	t.Run("valid uuid", func(t *testing.T) {
		id, err := parseTerminalID("550e8400-e29b-41d4-a716-446655440000")
		if err != nil {
			t.Fatalf("parseTerminalID() error = %v", err)
		}
		if id.String() != "550e8400-e29b-41d4-a716-446655440000" {
			t.Errorf("String() = %q, want %q", id.String(), "550e8400-e29b-41d4-a716-446655440000")
		}
	})

	t.Run("invalid uuid", func(t *testing.T) {
		_, err := parseTerminalID("not-a-uuid")
		if err == nil {
			t.Fatal("parseTerminalID() error = nil, want error")
		}
	})
}
