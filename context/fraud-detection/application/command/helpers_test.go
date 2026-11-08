package command_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/application/port"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests que construyen un *command.EvaluateTransactionHandler real.
const idempotencySchema = "fraud_detection"

// ─── fakeFraudCaseRepo ────────────────────────────────────────────────────────

type fakeFraudCaseRepo struct {
	saveErr    error
	savedCases []*aggregate.FraudCase
	findResult *aggregate.FraudCase
	findErr    error

	// existing es el caso que ya tiene la transacción, para FindByTransactionIDTx.
	existing  *aggregate.FraudCase
	findTxErr error
	// raced simula otro pedido de la misma transacción que guarda primero: Save no inserta y,
	// desde entonces, FindByTransactionIDTx devuelve raced.
	raced *aggregate.FraudCase
}

var _ repository.FraudCaseRepository = (*fakeFraudCaseRepo)(nil)

func (f *fakeFraudCaseRepo) Save(_ context.Context, _ pgx.Tx, fc *aggregate.FraudCase) (bool, error) {
	if f.saveErr != nil {
		return false, f.saveErr
	}
	if f.raced != nil {
		f.existing = f.raced
		return false, nil
	}
	f.savedCases = append(f.savedCases, fc)
	return true, nil
}

func (f *fakeFraudCaseRepo) FindByTransactionIDTx(context.Context, pgx.Tx, domain.TransactionID) (*aggregate.FraudCase, error) {
	return f.existing, f.findTxErr
}

func (f *fakeFraudCaseRepo) FindByTransactionID(_ context.Context, _ domain.TransactionID) (*aggregate.FraudCase, error) {
	return f.findResult, f.findErr
}

// ─── fakeRuleRepo ────────────────────────────────────────────────────────────

type fakeRuleRepo struct {
	rules      []*entity.FraudRule
	findErr    error
	findCalls  int
	saveErr    error
	savedRules []*entity.FraudRule
}

var _ repository.FraudRuleRepository = (*fakeRuleRepo)(nil)

func (f *fakeRuleRepo) FindAllActive(context.Context) ([]*entity.FraudRule, error) {
	f.findCalls++
	return f.rules, f.findErr
}

func (f *fakeRuleRepo) Save(_ context.Context, rule *entity.FraudRule) error {
	f.savedRules = append(f.savedRules, rule)
	return f.saveErr
}

// ─── fakeHistRepo (para construir un *service.RuleEngine real) ────────────────

type fakeHistRepo struct {
	txPerHour int
}

var _ repository.TransactionHistoryRepository = (*fakeHistRepo)(nil)

func (f *fakeHistRepo) CountByTerminalLastHour(context.Context, domain.TerminalID) (int, error) {
	return f.txPerHour, nil
}

func (f *fakeHistRepo) AverageAmountByMerchant(context.Context, domain.MerchantID) (int64, error) {
	return 0, nil
}

func (f *fakeHistRepo) CountRecentRejectionsByTerminal(context.Context, domain.TerminalID, int) (int, error) {
	return 0, nil
}

func (f *fakeHistRepo) CountSameAmountAttempts(context.Context, domain.TerminalID, int64, int) (int, error) {
	return 0, nil
}

// ─── fakePublisher ───────────────────────────────────────────────────────────

type fakePublisher struct {
	publishCalls  int
	publishErr    error
	lastFraudCase *aggregate.FraudCase
}

var _ service.EventPublisher = (*fakePublisher)(nil)

func (f *fakePublisher) PublishFraudScoreCalculated(_ context.Context, _ pgx.Tx, fc *aggregate.FraudCase) error {
	f.publishCalls++
	f.lastFraudCase = fc
	return f.publishErr
}

// ─── builders ────────────────────────────────────────────────────────────────

func mustFraudRule(t *testing.T, id string, scoreWeight int) *entity.FraudRule {
	t.Helper()
	r, err := entity.NewFraudRule(id, "Rule "+id, "description", scoreWeight, 0)
	if err != nil {
		t.Fatalf("NewFraudRule(%q) error = %v", id, err)
	}
	return r
}

// newEngine construye un *service.RuleEngine real con reglas y comportamiento
// de historial controlables por el test — el engine en sí no es mockeable
// (es un tipo concreto), pero sus propias dependencias sí lo son.
func newEngine(rules []*entity.FraudRule, txPerHour int) *service.RuleEngine {
	ruleRepo := &fakeRuleRepo{rules: rules}
	histRepo := &fakeHistRepo{txPerHour: txPerHour}
	return service.NewRuleEngine(ruleRepo, histRepo, time.Second)
}

// validEvaluateCmd devuelve un EvaluateTransactionCommand válido con datos que
// activan RULE-005 (magstripe + monto alto) de forma determinística.
func validEvaluateCmd(t *testing.T) port.EvaluateTransactionCommand {
	t.Helper()
	return port.EvaluateTransactionCommand{
		EventID:       domain.NewTransactionID().String(),
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   6_000_000,
		Currency:      "ARS",
		CardNetwork:   "VISA",
		EntryMode:     "MAGSTRIPE",
		OccurredAt:    "2026-01-01T10:00:00Z",
	}
}
