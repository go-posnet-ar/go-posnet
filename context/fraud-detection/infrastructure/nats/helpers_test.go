package nats

import (
	"context"

	"github.com/jackc/pgx/v5"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/service"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests que construyen un *command.EvaluateTransactionHandler real.
const idempotencySchema = "fraud_detection"

// ─── fakeJetStream ───────────────────────────────────────────────────────────

// fakeJetStream implementa natsclient.JetStreamContext embebiendo la interfaz
// (nil) y sobreescribiendo solo PublishMsg y QueueSubscribe.
type fakeJetStream struct {
	natsclient.JetStreamContext

	publishErr error
	published  []*natsclient.Msg
	ackSeq     uint64

	subscribeErr   error
	subscribeCalls []queueSubscribeCall
}

type queueSubscribeCall struct {
	subject string
	durable string
}

func (f *fakeJetStream) PublishMsg(m *natsclient.Msg, _ ...natsclient.PubOpt) (*natsclient.PubAck, error) {
	f.published = append(f.published, m)
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return &natsclient.PubAck{Sequence: f.ackSeq}, nil
}

func (f *fakeJetStream) QueueSubscribe(subj, queue string, _ natsclient.MsgHandler, _ ...natsclient.SubOpt) (*natsclient.Subscription, error) {
	f.subscribeCalls = append(f.subscribeCalls, queueSubscribeCall{subject: subj, durable: queue})
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	return nil, nil
}

// ─── fakes de dominio (para construir un *command.EvaluateTransactionHandler) ─

type fakeFraudCaseRepo struct {
	saveErr    error
	savedCases []*aggregate.FraudCase
	findResult *aggregate.FraudCase
	findErr    error
}

var _ repository.FraudCaseRepository = (*fakeFraudCaseRepo)(nil)

func (f *fakeFraudCaseRepo) Save(_ context.Context, _ pgx.Tx, fc *aggregate.FraudCase) error {
	f.savedCases = append(f.savedCases, fc)
	return f.saveErr
}

func (f *fakeFraudCaseRepo) FindByTransactionID(_ context.Context, _ domain.TransactionID) (*aggregate.FraudCase, error) {
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

type fakeHistRepo struct{}

var _ repository.TransactionHistoryRepository = (*fakeHistRepo)(nil)

func (f *fakeHistRepo) CountByTerminalLastHour(context.Context, domain.TerminalID) (int, error) {
	return 0, nil
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

// fakeDomainPublisher implementa service.EventPublisher — distinto del
// *EventPublisher productivo de este mismo paquete (fd_nats_publisher.go),
// que es lo que se está testeando en fd_nats_publisher_test.go.
type fakeDomainPublisher struct {
	publishCalls int
}

var _ service.EventPublisher = (*fakeDomainPublisher)(nil)

func (f *fakeDomainPublisher) PublishFraudScoreCalculated(context.Context, pgx.Tx, *aggregate.FraudCase) error {
	f.publishCalls++
	return nil
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

// newEngine construye un *service.RuleEngine real con la regla RULE-005
// (magstripe + monto alto), que se puede activar de forma determinística
// sin depender del historial de transacciones.
func newEngine(t *testing.T) *service.RuleEngine {
	t.Helper()
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{mustFraudRule(t, "RULE-005", 20)}}
	return service.NewRuleEngine(ruleRepo, &fakeHistRepo{}, time.Second)
}

func newEvaluatedFraudCase(t *testing.T, txID domain.TransactionID) *aggregate.FraudCase {
	t.Helper()
	score, err := valueobject.NewFraudScore(30, []string{"RULE-001"})
	if err != nil {
		t.Fatalf("NewFraudScore() error = %v", err)
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
		OccurredAt:    time.Now().UTC(),
		Score:         score,
	})
}

// newApprovedFraudCase construye un FraudCase con score 0 y ninguna regla
// activada (RulesHit vacío) — el caso de aprobación limpia.
func newApprovedFraudCase(t *testing.T, txID domain.TransactionID) *aggregate.FraudCase {
	t.Helper()
	score, err := valueobject.NewFraudScore(0, nil)
	if err != nil {
		t.Fatalf("NewFraudScore() error = %v", err)
	}
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:            "fraud-case-2",
		TransactionID: txID,
		TerminalID:    domain.NewTerminalID(),
		MerchantID:    domain.NewMerchantID(),
		AmountCents:   5000,
		Currency:      "ARS",
		CardNetwork:   "VISA",
		EntryMode:     "CHIP",
		OccurredAt:    time.Now().UTC(),
		Score:         score,
	})
}
