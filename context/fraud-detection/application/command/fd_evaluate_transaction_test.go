package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/fraud-detection/application/command"
	"github.com/juantevez/go-posnet/context/fraud-detection/application/port"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/service"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// defaultEngine tiene RULE-005, que validEvaluateCmd activa (magstripe + monto alto): score 20.
func defaultEngine(t *testing.T) *service.RuleEngine {
	t.Helper()
	return newEngine([]*entity.FraudRule{mustFraudRule(t, "RULE-005", 20)}, 0)
}

func newHandler(pool pgxmock.PgxPoolIface, repo *fakeFraudCaseRepo, engine *service.RuleEngine, pub *fakePublisher) *command.EvaluateTransactionHandler {
	return command.NewEvaluateTransactionHandler(repo, engine, pub, natsutil.NewIdempotencyStore(pool, idempotencySchema), pool)
}

// Sin transaction_id no hay a quién responder: se descarta sin tocar la base.
func TestEvaluateTransaction_InvalidTransactionIDIsDropped(t *testing.T) {
	pool := newMockPool(t)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}
	cmd := validEvaluateCmd(t)
	cmd.TransactionID = "not-a-uuid"

	err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) || !strings.Contains(err.Error(), "invalid transaction_id") {
		t.Fatalf("error = %v, want a ValidationError about transaction_id", err)
	}
	if len(repo.savedCases) != 0 || pub.publishCalls != 0 {
		t.Errorf("saved = %d, published = %d, want 0 and 0", len(repo.savedCases), pub.publishCalls)
	}
}

// Un pedido con transaction_id pero datos inválidos se rechaza: se guarda y se responde REJECT.
func TestEvaluateTransaction_InvalidEventIsRejected(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*port.EvaluateTransactionCommand)
		wantReason string
	}{
		{"invalid terminal id", func(c *port.EvaluateTransactionCommand) { c.TerminalID = "not-a-uuid" }, "invalid terminal_id"},
		{"invalid merchant id", func(c *port.EvaluateTransactionCommand) { c.MerchantID = "not-a-uuid" }, "invalid merchant_id"},
		{"non-positive amount", func(c *port.EvaluateTransactionCommand) { c.AmountCents = 0 }, "amount_cents must be positive"},
		{"currency too long", func(c *port.EvaluateTransactionCommand) { c.Currency = "ARSX" }, "currency too long"},
		{"card network too long", func(c *port.EvaluateTransactionCommand) { c.CardNetwork = strings.Repeat("V", 21) }, "card_network too long"},
		{"entry mode too long", func(c *port.EvaluateTransactionCommand) { c.EntryMode = strings.Repeat("C", 21) }, "entry_mode too long"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			expectNotProcessed(pool)
			expectClaimed(pool, 1)
			repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}
			cmd := validEvaluateCmd(t)
			tc.mutate(&cmd)

			if err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), cmd); err != nil {
				t.Fatalf("EvaluateTransaction() error = %v, want nil", err)
			}
			if len(repo.savedCases) != 1 || pub.publishCalls != 1 {
				t.Fatalf("saved = %d, published = %d, want 1 and 1", len(repo.savedCases), pub.publishCalls)
			}
			fc := pub.lastFraudCase
			if fc.Score().Score() != 100 || fc.Score().Decision() != valueobject.DecisionReject {
				t.Errorf("score = %d %s, want 100 REJECT", fc.Score().Score(), fc.Score().Decision())
			}
			evals := fc.Evaluations()
			if len(evals) != 1 || evals[0].RuleID() != aggregate.RuleInvalidEvent || !strings.Contains(evals[0].Reason(), tc.wantReason) {
				t.Errorf("evaluations = %+v, want %s mentioning %q", evals, aggregate.RuleInvalidEvent, tc.wantReason)
			}
		})
	}
}

func TestEvaluateTransaction_OccurredAtFallback(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	expectClaimed(pool, 1)
	repo := &fakeFraudCaseRepo{}

	cmd := validEvaluateCmd(t)
	cmd.OccurredAt = "not-a-date"

	before := time.Now().UTC()
	if err := newHandler(pool, repo, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	after := time.Now().UTC()

	if len(repo.savedCases) != 1 {
		t.Fatalf("saved cases = %d, want 1", len(repo.savedCases))
	}
	occurredAt := repo.savedCases[0].OccurredAt()
	if occurredAt.Before(before) || occurredAt.After(after) {
		t.Errorf("OccurredAt() = %v, want between %v and %v (fallback a time.Now())", occurredAt, before, after)
	}
}

// Si el motor falla, el score neutral se guarda, marca el evento y se encola como cualquier otro.
func TestEvaluateTransaction_EngineFailure_SavesNeutralScore(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	expectClaimed(pool, 1)
	engine := service.NewRuleEngine(&fakeRuleRepo{findErr: errors.New("db down")}, &fakeHistRepo{}, time.Second)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}

	if err := newHandler(pool, repo, engine, pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t)); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if len(repo.savedCases) != 1 || pub.publishCalls != 1 {
		t.Fatalf("saved = %d, published = %d, want 1 and 1", len(repo.savedCases), pub.publishCalls)
	}
	fc := pub.lastFraudCase
	if fc.Score().Score() != 50 || fc.Score().Decision() != valueobject.DecisionReview {
		t.Errorf("score = %d %s, want 50 REVIEW (neutral)", fc.Score().Score(), fc.Score().Decision())
	}
}

// Sin reglas activas el control está apagado: APPROVE con score 0.
func TestEvaluateTransaction_NoActiveRulesApproves(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	expectClaimed(pool, 1)
	pub := &fakePublisher{}

	if err := newHandler(pool, &fakeFraudCaseRepo{}, newEngine(nil, 0), pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t)); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if pub.publishCalls != 1 {
		t.Fatalf("published = %d, want 1", pub.publishCalls)
	}
	if s := pub.lastFraudCase.Score(); s.Score() != 0 || s.Decision() != valueobject.DecisionApprove {
		t.Errorf("score = %d %s, want 0 APPROVE", s.Score(), s.Decision())
	}
}

func TestEvaluateTransaction_Success(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	expectClaimed(pool, 1)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}

	if err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t)); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if len(repo.savedCases) != 1 {
		t.Fatalf("saved cases = %d, want 1", len(repo.savedCases))
	}
	if repo.savedCases[0].Score().Score() != 20 {
		t.Errorf("saved Score().Score() = %d, want 20 (RULE-005 activada)", repo.savedCases[0].Score().Score())
	}
	if pub.publishCalls != 1 || pub.lastFraudCase != repo.savedCases[0] {
		t.Errorf("PublishFraudScoreCalculated calls = %d, want 1 with the saved case", pub.publishCalls)
	}
}

// Una reentrega de un evento ya procesado no vuelve a correr el motor.
func TestEvaluateTransaction_AlreadyProcessedSkipsTheEngine(t *testing.T) {
	pool := newMockPool(t)
	expectAlreadyProcessed(pool)
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{mustFraudRule(t, "RULE-005", 20)}}
	engine := service.NewRuleEngine(ruleRepo, &fakeHistRepo{}, time.Second)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}

	if err := newHandler(pool, repo, engine, pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t)); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if ruleRepo.findCalls != 0 {
		t.Errorf("rule loads = %d, want 0: the engine must not run", ruleRepo.findCalls)
	}
	if len(repo.savedCases) != 0 || pub.publishCalls != 0 {
		t.Errorf("saved = %d, published = %d, want 0 and 0", len(repo.savedCases), pub.publishCalls)
	}
}

func TestEvaluateTransaction_IdempotencyCheckError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("SELECT 1 FROM " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))

	err := newHandler(pool, &fakeFraudCaseRepo{}, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
	var ve *pkgerrors.ValidationError
	if err == nil || errors.As(err, &ve) || !strings.Contains(err.Error(), "check idempotency") {
		t.Fatalf("error = %v, want a transient error about the idempotency check", err)
	}
}

// Dos entregas simultáneas: la que llega segunda a la marca no guarda ni encola.
func TestEvaluateTransaction_ConcurrentDuplicateSkipsSaveAndEnqueue(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	expectClaimed(pool, 0) // 0 filas afectadas → otra entrega lo marcó primero
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}

	if err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t)); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if len(repo.savedCases) != 0 || pub.publishCalls != 0 {
		t.Errorf("saved = %d, published = %d, want 0 and 0", len(repo.savedCases), pub.publishCalls)
	}
}

func TestEvaluateTransaction_ClaimExecError(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	err := newHandler(pool, &fakeFraudCaseRepo{}, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
	if err == nil || !strings.Contains(err.Error(), "EvaluateTransaction: persist") {
		t.Fatalf("error = %v, want it to contain %q", err, "EvaluateTransaction: persist")
	}
}

// expectRolledBack simula una transacción que marca el evento y después falla: se deshace.
func expectRolledBack(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()
}

// Si falla el guardado o el encolado, se deshace todo, también la marca: el evento se reintenta.
func TestEvaluateTransaction_PersistErrorsRollBack(t *testing.T) {
	tests := []struct {
		name string
		repo *fakeFraudCaseRepo
		pub  *fakePublisher
	}{
		{"save fails", &fakeFraudCaseRepo{saveErr: errors.New("db unreachable")}, &fakePublisher{}},
		{"enqueue fails", &fakeFraudCaseRepo{}, &fakePublisher{publishErr: errors.New("outbox insert failed")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			expectNotProcessed(pool)
			expectRolledBack(pool)

			err := newHandler(pool, tc.repo, defaultEngine(t), tc.pub).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
			var ve *pkgerrors.ValidationError
			if err == nil || errors.As(err, &ve) || !strings.Contains(err.Error(), "EvaluateTransaction: persist") {
				t.Fatalf("error = %v, want a transient persist error", err)
			}
		})
	}
}

func TestEvaluateTransaction_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	expectNotProcessed(pool)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	err := newHandler(pool, &fakeFraudCaseRepo{}, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
	if err == nil || !strings.Contains(err.Error(), "EvaluateTransaction: persist") {
		t.Fatalf("error = %v, want it to contain %q", err, "EvaluateTransaction: persist")
	}
}
