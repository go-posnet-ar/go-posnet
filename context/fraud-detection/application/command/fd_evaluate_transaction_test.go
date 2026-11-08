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
	"github.com/juantevez/go-posnet/pkg/domain"
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

func TestEvaluateTransaction_ClaimExecError(t *testing.T) {
	pool := newMockPool(t)
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
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	err := newHandler(pool, &fakeFraudCaseRepo{}, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
	if err == nil || !strings.Contains(err.Error(), "EvaluateTransaction: persist") {
		t.Fatalf("error = %v, want it to contain %q", err, "EvaluateTransaction: persist")
	}
}

// Sin transaction_id legible, ni en el payload ni en el envelope, no hay a quién responder: se
// descarta sin tocar la base.
func TestEvaluateTransaction_UnanswerableIsDropped(t *testing.T) {
	pool := newMockPool(t)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}
	cmd := validEvaluateCmd(t)
	cmd.TransactionID, cmd.CorrelationID = "not-a-uuid", "also-not-a-uuid"

	err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) || !strings.Contains(err.Error(), "invalid transaction_id") {
		t.Fatalf("error = %v, want a ValidationError about transaction_id", err)
	}
	if len(repo.savedCases) != 0 || pub.publishCalls != 0 {
		t.Errorf("saved = %d, published = %d, want 0 and 0", len(repo.savedCases), pub.publishCalls)
	}
}

// Con el transaction_id del payload ilegible, se usa el correlation_id del envelope: la
// transacción recibe un REJECT en lugar de esperar su timeout.
func TestEvaluateTransaction_FallsBackToTheEnvelopeTransactionID(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	repo, pub := &fakeFraudCaseRepo{}, &fakePublisher{}
	cmd := validEvaluateCmd(t)
	cmd.CorrelationID = cmd.TransactionID
	cmd.TransactionID = "not-a-uuid"

	if err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v, want nil", err)
	}
	if pub.publishCalls != 1 {
		t.Fatalf("published = %d, want 1", pub.publishCalls)
	}
	fc := pub.lastFraudCase
	if fc.TransactionID().String() != cmd.CorrelationID || fc.Score().Decision() != valueobject.DecisionReject {
		t.Errorf("case = %s %s, want %s REJECT", fc.TransactionID(), fc.Score().Decision(), cmd.CorrelationID)
	}
	if evals := fc.Evaluations(); len(evals) != 1 || !strings.Contains(evals[0].Reason(), "invalid transaction_id in the payload") {
		t.Errorf("evaluations = %+v, want INVALID_EVENT about the payload transaction_id", evals)
	}
}

// Un evento ya procesado se descarta al marcarlo, sin correr el motor. También cubre dos
// entregas simultáneas: la segunda espera en la clave única y encuentra el evento marcado.
func TestEvaluateTransaction_AlreadyProcessedSkipsTheEngine(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0)
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

// storedCase es un caso ya guardado para la transacción de cmd, con score 80: REJECT.
func storedCase(t *testing.T, cmd port.EvaluateTransactionCommand) *aggregate.FraudCase {
	t.Helper()
	txID, _ := domain.ParseTransactionID(cmd.TransactionID)
	score, err := valueobject.NewFraudScore(80, []string{"RULE-003", "RULE-005"})
	if err != nil {
		t.Fatalf("NewFraudScore() error = %v", err)
	}
	at := time.Now().UTC()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{ID: "stored-case", TransactionID: txID, Score: score, EvaluatedAt: &at})
}

// Otro pedido de una transacción que ya tiene caso recibe el mismo score, sin evaluar.
func TestEvaluateTransaction_ExistingCaseAnswersWithTheStoredScore(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	cmd := validEvaluateCmd(t)
	stored := storedCase(t, cmd)
	ruleRepo := &fakeRuleRepo{rules: []*entity.FraudRule{mustFraudRule(t, "RULE-005", 20)}}
	engine := service.NewRuleEngine(ruleRepo, &fakeHistRepo{}, time.Second)
	repo, pub := &fakeFraudCaseRepo{existing: stored}, &fakePublisher{}

	if err := newHandler(pool, repo, engine, pub).EvaluateTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if ruleRepo.findCalls != 0 || len(repo.savedCases) != 0 {
		t.Errorf("rule loads = %d, saved = %d, want 0 and 0", ruleRepo.findCalls, len(repo.savedCases))
	}
	if pub.publishCalls != 1 || pub.lastFraudCase != stored {
		t.Errorf("published = %d, want 1 with the stored case", pub.publishCalls)
	}
}

// Dos pedidos de la misma transacción a la vez: el que pierde el INSERT responde con el caso
// del otro, no con su propia evaluación.
func TestEvaluateTransaction_LostInsertAnswersWithTheWinner(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	cmd := validEvaluateCmd(t)
	winner := storedCase(t, cmd)
	repo, pub := &fakeFraudCaseRepo{raced: winner}, &fakePublisher{}

	if err := newHandler(pool, repo, defaultEngine(t), pub).EvaluateTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("EvaluateTransaction() error = %v", err)
	}
	if pub.publishCalls != 1 || pub.lastFraudCase != winner {
		t.Errorf("published = %d, want 1 with the winner's case (score %d)", pub.publishCalls, winner.Score().Score())
	}
}

func TestEvaluateTransaction_FindExistingError(t *testing.T) {
	pool := newMockPool(t)
	expectRolledBack(pool)
	repo := &fakeFraudCaseRepo{findTxErr: errors.New("connection reset")}

	err := newHandler(pool, repo, defaultEngine(t), &fakePublisher{}).EvaluateTransaction(context.Background(), validEvaluateCmd(t))
	if err == nil || !strings.Contains(err.Error(), "EvaluateTransaction: persist") {
		t.Fatalf("error = %v, want a persist error", err)
	}
}
