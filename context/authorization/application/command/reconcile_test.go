package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func processingTx(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newFraudCheckingTransaction(t)
	fd, err := valueobject.NewFraudDecision(10, valueobject.FraudDecisionApprove, nil)
	if err != nil {
		t.Fatalf("NewFraudDecision() error = %v", err)
	}
	if err := tx.ApplyFraudDecision(fd); err != nil {
		t.Fatalf("ApplyFraudDecision() error = %v", err)
	}
	return tx
}

func indeterminateTx(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := processingTx(t)
	if err := tx.MarkIndeterminate(); err != nil {
		t.Fatalf("MarkIndeterminate() error = %v", err)
	}
	return tx
}

func reconciler(repo *fakeRepo, acquirer *fakeAcquirer, publisher *fakePublisher) *command.AuthorizationHandler {
	return command.NewAuthorizationHandler(repo, acquirer, publisher, natsutil.NewIdempotencyStore(nil, idempotencySchema), nil)
}

func TestReconcile_IndeterminateIsReversedAndRejected(t *testing.T) {
	tx := indeterminateTx(t)
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{tx}}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}

	resolved, failed, err := reconciler(repo, acquirer, publisher).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 1 || failed != 0 {
		t.Fatalf("ReconcileUnresolved() = %d, %d, %v", resolved, failed, err)
	}
	if acquirer.reverseCalls != 1 {
		t.Errorf("Reverse calls = %d, want 1", acquirer.reverseCalls)
	}
	if tx.State() != valueobject.StateRejected || tx.RejectionCode().Source() != valueobject.SourceTimeout {
		t.Errorf("state = %s, rejection = %v", tx.State(), tx.RejectionCode())
	}
	if !tx.RejectionCode().IsRetryable() {
		t.Error("a reversed transaction should be retryable")
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
	// Nunca se informó como aprobada: Settlement no tiene nada que descontar.
	if publisher.reversalCalls != 0 {
		t.Errorf("PublishReversalCompleted calls = %d, want 0", publisher.reversalCalls)
	}
	if len(repo.expected) != 1 || repo.expected[0] != valueobject.StateIndeterminate {
		t.Errorf("saved expecting %v", repo.expected)
	}
}

// Primero se bloquea la transacción en INDETERMINATE: desde ahí, una respuesta tardía del
// adquirente ya no se puede guardar.
func TestReconcile_StuckProcessingIsMarkedBeforeReversing(t *testing.T) {
	tx := processingTx(t)
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{tx}}
	acquirer := &fakeAcquirer{}

	resolved, _, err := reconciler(repo, acquirer, &fakePublisher{}).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 1 {
		t.Fatalf("ReconcileUnresolved() = %d, %v", resolved, err)
	}
	want := []valueobject.TransactionState{valueobject.StateProcessing, valueobject.StateIndeterminate}
	if len(repo.expected) != 2 || repo.expected[0] != want[0] || repo.expected[1] != want[1] {
		t.Errorf("saves expected %v, want %v", repo.expected, want)
	}
}

// Otro proceso guardó la respuesta del adquirente: no hay nada que revertir.
func TestReconcile_ProcessingAlreadyMovedIsNotReversed(t *testing.T) {
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{processingTx(t)}, resultLost: true}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}

	resolved, failed, err := reconciler(repo, acquirer, publisher).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 0 || failed != 0 {
		t.Fatalf("ReconcileUnresolved() = %d, %d, %v", resolved, failed, err)
	}
	if acquirer.reverseCalls != 0 || publisher.rejectedCalls != 0 {
		t.Errorf("reverse = %d, rejected = %d, want 0", acquirer.reverseCalls, publisher.rejectedCalls)
	}
}

// Una reversa que falla no rechaza la transacción, ni frena a las demás.
func TestReconcile_ReversalFailureIsRetriedAndDoesNotStopTheRest(t *testing.T) {
	first, second := indeterminateTx(t), indeterminateTx(t)
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{first, second}}
	acquirer := &fakeAcquirer{reverseErr: errors.New("host down")}
	publisher := &fakePublisher{}

	resolved, failed, err := reconciler(repo, acquirer, publisher).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 0 || failed != 2 {
		t.Fatalf("ReconcileUnresolved() = %d, %d, %v", resolved, failed, err)
	}
	if acquirer.reverseCalls != 2 {
		t.Errorf("Reverse calls = %d, want 2", acquirer.reverseCalls)
	}
	if first.State() != valueobject.StateIndeterminate || publisher.rejectedCalls != 0 {
		t.Errorf("state = %s, rejected = %d", first.State(), publisher.rejectedCalls)
	}
}

// Dos instancias resuelven lo mismo: solo una informa.
func TestReconcile_LostRaceIsNotPublished(t *testing.T) {
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{indeterminateTx(t)}, resultLost: true}
	publisher := &fakePublisher{}

	resolved, failed, err := reconciler(repo, &fakeAcquirer{}, publisher).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 0 || failed != 0 {
		t.Fatalf("ReconcileUnresolved() = %d, %d, %v", resolved, failed, err)
	}
	if publisher.rejectedCalls != 0 {
		t.Errorf("PublishRejected calls = %d, want 0", publisher.rejectedCalls)
	}
}

func TestReconcile_SaveErrorCountsAsFailed(t *testing.T) {
	repo := &fakeRepo{unresolved: []*aggregate.Transaction{processingTx(t)}, failSaveAt: 1}
	acquirer := &fakeAcquirer{}

	_, failed, err := reconciler(repo, acquirer, &fakePublisher{}).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || failed != 1 {
		t.Fatalf("failed = %d, err = %v", failed, err)
	}
	if acquirer.reverseCalls != 0 {
		t.Errorf("Reverse calls = %d, want 0: it must be marked before reversing", acquirer.reverseCalls)
	}
}

func TestReconcile_FindError(t *testing.T) {
	repo := &fakeRepo{unresolvedErr: errors.New("db down")}

	_, _, err := reconciler(repo, &fakeAcquirer{}, &fakePublisher{}).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err == nil || !strings.Contains(err.Error(), "ReconcileUnresolved") {
		t.Fatalf("error = %v", err)
	}
}

// ─── Reversas pendientes ──────────────────────────────────────────────────────

func TestReconcile_PendingReversalIsCompleted(t *testing.T) {
	tx := newApprovedTransaction(t)
	repo := &fakeRepo{pendingReversals: []*aggregate.Transaction{tx}}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}

	resolved, failed, err := reconciler(repo, acquirer, publisher).ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || resolved != 1 || failed != 0 {
		t.Fatalf("ReconcileUnresolved() = %d, %d, %v", resolved, failed, err)
	}
	if tx.State() != valueobject.StateReversed || acquirer.reverseCalls != 1 {
		t.Errorf("state = %s, reverse = %d", tx.State(), acquirer.reverseCalls)
	}
	// Settlement ya registró la venta: acá sí corresponde informar la reversa.
	if publisher.reversalCalls != 1 || publisher.rejectedCalls != 0 {
		t.Errorf("reversal = %d, rejected = %d", publisher.reversalCalls, publisher.rejectedCalls)
	}
}

func TestReconcile_PendingReversalFailureIsRetried(t *testing.T) {
	tx := newApprovedTransaction(t)
	repo := &fakeRepo{pendingReversals: []*aggregate.Transaction{tx}}
	publisher := &fakePublisher{}

	_, failed, err := reconciler(repo, &fakeAcquirer{reverseErr: errors.New("host down")}, publisher).
		ReconcileUnresolved(context.Background(), time.Now(), time.Minute)
	if err != nil || failed != 1 {
		t.Fatalf("failed = %d, err = %v", failed, err)
	}
	if tx.State() != valueobject.StateApproved || publisher.reversalCalls != 0 {
		t.Errorf("state = %s, reversal = %d", tx.State(), publisher.reversalCalls)
	}
}

func TestReconcile_PendingReversalsFindError(t *testing.T) {
	repo := &fakeRepo{pendingErr: errors.New("db down")}

	if _, _, err := reconciler(repo, &fakeAcquirer{}, &fakePublisher{}).ReconcileUnresolved(context.Background(), time.Now(), time.Minute); err == nil {
		t.Fatal("ReconcileUnresolved() error = nil, want error")
	}
}

// ─── Llamadas fuera del handler ───────────────────────────────────────────────

type blockingAcquirer struct {
	fakeAcquirer
	started chan struct{}
	release chan struct{}
	timeout chan bool
}

func (a *blockingAcquirer) Authorize(ctx context.Context, _ *aggregate.Transaction) (service.AcquirerResponse, error) {
	_, hasDeadline := ctx.Deadline()
	a.timeout <- hasDeadline
	a.started <- struct{}{}
	<-a.release
	return service.AcquirerResponse{ResponseCode: valueobject.ISO_APPROVED, AuthCode: "AB1234"}, nil
}

// Un adquirente lento no retiene el handler: ApplyFraudScore vuelve con la llamada en curso.
func TestApplyFraudScore_CallRunsOutsideTheHandler(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &blockingAcquirer{started: make(chan struct{}, 1), release: make(chan struct{}), timeout: make(chan bool, 1)}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(repo, acquirer, publisher, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	h.ConfigureAcquirerCalls(1, time.Minute)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	<-acquirer.started
	if !<-acquirer.timeout {
		t.Error("the acquirer call has no timeout")
	}
	if publisher.approvedCalls != 0 {
		t.Error("the result should not be published while the call is in progress")
	}

	close(acquirer.release)
	h.WaitAcquirerCalls(context.Background())
	if publisher.approvedCalls != 1 {
		t.Errorf("PublishApproved calls = %d, want 1", publisher.approvedCalls)
	}
}

// Con todos los workers ocupados, la siguiente llamada espera: no se acumulan sin límite.
func TestRunAcquirerCalls_AreBounded(t *testing.T) {
	first, second := newFraudCheckingTransaction(t), newFraudCheckingTransaction(t)
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: first}
	acquirer := &blockingAcquirer{started: make(chan struct{}, 2), release: make(chan struct{}), timeout: make(chan bool, 2)}
	h := command.NewAuthorizationHandler(repo, acquirer, &fakePublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	h.ConfigureAcquirerCalls(1, time.Minute)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, first.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	<-acquirer.started

	repo.findResult = second
	returned := make(chan error, 1)
	go func() { returned <- h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, second.ID())) }()
	select {
	case <-returned:
		t.Fatal("the second call should wait for a free worker")
	case <-time.After(100 * time.Millisecond):
	}

	close(acquirer.release)
	if err := <-returned; err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	h.WaitAcquirerCalls(context.Background())
}

// Un apagado no espera para siempre: lo que quede lo retoma el conciliador.
func TestWaitAcquirerCalls_RespectsTheDeadline(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &blockingAcquirer{started: make(chan struct{}, 1), release: make(chan struct{}), timeout: make(chan bool, 1)}
	h := command.NewAuthorizationHandler(repo, acquirer, &fakePublisher{}, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
	h.ConfigureAcquirerCalls(1, time.Minute)
	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	<-acquirer.started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	h.WaitAcquirerCalls(ctx)
	if ctx.Err() == nil {
		t.Error("it should have waited until the deadline")
	}
	close(acquirer.release)
	h.WaitAcquirerCalls(context.Background())
}
