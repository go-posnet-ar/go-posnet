package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func TestProcessReversal_DuplicateEventIsSkipped(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0)

	repo := &fakeRepo{}
	acquirer := &fakeAcquirer{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ProcessReversal(context.Background(), validReversalCmd(t, domain.NewTransactionID()))
	if err != nil {
		t.Fatalf("ProcessReversal() error = %v", err)
	}
	if acquirer.reverseCalls != 0 {
		t.Errorf("Reverse calls = %d, want 0", acquirer.reverseCalls)
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestProcessReversal_ClaimError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ProcessReversal(context.Background(), validReversalCmd(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "claim event") {
		t.Fatalf("error = %v, want it to contain %q", err, "claim event")
	}
}

func TestProcessReversal_InvalidTransactionID(t *testing.T) {
	pool := newMockPool(t)

	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validReversalCmd(t, domain.NewTransactionID())
	cmd.OriginalTransactionID = "not-a-uuid"

	err := h.ProcessReversal(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestProcessReversal_FindByIDError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	repo := &fakeRepo{findErr: errors.New("not found")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ProcessReversal(context.Background(), validReversalCmd(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "find transaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "find transaction")
	}
}

func TestProcessReversal_AcquirerErrorLeavesForReconciliation(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newApprovedTransaction(t)}
	acquirer := &fakeAcquirer{reverseErr: errors.New("acquirer timeout")}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID()))
	if err != nil {
		t.Fatalf("ProcessReversal() error = %v, want nil (acquirer failure is left for reconciliation)", err)
	}
	if repo.reversalMarks != 1 {
		t.Errorf("reversal marks = %d, want 1: the request must survive the failure", repo.reversalMarks)
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
	if publisher.reversalCalls != 0 {
		t.Errorf("PublishReversalCompleted calls = %d, want 0", publisher.reversalCalls)
	}
}

// Si no se puede guardar, queda en APPROVED con el pedido: el conciliador la reintenta.
func TestProcessReversal_SaveErrorIsLeftForTheReconciler(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newApprovedTransaction(t), saveErr: errors.New("db unreachable")}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ProcessReversal() error = %v", err)
	}
	if publisher.reversalCalls != 0 {
		t.Errorf("PublishReversalCompleted calls = %d, want 0", publisher.reversalCalls)
	}
}
func TestProcessReversal_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // REVERSED y su evento en el outbox

	repo := &fakeRepo{findResult: newApprovedTransaction(t)}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ProcessReversal() error = %v", err)
	}
	if acquirer.reverseCalls != 1 {
		t.Errorf("Reverse calls = %d, want 1", acquirer.reverseCalls)
	}
	if len(repo.savedTxs) != 1 || repo.savedTxs[0].State() != valueobject.StateReversed {
		t.Fatalf("saved tx = %+v, want a single tx in state REVERSED", repo.savedTxs)
	}
	if publisher.reversalCalls != 1 {
		t.Errorf("PublishReversalCompleted calls = %d, want 1", publisher.reversalCalls)
	}
}

// La reversa completada se encola en la transacción que la guarda: si falla, no queda ninguna de
// las dos. Sigue en APPROVED con el pedido guardado, y el conciliador la reintenta.
func TestProcessReversal_EnqueueErrorIsRetriedByTheReconciler(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectRollback()

	repo := &fakeRepo{findResult: newApprovedTransaction(t)}
	publisher := &fakePublisher{reversalErr: errors.New("outbox insert failed")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ProcessReversal() error = %v, want nil: el pedido ya quedó guardado", err)
	}
	if repo.reversalMarks != 1 {
		t.Errorf("marks = %d, want 1: el pedido tiene que quedar para el conciliador", repo.reversalMarks)
	}
}

// El estado se valida antes de llamar al adquirente.
func TestProcessReversal_ByState(t *testing.T) {
	cases := []struct {
		name     string
		tx       func(*testing.T) *aggregate.Transaction
		marks    int
		reverses int
	}{
		{"aprobada", newApprovedTransaction, 1, 1},
		{"esperando el fraude: se guarda el pedido", newFraudCheckingTransaction, 1, 0},
		{"en el adquirente: se guarda el pedido", processingTx, 1, 0},
		{"indeterminada: la revierte el conciliador", indeterminateTx, 0, 0},
		{"rechazada", rejectedTx, 0, 0},
		{"ya revertida", reversedTx, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			if tc.reverses > 0 {
				expectTx(pool) // REVERSED y su evento en el outbox
			}

			repo := &fakeRepo{findResult: tc.tx(t)}
			acquirer := &fakeAcquirer{}
			publisher := &fakePublisher{}
			h := command.NewAuthorizationHandler(
				repo, acquirer, publisher,
				natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
			)

			if err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID())); err != nil {
				t.Fatalf("ProcessReversal() error = %v", err)
			}
			if repo.reversalMarks != tc.marks {
				t.Errorf("marks = %d, want %d", repo.reversalMarks, tc.marks)
			}
			if acquirer.reverseCalls != tc.reverses {
				t.Errorf("Reverse calls = %d, want %d", acquirer.reverseCalls, tc.reverses)
			}
			if publisher.reversalCalls != tc.reverses {
				t.Errorf("PublishReversalCompleted calls = %d, want %d", publisher.reversalCalls, tc.reverses)
			}
		})
	}
}

// Sin poder guardar el pedido, no se llama al adquirente y el evento se reintenta.
func TestProcessReversal_MarkErrorDoesNotCallAcquirer(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	repo := &fakeRepo{findResult: newApprovedTransaction(t), markErr: errors.New("db unreachable")}
	acquirer := &fakeAcquirer{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ProcessReversal(context.Background(), validReversalCmd(t, repo.findResult.ID())); err == nil {
		t.Fatal("ProcessReversal() error = nil, want an error so the event is redelivered")
	}
	if acquirer.reverseCalls != 0 {
		t.Errorf("Reverse calls = %d, want 0", acquirer.reverseCalls)
	}
}

func rejectedTx(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := processingTx(t)
	if err := tx.Reject(valueobject.NewRejectionFromTimeout()); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	return tx
}

func reversedTx(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newApprovedTransaction(t)
	if err := tx.Reverse(); err != nil {
		t.Fatalf("Reverse() error = %v", err)
	}
	return tx
}
