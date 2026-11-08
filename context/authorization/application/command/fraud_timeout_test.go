package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
)

func TestExpireFraudChecks_RejectsWithoutCallingTheAcquirer(t *testing.T) {
	tx := newFraudCheckingTransaction(t)
	repo := &fakeRepo{fraudExpired: []*aggregate.Transaction{tx}}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}

	expired, failed, err := reconciler(repo, acquirer, publisher).ExpireFraudChecks(context.Background(), time.Now(), 5*time.Second)
	if err != nil || expired != 1 || failed != 0 {
		t.Fatalf("ExpireFraudChecks() = %d, %d, %v, want 1, 0, nil", expired, failed, err)
	}
	if tx.State() != valueobject.StateRejected || tx.RejectionCode().Code() != valueobject.CodeFraudTimeout {
		t.Errorf("state = %s, rejection = %v, want REJECTED with %s", tx.State(), tx.RejectionCode(), valueobject.CodeFraudTimeout)
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
	// Nunca llegó al adquirente: no hay nada que autorizar ni que revertir.
	if acquirer.authorizeCalls != 0 || acquirer.reverseCalls != 0 {
		t.Errorf("acquirer calls = %d authorize, %d reverse, want none", acquirer.authorizeCalls, acquirer.reverseCalls)
	}
	// Se guarda solo si sigue esperando el score.
	if len(repo.claimed) != 1 || repo.claimed[0].expected != valueobject.StateFraudChecking {
		t.Errorf("saved = %+v, want one save expecting FRAUD_CHECKING", repo.claimed)
	}
}

// Si el score llegó antes, el rechazo no se guarda ni se publica.
func TestExpireFraudChecks_ScoreArrivedFirst(t *testing.T) {
	repo := &fakeRepo{fraudExpired: []*aggregate.Transaction{newFraudCheckingTransaction(t)}, claimLost: true}
	publisher := &fakePublisher{}

	expired, failed, err := reconciler(repo, &fakeAcquirer{}, publisher).ExpireFraudChecks(context.Background(), time.Now(), 5*time.Second)
	if err != nil || expired != 0 || failed != 0 {
		t.Fatalf("ExpireFraudChecks() = %d, %d, %v, want 0, 0, nil", expired, failed, err)
	}
	if publisher.rejectedCalls != 0 {
		t.Errorf("PublishRejected calls = %d, want 0", publisher.rejectedCalls)
	}
}

// Un error al encolar deshace el rechazo: la transacción sigue esperando y la corrida
// siguiente la vuelve a tomar.
func TestExpireFraudChecks_EnqueueErrorIsRetried(t *testing.T) {
	repo := &fakeRepo{fraudExpired: []*aggregate.Transaction{newFraudCheckingTransaction(t), newFraudCheckingTransaction(t)}}
	publisher := &fakePublisher{rejectedErr: errors.New("db down")}

	expired, failed, err := reconciler(repo, &fakeAcquirer{}, publisher).ExpireFraudChecks(context.Background(), time.Now(), 5*time.Second)
	if err != nil || expired != 0 || failed != 2 {
		t.Fatalf("ExpireFraudChecks() = %d, %d, %v, want 0, 2, nil", expired, failed, err)
	}
}

func TestExpireFraudChecks_FindError(t *testing.T) {
	repo := &fakeRepo{fraudExpiredErr: errors.New("db down")}

	_, _, err := reconciler(repo, &fakeAcquirer{}, &fakePublisher{}).ExpireFraudChecks(context.Background(), time.Now(), 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "ExpireFraudChecks") {
		t.Fatalf("ExpireFraudChecks() error = %v, want it to mention ExpireFraudChecks", err)
	}
}

// Una transacción que ya no está en FRAUD_CHECKING, por un cambio entre la consulta y la
// carga, se cuenta como fallida y no se toca.
func TestExpireFraudChecks_WrongStateIsSkipped(t *testing.T) {
	tx := processingTx(t)
	repo := &fakeRepo{fraudExpired: []*aggregate.Transaction{tx}}
	publisher := &fakePublisher{}

	expired, failed, err := reconciler(repo, &fakeAcquirer{}, publisher).ExpireFraudChecks(context.Background(), time.Now(), 5*time.Second)
	if err != nil || expired != 0 || failed != 1 {
		t.Fatalf("ExpireFraudChecks() = %d, %d, %v, want 0, 1, nil", expired, failed, err)
	}
	if tx.State() != valueobject.StateProcessing || publisher.rejectedCalls != 0 {
		t.Errorf("state = %s, PublishRejected calls = %d, want PROCESSING and 0", tx.State(), publisher.rejectedCalls)
	}
}
