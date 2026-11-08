package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func TestApplyFraudScore_DuplicateEventIsSkipped(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0)

	repo := &fakeRepo{}
	acquirer := &fakeAcquirer{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, domain.NewTransactionID()))
	if err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if acquirer.authorizeCalls != 0 {
		t.Errorf("Authorize calls = %d, want 0", acquirer.authorizeCalls)
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
}

func TestApplyFraudScore_ClaimError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "claim event") {
		t.Fatalf("error = %v, want it to contain %q", err, "claim event")
	}
}

func TestApplyFraudScore_InvalidTransactionID(t *testing.T) {
	pool := newMockPool(t)

	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validFraudScoreCmd(t, domain.NewTransactionID())
	cmd.TransactionID = "not-a-uuid"

	err := h.ApplyFraudScore(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestApplyFraudScore_FindByIDError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	repo := &fakeRepo{findErr: errors.New("not found")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "find transaction") {
		t.Fatalf("error = %v, want it to contain %q", err, "find transaction")
	}
}

func TestApplyFraudScore_InvalidFraudDecision(t *testing.T) {
	pool := newMockPool(t)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validFraudScoreCmd(t, repo.findResult.ID())
	cmd.Score = 999 // fuera de rango [0,100]

	err := h.ApplyFraudScore(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

// Una transacción que ya no espera el score no se toca: el mensaje se confirma, y el evento
// queda marcado para que un duplicado no vuelva a evaluarse.
func TestApplyFraudScore_WrongStateIsIgnored(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newApprovedTransaction(t)}
	acquirer := &fakeAcquirer{}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if acquirer.authorizeCalls != 0 || len(repo.claimed) != 0 || publisher.approvedCalls != 0 {
		t.Errorf("calls = %d, claimed = %v, approved = %d, want nothing", acquirer.authorizeCalls, repo.claimed, publisher.approvedCalls)
	}
}

func TestApplyFraudScore_RejectByFraud(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validFraudScoreCmd(t, repo.findResult.ID())
	cmd.Decision = valueobject.FraudDecisionReject
	cmd.Score = 90

	if err := h.ApplyFraudScore(context.Background(), cmd); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	want := claimedSave{state: valueobject.StateRejected, expected: valueobject.StateFraudChecking}
	if len(repo.claimed) != 1 || repo.claimed[0] != want {
		t.Fatalf("claimed = %+v, want [%+v]", repo.claimed, want)
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
}

// Si el rechazo no se puede guardar, el evento tampoco queda marcado: la reentrega reprocesa.
func TestApplyFraudScore_RejectByFraud_SaveError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	publisher := &fakePublisher{}
	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), claimErr: errors.New("db unreachable")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validFraudScoreCmd(t, repo.findResult.ID())
	cmd.Decision = valueobject.FraudDecisionReject
	cmd.Score = 90

	err := h.ApplyFraudScore(context.Background(), cmd)
	if err == nil || !strings.Contains(err.Error(), "persist decision") {
		t.Fatalf("error = %v, want it to contain %q", err, "persist decision")
	}
	if publisher.rejectedCalls != 0 {
		t.Errorf("PublishRejected calls = %d, want 0", publisher.rejectedCalls)
	}
}

// El rechazo por fraude se encola en la transacción del score: si falla, no queda ni el estado
// ni el evento marcado, y la reentrega lo reintenta.
func TestApplyFraudScore_RejectByFraud_EnqueueErrorRollsBack(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	publisher := &fakePublisher{rejectedErr: errors.New("outbox insert failed")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	cmd := validFraudScoreCmd(t, repo.findResult.ID())
	cmd.Decision = valueobject.FraudDecisionReject
	cmd.Score = 90

	err := h.ApplyFraudScore(context.Background(), cmd)
	if err == nil || !strings.Contains(err.Error(), "enqueue rejection") {
		t.Fatalf("ApplyFraudScore() error = %v, want el error del encolado, para reintentar", err)
	}
}

func TestApplyFraudScore_ApproveThenAcquirerApproves(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // el resultado y su evento en el outbox

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{
		ResponseCode: valueobject.ISO_APPROVED,
		AuthCode:     "AB1234",
	}}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if len(repo.savedTxs) != 1 || repo.savedTxs[0].State() != valueobject.StateApproved {
		t.Fatalf("saved tx = %+v, want a single tx in state APPROVED", repo.savedTxs)
	}
	if ac := repo.savedTxs[0].AuthCode(); ac == nil || ac.String() != "AB1234" {
		t.Errorf("AuthCode() = %v, want AB1234", ac)
	}
	if publisher.approvedCalls != 1 {
		t.Errorf("PublishApproved calls = %d, want 1", publisher.approvedCalls)
	}
}

// La aprobación se encola en la transacción que la guarda: si falla, no queda ninguna de las dos,
// y la transacción sigue en PROCESSING, donde la toma el conciliador.
func TestApplyFraudScore_ApproveThenEnqueueErrorLeavesItProcessing(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectRollback() // sin commit: el guardado y el evento se deshacen juntos

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{
		ResponseCode: valueobject.ISO_APPROVED,
		AuthCode:     "AB1234",
	}}
	publisher := &fakePublisher{approvedErr: errors.New("outbox insert failed")}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID()))
	if err == nil || !strings.Contains(err.Error(), "persist result") {
		t.Fatalf("ApplyFraudScore() error = %v, want el error del guardado del resultado", err)
	}
}

func TestApplyFraudScore_AcquirerErrorMarksIndeterminate(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeErr: errors.New("acquirer timeout")}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID()))
	if err != nil {
		t.Fatalf("ApplyFraudScore() error = %v, want nil (an acquirer error becomes INDETERMINATE, not a saga failure)", err)
	}
	if len(repo.savedTxs) != 1 || repo.savedTxs[0].State() != valueobject.StateIndeterminate {
		t.Fatalf("saved tx = %+v, want a single tx in state INDETERMINATE", repo.savedTxs)
	}
}

func TestApplyFraudScore_AcquirerRejectsWithISOCode(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // el resultado y su evento en el outbox

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: valueobject.ISO_DO_NOT_HONOR}}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	rc := repo.savedTxs[0].RejectionCode()
	if rc == nil || rc.Code() != valueobject.ISO_DO_NOT_HONOR {
		t.Fatalf("RejectionCode() = %v, want code %q", rc, valueobject.ISO_DO_NOT_HONOR)
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
}

func TestApplyFraudScore_AcquirerRejectsThenEnqueueErrorLeavesItProcessing(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectRollback()

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: valueobject.ISO_DO_NOT_HONOR}}
	publisher := &fakePublisher{rejectedErr: errors.New("outbox insert failed")}
	h := command.NewAuthorizationHandler(
		repo, acquirer, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID()))
	if err == nil || !strings.Contains(err.Error(), "persist result") {
		t.Fatalf("ApplyFraudScore() error = %v, want el error del guardado del resultado", err)
	}
}

func TestApplyFraudScore_AcquirerRejectsWithUnknownResponseCode(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // el resultado y su evento en el outbox

	// ResponseCode vacío → AcquirerResponse.ToRejectionCode() falla →
	// callAcquirer cae al fallback NewRejectionFromValidation("UNKNOWN_RESPONSE").
	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: ""}}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	rc := repo.savedTxs[0].RejectionCode()
	if rc == nil || rc.Code() != "UNKNOWN_RESPONSE" || rc.Source() != valueobject.SourceValidation {
		t.Fatalf("RejectionCode() = %v, want UNKNOWN_RESPONSE/VALIDATION", rc)
	}
}

func TestApplyFraudScore_ApproveWithInvalidAuthCode(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{
		ResponseCode: valueobject.ISO_APPROVED,
		AuthCode:     "not-6-chars",
	}}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID()))
	if err == nil || !strings.Contains(err.Error(), "invalid auth code") {
		t.Fatalf("error = %v, want it to contain %q", err, "invalid auth code")
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0 (should fail before persisting)", len(repo.savedTxs))
	}
}

func TestApplyFraudScore_SaveErrorAfterAcquirerApproval(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), saveErr: errors.New("db unreachable")}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{
		ResponseCode: valueobject.ISO_APPROVED,
		AuthCode:     "AB1234",
	}}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID()))
	if err == nil || !strings.Contains(err.Error(), "persist result") {
		t.Fatalf("error = %v, want it to contain %q", err, "persist result")
	}
}

// PROCESSING queda guardado, junto con el evento, antes de llamar al adquirente.
func TestApplyFraudScore_SavesProcessingBeforeCallingAcquirer(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // el resultado y su evento en el outbox

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
	acquirer := &orderedAcquirer{repo: repo, t: t}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if !acquirer.called {
		t.Fatal("the acquirer was not called")
	}
	if want := []valueobject.TransactionState{valueobject.StateProcessing}; len(repo.expected) != 1 || repo.expected[0] != want[0] {
		t.Errorf("result saved expecting %v, want %v", repo.expected, want)
	}
}

type orderedAcquirer struct {
	fakeAcquirer
	repo   *fakeRepo
	t      *testing.T
	called bool
}

func (a *orderedAcquirer) Authorize(context.Context, *aggregate.Transaction) (service.AcquirerResponse, error) {
	a.called = true
	want := claimedSave{state: valueobject.StateProcessing, expected: valueobject.StateFraudChecking}
	if len(a.repo.claimed) != 1 || a.repo.claimed[0] != want {
		a.t.Errorf("at the acquirer call, claimed = %+v, want [%+v]", a.repo.claimed, want)
	}
	return service.AcquirerResponse{ResponseCode: valueobject.ISO_APPROVED, AuthCode: "AB1234"}, nil
}

// Dos scores de la misma transacción: el que pierde no llega al adquirente.
func TestApplyFraudScore_LosingTheRaceDoesNotCallAcquirer(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), claimLost: true}
	acquirer := &fakeAcquirer{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if acquirer.authorizeCalls != 0 {
		t.Errorf("Authorize calls = %d, want 0", acquirer.authorizeCalls)
	}
}

// Sin poder guardar PROCESSING, no se llama al adquirente y el evento no queda marcado.
func TestApplyFraudScore_ProcessingSaveErrorDoesNotCallAcquirer(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), claimErr: errors.New("db unreachable")}
	acquirer := &fakeAcquirer{}
	h := command.NewAuthorizationHandler(
		repo, acquirer, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err == nil {
		t.Fatal("ApplyFraudScore() error = nil, want an error so the event is redelivered")
	}
	if acquirer.authorizeCalls != 0 {
		t.Errorf("Authorize calls = %d, want 0", acquirer.authorizeCalls)
	}
}

// El error al guardar INDETERMINATE ya no se ignora en silencio, pero tampoco rompe: la
// transacción sigue en PROCESSING y el conciliador la toma.
func TestApplyFraudScore_IndeterminateSaveErrorIsNotFatal(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), saveErr: errors.New("db unreachable")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{authorizeErr: errors.New("acquirer timeout")}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
}

// Una respuesta que llega después de que el conciliador tomó la transacción no se informa.
func TestApplyFraudScore_LateAcquirerResponseIsNotPublished(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectTx(pool) // el resultado y su evento en el outbox

	repo := &fakeRepo{findResult: newFraudCheckingTransaction(t), resultLost: true}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo,
		&fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: valueobject.ISO_APPROVED, AuthCode: "AB1234"}},
		publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v", err)
	}
	if publisher.approvedCalls != 0 {
		t.Errorf("PublishApproved calls = %d, want 0", publisher.approvedCalls)
	}
}

// Si la autorización seguro no salió, se rechaza sin pasar por INDETERMINATE ni revertir.
func TestApplyFraudScore_NotSentIsRejectedWithoutReversal(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code string
	}{
		{"sin conexión", service.ErrNotSent, valueobject.ISO_ISSUER_UNAVAILABLE},
		{"tarjeta desconocida", fmt.Errorf("%w: %w", service.ErrNotSent, service.ErrCardDataNotFound), valueobject.ISO_CARD_NOT_FOUND},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			expectTx(pool) // el resultado y su evento en el outbox

			repo := &fakeRepo{findResult: newFraudCheckingTransaction(t)}
			publisher := &fakePublisher{}
			acquirer := &fakeAcquirer{authorizeErr: tc.err}
			h := command.NewAuthorizationHandler(repo, acquirer, publisher, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)

			if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, repo.findResult.ID())); err != nil {
				t.Fatalf("ApplyFraudScore() error = %v", err)
			}
			tx := repo.savedTxs[0]
			if tx.State() != valueobject.StateRejected || tx.RejectionCode().Code() != tc.code {
				t.Errorf("state = %s, rejection = %v, want REJECTED %s", tx.State(), tx.RejectionCode(), tc.code)
			}
			if publisher.rejectedCalls != 1 || acquirer.reverseCalls != 0 {
				t.Errorf("rejected = %d, reverse = %d", publisher.rejectedCalls, acquirer.reverseCalls)
			}
		})
	}
}
