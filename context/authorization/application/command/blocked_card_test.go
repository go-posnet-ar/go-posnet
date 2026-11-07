package command_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// newHandlerWithBlocklist arma el handler con la blocklist inyectada.
func newHandlerWithBlocklist(t *testing.T, pool pgxmock.PgxPoolIface, repo *fakeRepo, acquirer *fakeAcquirer, pub *fakePublisher, bl *fakeBlockedCards) *command.AuthorizationHandler {
	t.Helper()
	h := command.NewAuthorizationHandler(
		repo, acquirer, pub,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)
	h.SetBlockedCards(bl)
	return h
}

func TestAuthorizeTransaction_BlockedCardIsRejectedBeforeFraudCheck(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{}
	publisher := &fakePublisher{}
	h := newHandlerWithBlocklist(t, pool, repo, &fakeAcquirer{}, publisher, &fakeBlockedCards{blocked: true})

	cmd := validAuthorizeCmd(t)
	cmd.CardToken = testCardToken

	if err := h.AuthorizeTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}

	if len(repo.savedTxs) != 1 {
		t.Fatalf("saved txs = %d, want 1", len(repo.savedTxs))
	}
	tx := repo.savedTxs[0]
	if tx.State() != valueobject.StateRejected {
		t.Errorf("state = %v, want %v", tx.State(), valueobject.StateRejected)
	}

	rc := tx.RejectionCode()
	if rc == nil {
		t.Fatal("RejectionCode() = nil, want a blocklist rejection")
	}
	if rc.Source() != valueobject.SourceBlocklist {
		t.Errorf("rejection source = %v, want %v", rc.Source(), valueobject.SourceBlocklist)
	}
	if !rc.RequiresCardCapture() {
		t.Error("RequiresCardCapture() = false, want true — la tarjeta bloqueada debe retenerse")
	}

	if publisher.fraudCheckCalls != 0 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 0 — no se consulta fraude por una tarjeta ya bloqueada", publisher.fraudCheckCalls)
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
}

// Si la tarjeta se bloqueó después de la venta original, un alta repetida no puede publicar un
// rechazo de una transacción que quizás ya se aprobó.
func TestAuthorizeTransaction_BlockedCardDoesNotRejectExistingTransaction(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	publisher := &fakePublisher{}
	h := newHandlerWithBlocklist(t, pool, &fakeRepo{alreadyExists: true}, &fakeAcquirer{}, publisher, &fakeBlockedCards{blocked: true})

	cmd := validAuthorizeCmd(t)
	cmd.CardToken = testCardToken

	if err := h.AuthorizeTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}
	if publisher.rejectedCalls != 0 {
		t.Errorf("PublishRejected calls = %d, want 0", publisher.rejectedCalls)
	}
}

func TestAuthorizeTransaction_TokenlessCardIsNeverBlocked(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{}
	publisher := &fakePublisher{}
	// La blocklist responde "bloqueada" a cualquier consulta: aun así, sin
	// token no hay tarjeta que identificar y la Saga debe seguir normal.
	h := newHandlerWithBlocklist(t, pool, repo, &fakeAcquirer{}, publisher, &fakeBlockedCards{blocked: true})

	cmd := validAuthorizeCmd(t)
	cmd.CardToken = ""

	if err := h.AuthorizeTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}

	if len(repo.savedTxs) != 1 {
		t.Fatalf("saved txs = %d, want 1", len(repo.savedTxs))
	}
	if got := repo.savedTxs[0].State(); got != valueobject.StateFraudChecking {
		t.Errorf("state = %v, want %v", got, valueobject.StateFraudChecking)
	}
	if publisher.fraudCheckCalls != 1 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 1", publisher.fraudCheckCalls)
	}
}

func TestAuthorizeTransaction_BlocklistErrorDoesNotRejectTheTransaction(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{}
	publisher := &fakePublisher{}
	bl := &fakeBlockedCards{isBlockErr: errors.New("db unreachable")}
	h := newHandlerWithBlocklist(t, pool, repo, &fakeAcquirer{}, publisher, bl)

	cmd := validAuthorizeCmd(t)
	cmd.CardToken = testCardToken

	if err := h.AuthorizeTransaction(context.Background(), cmd); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}

	if len(repo.savedTxs) != 1 {
		t.Fatalf("saved txs = %d, want 1", len(repo.savedTxs))
	}
	if got := repo.savedTxs[0].State(); got != valueobject.StateFraudChecking {
		t.Errorf("state = %v, want %v — un error de la blocklist no debe rechazar ni retener la tarjeta", got, valueobject.StateFraudChecking)
	}
}

func TestAuthorizeTransaction_InvalidCardTokenIsRejectedAsValidation(t *testing.T) {
	pool := newMockPool(t)

	h := newHandlerWithBlocklist(t, pool, &fakeRepo{}, &fakeAcquirer{}, &fakePublisher{}, &fakeBlockedCards{})

	cmd := validAuthorizeCmd(t)
	cmd.CardToken = "1234"

	err := h.AuthorizeTransaction(context.Background(), cmd)
	if err == nil {
		t.Fatal("AuthorizeTransaction() error = nil, want a validation error")
	}
}

func TestApplyFraudScore_CaptureOrderBlocksTheCard(t *testing.T) {
	tests := []struct {
		name      string
		isoCode   string
		wantBlock bool
	}{
		{name: "stolen card", isoCode: valueobject.ISO_STOLEN_CARD, wantBlock: true},
		{name: "lost card", isoCode: valueobject.ISO_LOST_CARD, wantBlock: true},
		{name: "capture card", isoCode: valueobject.ISO_CAPTURE_CARD, wantBlock: true},
		{name: "insufficient funds", isoCode: valueobject.ISO_INSUFFICIENT_FUNDS, wantBlock: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			expectClaimed(pool, 1)

			tx := newFraudCheckingTransactionWithToken(t)
			repo := &fakeRepo{findResult: tx}
			acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: tc.isoCode}}
			bl := &fakeBlockedCards{}
			h := newHandlerWithBlocklist(t, pool, repo, acquirer, &fakePublisher{}, bl)

			if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, tx.ID())); err != nil {
				t.Fatalf("ApplyFraudScore() error = %v", err)
			}

			if tc.wantBlock {
				if len(bl.blockCalls) != 1 {
					t.Fatalf("Block calls = %d, want 1", len(bl.blockCalls))
				}
				call := bl.blockCalls[0]
				if call.token.String() != testCardToken {
					t.Errorf("blocked token = %q, want %q", call.token, testCardToken)
				}
				if call.reason != tc.isoCode {
					t.Errorf("block reason = %q, want %q", call.reason, tc.isoCode)
				}
				if call.txID.String() != tx.ID().String() {
					t.Errorf("source tx = %q, want %q", call.txID, tx.ID())
				}
				return
			}
			if len(bl.blockCalls) != 0 {
				t.Errorf("Block calls = %d, want 0 — solo 04/41/43 bloquean la tarjeta", len(bl.blockCalls))
			}
		})
	}
}

func TestApplyFraudScore_BlocklistWriteFailureStillRejects(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	tx := newFraudCheckingTransactionWithToken(t)
	repo := &fakeRepo{findResult: tx}
	acquirer := &fakeAcquirer{authorizeResp: service.AcquirerResponse{ResponseCode: valueobject.ISO_STOLEN_CARD}}
	publisher := &fakePublisher{}
	bl := &fakeBlockedCards{blockErr: errors.New("db unreachable")}
	h := newHandlerWithBlocklist(t, pool, repo, acquirer, publisher, bl)

	if err := h.ApplyFraudScore(context.Background(), validFraudScoreCmd(t, tx.ID())); err != nil {
		t.Fatalf("ApplyFraudScore() error = %v — el rechazo no debe depender de la escritura en la blocklist", err)
	}
	if tx.State() != valueobject.StateRejected {
		t.Errorf("state = %v, want %v", tx.State(), valueobject.StateRejected)
	}
	if publisher.rejectedCalls != 1 {
		t.Errorf("PublishRejected calls = %d, want 1", publisher.rejectedCalls)
	}
}

// newFraudCheckingTransactionWithToken arma una Transaction en FRAUD_CHECKING
// que sí trae token de tarjeta — es la única que puede terminar bloqueada.
func newFraudCheckingTransactionWithToken(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tok, err := domain.NewCardToken(testCardToken)
	if err != nil {
		t.Fatalf("NewCardToken() error = %v", err)
	}
	tx, err := aggregate.NewTransaction(
		domain.NewTransactionID(),
		domain.NewTerminalID(),
		domain.NewMerchantID(),
		mustMoney(t, 5000),
		mustSTAN(t, 1),
		mustPAN(t),
		valueobject.EntryModeChip,
		tok,
		"emv-data-base64",
		[]byte{0xAA, 0xBB},
	)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	return tx
}
