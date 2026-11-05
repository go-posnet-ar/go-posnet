package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/command"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

func newHandler(
	pool pgutil.PgxPool,
	sessionRepo *fakeSessionRepo, terminalRepo *fakeTerminalRepo,
	notifier *fakeNotifier, publisher *fakePublisher,
) *command.SessionHandler {
	return command.NewSessionHandler(
		sessionRepo, terminalRepo, notifier, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema),
		outbox.NewStore(idempotencySchema),
		pool,
	)
}

// ─── CreateSession ─────────────────────────────────────────────────────────────

func validCreateSessionCmd(terminalID, merchantID string) port.CreateSessionCommand {
	return port.CreateSessionCommand{
		TerminalID:     terminalID,
		MerchantID:     merchantID,
		AmountCents:    1000,
		Currency:       "ARS",
		STAN:           123456,
		PaymentChannel: "QR",
	}
}

func TestCreateSession_ValidationErrors(t *testing.T) {
	terminalID := domain.NewTerminalID().String()
	merchantID := domain.NewMerchantID().String()

	tests := []struct {
		name string
		mut  func(*port.CreateSessionCommand)
	}{
		{"invalid terminal_id", func(c *port.CreateSessionCommand) { c.TerminalID = "not-a-uuid" }},
		{"invalid merchant_id", func(c *port.CreateSessionCommand) { c.MerchantID = "not-a-uuid" }},
		{"invalid currency", func(c *port.CreateSessionCommand) { c.Currency = "XXX" }},
		{"invalid amount", func(c *port.CreateSessionCommand) { c.AmountCents = 0 }},
		{"invalid stan", func(c *port.CreateSessionCommand) { c.STAN = 0 }},
		{"invalid payment channel", func(c *port.CreateSessionCommand) { c.PaymentChannel = "BOGUS" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
			cmd := validCreateSessionCmd(terminalID, merchantID)
			tc.mut(&cmd)

			_, err := h.CreateSession(context.Background(), cmd)
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
			}
		})
	}
}

func TestCreateSession_FindTerminalError(t *testing.T) {
	terminalRepo := &fakeTerminalRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, &fakeSessionRepo{}, terminalRepo, &fakeNotifier{}, &fakePublisher{})

	terminalID := domain.NewTerminalID().String()
	_, err := h.CreateSession(context.Background(), validCreateSessionCmd(terminalID, domain.NewMerchantID().String()))
	if err == nil || !strings.Contains(err.Error(), "CreateSession: find terminal") {
		t.Fatalf("error = %v, want it to mention CreateSession: find terminal", err)
	}
}

func TestCreateSession_TerminalNotActive(t *testing.T) {
	terminalID := domain.NewTerminalID()
	terminalRepo := &fakeTerminalRepo{findResult: blockedTerminal(terminalID)}
	h := newHandler(nil, &fakeSessionRepo{}, terminalRepo, &fakeNotifier{}, &fakePublisher{})

	_, err := h.CreateSession(context.Background(), validCreateSessionCmd(terminalID.String(), domain.NewMerchantID().String()))
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestCreateSession_SaveError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	terminalRepo := &fakeTerminalRepo{findResult: activeTerminal(terminalID)}
	sessionRepo := &fakeSessionRepo{saveErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, terminalRepo, &fakeNotifier{}, &fakePublisher{})

	_, err := h.CreateSession(context.Background(), validCreateSessionCmd(terminalID.String(), domain.NewMerchantID().String()))
	if err == nil || !strings.Contains(err.Error(), "CreateSession: save session") {
		t.Fatalf("error = %v, want it to mention CreateSession: save session", err)
	}
}

func TestCreateSession_NotifyErrorIsNonFatal(t *testing.T) {
	terminalID := domain.NewTerminalID()
	terminalRepo := &fakeTerminalRepo{findResult: activeTerminal(terminalID)}
	notifier := &fakeNotifier{notifySessionCreatedErr: errors.New("terminal not connected")}
	h := newHandler(nil, &fakeSessionRepo{}, terminalRepo, notifier, &fakePublisher{})

	result, err := h.CreateSession(context.Background(), validCreateSessionCmd(terminalID.String(), domain.NewMerchantID().String()))
	if err != nil {
		t.Fatalf("CreateSession() error = %v, want nil (notify failure is non-fatal)", err)
	}
	if result == nil {
		t.Fatal("result is nil, want a SessionCreatedResult")
	}
}

func TestCreateSession_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	terminalRepo := &fakeTerminalRepo{findResult: activeTerminal(terminalID)}
	sessionRepo := &fakeSessionRepo{}
	notifier := &fakeNotifier{}
	h := newHandler(nil, sessionRepo, terminalRepo, notifier, &fakePublisher{})

	result, err := h.CreateSession(context.Background(), validCreateSessionCmd(terminalID.String(), domain.NewMerchantID().String()))
	if err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if result.TransactionID == "" {
		t.Error("TransactionID is empty")
	}
	if result.Channel != "QR" {
		t.Errorf("Channel = %q, want %q", result.Channel, "QR")
	}
	if result.TTLSeconds <= 0 {
		t.Errorf("TTLSeconds = %d, want > 0", result.TTLSeconds)
	}
	if result.Amount.Cents() != 1000 {
		t.Errorf("Amount.Cents() = %d, want 1000", result.Amount.Cents())
	}
	if sessionRepo.saveCallCount != 1 {
		t.Errorf("Save call count = %d, want 1", sessionRepo.saveCallCount)
	}
	if notifier.notifySessionCreatedCall != 1 {
		t.Errorf("NotifySessionCreated call count = %d, want 1", notifier.notifySessionCreatedCall)
	}
}

// ─── ProcessPayment ─────────────────────────────────────────────────────────────

func TestProcessPayment_InvalidTransactionID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestProcessPayment_FindSessionError(t *testing.T) {
	sessionRepo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "ProcessPayment: find session") {
		t.Fatalf("error = %v, want it to mention ProcessPayment: find session", err)
	}
}

func TestProcessPayment_StartProcessingError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: approvedSession(t, terminalID)} // terminal — no puede reprocesar
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "ProcessPayment: start processing") {
		t.Fatalf("error = %v, want it to mention ProcessPayment: start processing", err)
	}
}

func TestProcessPayment_BuildEventError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)}
	publisher := &fakePublisher{buildErr: errors.New("marshal failed")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "ProcessPayment: build event") {
		t.Fatalf("error = %v, want it to mention ProcessPayment: build event", err)
	}
}

func TestProcessPayment_BeginTxError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "begin transaction") {
		t.Fatalf("error = %v, want it to mention begin transaction", err)
	}
}

func TestProcessPayment_SaveTxError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID), saveTxErr: errors.New("connection reset")}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectRollback()

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "save session") {
		t.Fatalf("error = %v, want it to mention save session", err)
	}
}

func TestProcessPayment_OutboxInsertError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".outbox").
		WithArgs("posnet.transaction.received.v1", "evt-1", []byte(`{}`)).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), `outbox: insert "evt-1"`) {
		t.Fatalf("error = %v, want it to mention outbox: insert", err)
	}
}

func TestProcessPayment_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	session := awaitingSession(t, terminalID)
	sessionRepo := &fakeSessionRepo{findResult: session}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".outbox").
		WithArgs("posnet.transaction.received.v1", "evt-1", []byte(`{}`)).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectCommit()
	pool.ExpectRollback()

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ProcessPayment(context.Background(), port.ProcessPaymentCommand{
		TransactionID: domain.NewTransactionID().String(),
		ISO8583Raw:    []byte("iso-raw"),
		EMVDataBase64: "emv-base64",
	})
	if err != nil {
		t.Fatalf("ProcessPayment() error = %v", err)
	}
	if session.State() != valueobject.StateProcessing {
		t.Errorf("State() = %v, want %v", session.State(), valueobject.StateProcessing)
	}
	if sessionRepo.saveCallCount != 1 {
		t.Errorf("SaveTx call count = %d, want 1", sessionRepo.saveCallCount)
	}
}

// ─── ApplyApproval ──────────────────────────────────────────────────────────────

func TestApplyApproval_InvalidTransactionID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestApplyApproval_FindSessionError(t *testing.T) {
	sessionRepo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "ApplyApproval: find session") {
		t.Fatalf("error = %v, want it to mention ApplyApproval: find session", err)
	}
}

func TestApplyApproval_ApproveError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)} // no puede aprobar directo desde AWAITING
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err == nil || !strings.Contains(err.Error(), "ApplyApproval: approve session") {
		t.Fatalf("error = %v, want it to mention ApplyApproval: approve session", err)
	}
}

func TestApplyApproval_DuplicateEventSkipsPersistAndNotify(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 0)
	notifier := &fakeNotifier{}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err != nil {
		t.Fatalf("ApplyApproval() error = %v", err)
	}
	if sessionRepo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 for a duplicate event", sessionRepo.saveCallCount)
	}
	if notifier.notifyResultCalls != 0 {
		t.Errorf("NotifyResult calls = %d, want 0", notifier.notifyResultCalls)
	}
}

func TestApplyApproval_ClaimExecError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))
	pool.ExpectRollback()

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err == nil || !strings.Contains(err.Error(), "ApplyApproval: persist") {
		t.Fatalf("error = %v, want it to mention ApplyApproval: persist", err)
	}
}

func TestApplyApproval_SaveError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID), saveErr: errors.New("connection reset")}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err == nil || !strings.Contains(err.Error(), "ApplyApproval: persist") {
		t.Fatalf("error = %v, want it to mention ApplyApproval: persist", err)
	}
}

func TestApplyApproval_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	notifier := &fakeNotifier{}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err != nil {
		t.Fatalf("ApplyApproval() error = %v", err)
	}
	if sessionRepo.saveCallCount != 1 {
		t.Errorf("Save call count = %d, want 1", sessionRepo.saveCallCount)
	}
	if notifier.notifyResultCalls != 1 {
		t.Errorf("NotifyResult calls = %d, want 1", notifier.notifyResultCalls)
	}
}

func TestApplyApproval_NotifyErrorIsNonFatal(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	notifier := &fakeNotifier{notifyResultErr: errors.New("terminal not connected")}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyApproval(context.Background(), port.ApplyApprovalCommand{TransactionID: domain.NewTransactionID().String(), AuthCode: "AUTH123"})
	if err != nil {
		t.Fatalf("ApplyApproval() error = %v, want nil (notify failure is non-fatal)", err)
	}
}

// ─── ApplyRejection ─────────────────────────────────────────────────────────────

func TestApplyRejection_InvalidTransactionID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestApplyRejection_FindSessionError(t *testing.T) {
	sessionRepo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String()})
	if err == nil || !strings.Contains(err.Error(), "ApplyRejection: find session") {
		t.Fatalf("error = %v, want it to mention ApplyRejection: find session", err)
	}
}

func TestApplyRejection_RejectError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err == nil || !strings.Contains(err.Error(), "ApplyRejection: reject session") {
		t.Fatalf("error = %v, want it to mention ApplyRejection: reject session", err)
	}
}

func TestApplyRejection_DuplicateEventSkipsPersistAndNotify(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 0)
	notifier := &fakeNotifier{}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err != nil {
		t.Fatalf("ApplyRejection() error = %v", err)
	}
	if sessionRepo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 for a duplicate event", sessionRepo.saveCallCount)
	}
	if notifier.notifyResultCalls != 0 {
		t.Errorf("NotifyResult calls = %d, want 0", notifier.notifyResultCalls)
	}
}

func TestApplyRejection_ClaimExecError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))
	pool.ExpectRollback()

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err == nil || !strings.Contains(err.Error(), "ApplyRejection: persist") {
		t.Fatalf("error = %v, want it to mention ApplyRejection: persist", err)
	}
}

func TestApplyRejection_SaveError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID), saveErr: errors.New("connection reset")}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})
	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err == nil || !strings.Contains(err.Error(), "ApplyRejection: persist") {
		t.Fatalf("error = %v, want it to mention ApplyRejection: persist", err)
	}
}

func TestApplyRejection_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	notifier := &fakeNotifier{}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err != nil {
		t.Fatalf("ApplyRejection() error = %v", err)
	}
	if sessionRepo.saveCallCount != 1 {
		t.Errorf("Save call count = %d, want 1", sessionRepo.saveCallCount)
	}
	if notifier.notifyResultCalls != 1 {
		t.Errorf("NotifyResult calls = %d, want 1", notifier.notifyResultCalls)
	}
}

func TestApplyRejection_NotifyErrorIsNonFatal(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	notifier := &fakeNotifier{notifyResultErr: errors.New("terminal not connected")}

	h := newHandler(pool, sessionRepo, &fakeTerminalRepo{}, notifier, &fakePublisher{})
	err := h.ApplyRejection(context.Background(), port.ApplyRejectionCommand{TransactionID: domain.NewTransactionID().String(), RejectionCode: "05", RejectionReason: "Do not honor"})
	if err != nil {
		t.Fatalf("ApplyRejection() error = %v, want nil (notify failure is non-fatal)", err)
	}
}

// ─── CancelSession ──────────────────────────────────────────────────────────────

func TestCancelSession_InvalidTransactionID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: "not-a-uuid", TerminalID: domain.NewTerminalID().String()})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestCancelSession_InvalidTerminalID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: domain.NewTransactionID().String(), TerminalID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestCancelSession_FindSessionError(t *testing.T) {
	sessionRepo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: domain.NewTransactionID().String(), TerminalID: domain.NewTerminalID().String()})
	if err == nil || !strings.Contains(err.Error(), "CancelSession: find session") {
		t.Fatalf("error = %v, want it to mention CancelSession: find session", err)
	}
}

func TestCancelSession_NotAuthorized(t *testing.T) {
	sessionOwner := domain.NewTerminalID()
	otherTerminal := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, sessionOwner)}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    otherTerminal.String(),
	})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError (otro terminal no debe poder cancelar)", err)
	}
	if sessionRepo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", sessionRepo.saveCallCount)
	}
}

func TestCancelSession_CancelError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: processingSession(t, terminalID)} // PROCESSING no puede ir directo a CANCELLED
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: domain.NewTransactionID().String(), TerminalID: terminalID.String()})
	if err == nil || !strings.Contains(err.Error(), "CancelSession: cancel") {
		t.Fatalf("error = %v, want it to mention CancelSession: cancel", err)
	}
}

func TestCancelSession_SaveError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID), saveErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: domain.NewTransactionID().String(), TerminalID: terminalID.String()})
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error = %v, want it to propagate the Save error", err)
	}
}

func TestCancelSession_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: awaitingSession(t, terminalID)}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.CancelSession(context.Background(), port.CancelSessionCommand{TransactionID: domain.NewTransactionID().String(), TerminalID: terminalID.String()})
	if err != nil {
		t.Fatalf("CancelSession() error = %v", err)
	}
	if sessionRepo.saveCallCount != 1 {
		t.Errorf("Save call count = %d, want 1", sessionRepo.saveCallCount)
	}
	if sessionRepo.lastSaved().State() != valueobject.StateCancelled {
		t.Errorf("State() = %v, want %v", sessionRepo.lastSaved().State(), valueobject.StateCancelled)
	}
}

// ─── RequestBatchClose ──────────────────────────────────────────────────────────

func TestRequestBatchClose_InvalidTerminalID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.RequestBatchClose(context.Background(), port.RequestBatchCloseCommand{TerminalID: "not-a-uuid", MerchantID: domain.NewMerchantID().String()})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestRequestBatchClose_InvalidMerchantID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.RequestBatchClose(context.Background(), port.RequestBatchCloseCommand{TerminalID: domain.NewTerminalID().String(), MerchantID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestRequestBatchClose_PublishError(t *testing.T) {
	publisher := &fakePublisher{publishBatchCloseErr: errors.New("nats unavailable")}
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.RequestBatchClose(context.Background(), port.RequestBatchCloseCommand{TerminalID: domain.NewTerminalID().String(), MerchantID: domain.NewMerchantID().String(), Currency: "ARS"})
	if err == nil || !strings.Contains(err.Error(), "nats unavailable") {
		t.Fatalf("error = %v, want it to propagate the publish error", err)
	}
}

func TestRequestBatchClose_Success(t *testing.T) {
	publisher := &fakePublisher{}
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.RequestBatchClose(context.Background(), port.RequestBatchCloseCommand{
		TerminalID: domain.NewTerminalID().String(), MerchantID: domain.NewMerchantID().String(),
		TerminalCount: 5, TerminalAmount: 5000, Currency: "ARS",
	})
	if err != nil {
		t.Fatalf("RequestBatchClose() error = %v", err)
	}
	if publisher.publishBatchCloseCall != 1 {
		t.Errorf("PublishBatchCloseRequested calls = %d, want 1", publisher.publishBatchCloseCall)
	}
}

// ─── RequestReversal ────────────────────────────────────────────────────────────

func TestRequestReversal_InvalidOriginalTransactionID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{OriginalTransactionID: "not-a-uuid", TerminalID: domain.NewTerminalID().String()})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestRequestReversal_InvalidTerminalID(t *testing.T) {
	h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{OriginalTransactionID: domain.NewTransactionID().String(), TerminalID: "not-a-uuid"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestRequestReversal_FindSessionError(t *testing.T) {
	sessionRepo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, &fakePublisher{})

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{OriginalTransactionID: domain.NewTransactionID().String(), TerminalID: domain.NewTerminalID().String()})
	if err == nil || !strings.Contains(err.Error(), "RequestReversal: find original session") {
		t.Fatalf("error = %v, want it to mention RequestReversal: find original session", err)
	}
}

func TestRequestReversal_NotAuthorized(t *testing.T) {
	sessionOwner := domain.NewTerminalID()
	otherTerminal := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: approvedSession(t, sessionOwner)}
	publisher := &fakePublisher{}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            otherTerminal.String(),
	})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError (otro terminal no debe poder pedir la anulación)", err)
	}
	if publisher.publishReversalCalls != 0 {
		t.Errorf("PublishReversalRequested calls = %d, want 0", publisher.publishReversalCalls)
	}
}

func TestRequestReversal_PublishError(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: approvedSession(t, terminalID)}
	publisher := &fakePublisher{publishReversalErr: errors.New("nats unavailable")}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            terminalID.String(),
	})
	if err == nil || !strings.Contains(err.Error(), "nats unavailable") {
		t.Fatalf("error = %v, want it to propagate the publish error", err)
	}
}

func TestRequestReversal_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	sessionRepo := &fakeSessionRepo{findResult: approvedSession(t, terminalID)}
	publisher := &fakePublisher{}
	h := newHandler(nil, sessionRepo, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)

	err := h.RequestReversal(context.Background(), port.RequestReversalCommand{
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            terminalID.String(),
	})
	if err != nil {
		t.Fatalf("RequestReversal() error = %v", err)
	}
	if publisher.publishReversalCalls != 1 {
		t.Errorf("PublishReversalRequested calls = %d, want 1", publisher.publishReversalCalls)
	}
}

func TestRequestBatchClose_InvalidCurrencyOrDate(t *testing.T) {
	tests := []struct {
		name                string
		currency, batchDate string
	}{
		{"missing currency", "", ""},
		{"unknown currency", "XXX", ""},
		{"invalid date", "ARS", "05/11/2026"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &fakePublisher{}
			h := newHandler(nil, &fakeSessionRepo{}, &fakeTerminalRepo{}, &fakeNotifier{}, publisher)
			err := h.RequestBatchClose(context.Background(), port.RequestBatchCloseCommand{
				TerminalID: domain.NewTerminalID().String(), MerchantID: domain.NewMerchantID().String(),
				Currency: tc.currency, BatchDate: tc.batchDate,
			})
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
			}
			if publisher.publishBatchCloseCall != 0 {
				t.Errorf("publish calls = %d, want 0", publisher.publishBatchCloseCall)
			}
		})
	}
}
