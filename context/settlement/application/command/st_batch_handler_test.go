package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	"github.com/pashagolub/pgxmock/v4"
)

func newHandler(pool pgutil.PgxPool, batchRepo repository.SettlementBatchRepository, publisher service.EventPublisher, processor service.SettlementProcessor) *command.BatchHandler {
	return command.NewBatchHandler(batchRepo, publisher, outbox.NewStore(idempotencySchema), processor, natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
}

// ─── RegisterApproval ──────────────────────────────────────────────────────────

func validApprovalCmd() port.RegisterApprovalCommand {
	return port.RegisterApprovalCommand{
		EventID:       "evt-1",
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   1000,
		Currency:      "ARS",
		AuthorizedAt:  time.Now().UTC().Format(time.RFC3339),
	}
}

func TestRegisterApproval_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*port.RegisterApprovalCommand)
	}{
		{"invalid terminal_id", func(c *port.RegisterApprovalCommand) { c.TerminalID = "not-a-uuid" }},
		{"invalid merchant_id", func(c *port.RegisterApprovalCommand) { c.MerchantID = "not-a-uuid" }},
		{"invalid transaction_id", func(c *port.RegisterApprovalCommand) { c.TransactionID = "not-a-uuid" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
			cmd := validApprovalCmd()
			tc.mut(&cmd)

			err := h.RegisterApproval(context.Background(), cmd)
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
			}
		})
	}
}

func TestRegisterApproval_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection reset"))

	h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	if err == nil || !strings.Contains(err.Error(), "RegisterApproval") {
		t.Fatalf("error = %v, want it to mention RegisterApproval", err)
	}
}

func TestRegisterApproval_ClaimExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))
	pool.ExpectRollback()

	h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	if err == nil || !strings.Contains(err.Error(), "RegisterApproval") {
		t.Fatalf("error = %v, want it to mention RegisterApproval", err)
	}
}

func TestRegisterApproval_DuplicateEventSkipsPersist(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0) // duplicado

	repo := &fakeBatchRepo{}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.RegisterApproval(context.Background(), validApprovalCmd()); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 for a duplicate event", repo.saveCallCount)
	}
}

func TestRegisterApproval_FindOrCreateOpenError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	repo := &fakeBatchRepo{findOrCreateErr: errors.New("connection reset")}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	if err == nil || !strings.Contains(err.Error(), "find or create batch") {
		t.Fatalf("error = %v, want it to mention find or create batch", err)
	}
}

func TestRegisterApproval_AddTransactionError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	// Un batch que no está OPEN no acepta transacciones.
	batch := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateClosed, Currency: "ARS",
	})
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	if err == nil || !strings.Contains(err.Error(), "add transaction") {
		t.Fatalf("error = %v, want it to mention add transaction", err)
	}
}

func TestRegisterApproval_SaveError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch, saveErr: errors.New("connection reset")}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	if err == nil || !strings.Contains(err.Error(), "RegisterApproval") {
		t.Fatalf("error = %v, want it to mention RegisterApproval", err)
	}
}

func TestRegisterApproval_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.RegisterApproval(context.Background(), validApprovalCmd()); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}

	saved := repo.lastSaved()
	if saved == nil || len(saved.Transactions()) != 1 {
		t.Fatalf("saved batch transactions = %+v, want 1", saved)
	}
	if saved.Transactions()[0].Type() != valueobject.BatchTxPurchase {
		t.Errorf("Type() = %v, want %v", saved.Transactions()[0].Type(), valueobject.BatchTxPurchase)
	}
}

func TestRegisterApproval_InvalidAuthorizedAtFallsBackToNow(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	cmd := validApprovalCmd()
	cmd.AuthorizedAt = "not-a-date"

	if err := h.RegisterApproval(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterApproval() error = %v, want nil (falls back to time.Now())", err)
	}
}

// ─── RegisterReversal ──────────────────────────────────────────────────────────

func validReversalCmd() port.RegisterReversalCommand {
	return port.RegisterReversalCommand{
		EventID:               "evt-2",
		OriginalTransactionID: domain.NewTransactionID().String(),
		TerminalID:            domain.NewTerminalID().String(),
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           1000,
		Currency:              "ARS",
		CompletedAt:           time.Now().UTC().Format(time.RFC3339),
	}
}

func TestRegisterReversal_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*port.RegisterReversalCommand)
	}{
		{"invalid terminal_id", func(c *port.RegisterReversalCommand) { c.TerminalID = "not-a-uuid" }},
		{"invalid merchant_id", func(c *port.RegisterReversalCommand) { c.MerchantID = "not-a-uuid" }},
		{"invalid original_transaction_id", func(c *port.RegisterReversalCommand) { c.OriginalTransactionID = "not-a-uuid" }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
			cmd := validReversalCmd()
			tc.mut(&cmd)

			err := h.RegisterReversal(context.Background(), cmd)
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
			}
		})
	}
}

func TestRegisterReversal_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection reset"))

	h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	if err := h.RegisterReversal(context.Background(), validReversalCmd()); err == nil {
		t.Fatal("RegisterReversal() error = nil, want an error")
	}
}

func TestRegisterReversal_ClaimExecError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))
	pool.ExpectRollback()

	h := newHandler(pool, &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	if err := h.RegisterReversal(context.Background(), validReversalCmd()); err == nil {
		t.Fatal("RegisterReversal() error = nil, want an error")
	}
}

func TestRegisterReversal_DuplicateEventSkipsPersist(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0)

	repo := &fakeBatchRepo{}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.RegisterReversal(context.Background(), validReversalCmd()); err != nil {
		t.Fatalf("RegisterReversal() error = %v", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 for a duplicate event", repo.saveCallCount)
	}
}

func TestRegisterReversal_FindOrCreateOpenError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	repo := &fakeBatchRepo{findOrCreateErr: errors.New("connection reset")}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	err := h.RegisterReversal(context.Background(), validReversalCmd())
	if err == nil || !strings.Contains(err.Error(), "find or create batch") {
		t.Fatalf("error = %v, want it to mention find or create batch", err)
	}
}

func TestRegisterReversal_RemoveTransactionError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	// Un batch que no está OPEN no acepta RemoveTransaction.
	batch := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "batch-1",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateClosed,
		Currency:   "ARS",
		CreatedAt:  time.Now(),
	})
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	err := h.RegisterReversal(context.Background(), validReversalCmd())
	if err == nil || !strings.Contains(err.Error(), "remove transaction") {
		t.Fatalf("error = %v, want it to mention remove transaction", err)
	}
}

func TestRegisterReversal_SaveError(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch, saveErr: errors.New("connection reset")}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.RegisterReversal(context.Background(), validReversalCmd()); err == nil {
		t.Fatal("RegisterReversal() error = nil, want it to propagate the Save error")
	}
}

func TestRegisterReversal_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.RegisterReversal(context.Background(), validReversalCmd()); err != nil {
		t.Fatalf("RegisterReversal() error = %v", err)
	}

	saved := repo.lastSaved()
	if saved == nil || len(saved.Transactions()) != 1 {
		t.Fatalf("saved batch transactions = %+v, want 1", saved)
	}
	if saved.Transactions()[0].Type() != valueobject.BatchTxReversal {
		t.Errorf("Type() = %v, want %v", saved.Transactions()[0].Type(), valueobject.BatchTxReversal)
	}
}

func TestRegisterReversal_InvalidCompletedAtFallsBackToNow(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})

	cmd := validReversalCmd()
	cmd.CompletedAt = "not-a-date"

	if err := h.RegisterReversal(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterReversal() error = %v, want nil (falls back to time.Now())", err)
	}
}

// ─── ProcessBatchClose ──────────────────────────────────────────────────────────

func validProcessCloseCmd() port.ProcessBatchCloseCommand {
	return port.ProcessBatchCloseCommand{
		EventID:        "evt-3",
		TerminalID:     domain.NewTerminalID().String(),
		MerchantID:     domain.NewMerchantID().String(),
		BatchDate:      "2026-01-15",
		TerminalCount:  2,
		TerminalAmount: 3000,
		Currency:       "ARS",
	}
}

// closableFoundBatch produce un batch OPEN con 2 compras de 1000+2000 (total 3000,
// count 2) — coincide con validProcessCloseCmd() por defecto (sin discrepancias).
func closableFoundBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddTransaction(domain.NewTransactionID(), 2000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	return b
}

func TestProcessBatchClose_InvalidTerminalID(t *testing.T) {
	h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	cmd := validProcessCloseCmd()
	cmd.TerminalID = "not-a-uuid"

	var ve *pkgerrors.ValidationError
	err := h.ProcessBatchClose(context.Background(), cmd)
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestProcessBatchClose_FindOpenByTerminalError(t *testing.T) {
	repo := &fakeBatchRepo{findOpenErr: errors.New("connection reset")}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})

	err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd())
	if err == nil || !strings.Contains(err.Error(), "find open batch") {
		t.Fatalf("error = %v, want it to mention find open batch", err)
	}
}

func TestProcessBatchClose_NoOpenBatch(t *testing.T) {
	repo := &fakeBatchRepo{findOpenResult: nil}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})

	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v, want nil when there's nothing to close", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestProcessBatchClose_RequestCloseError(t *testing.T) {
	// Un batch que no está OPEN no puede transicionar a PENDING_CLOSE.
	batch := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "batch-1",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateClosed,
		Currency:   "ARS",
		CreatedAt:  time.Now(),
	})
	repo := &fakeBatchRepo{findOpenResult: batch}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})

	err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd())
	if err == nil || !strings.Contains(err.Error(), "request close") {
		t.Fatalf("error = %v, want it to mention request close", err)
	}
}

func TestProcessBatchClose_BeginTxError(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection reset"))

	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want an error")
	}
}

func TestProcessBatchClose_ClaimExecError(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnError(errors.New("constraint violation"))
	pool.ExpectRollback()

	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd())
	if err == nil || !strings.Contains(err.Error(), "ProcessBatchClose") {
		t.Fatalf("error = %v, want it to mention ProcessBatchClose", err)
	}
}

func TestProcessBatchClose_DuplicateEventSkipsPersistAndPublish(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 0)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{}

	h := newHandler(pool, repo, publisher, processor)
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 for a duplicate event", repo.saveCallCount)
	}
	if publisher.buildBatchClosedCalls != 0 {
		t.Errorf("BuildBatchClosed calls = %d, want 0", publisher.buildBatchClosedCalls)
	}
	if processor.calls != 0 {
		t.Errorf("processor.Submit calls = %d, want 0", processor.calls)
	}
}

func TestProcessBatchClose_SaveError(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch, saveErr: errors.New("connection reset")}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want it to propagate the Save error")
	}
}

func TestProcessBatchClose_NoDiscrepancy_SubmitsToProcessor(t *testing.T) {
	batch := closableFoundBatch(t) // total 3000/2 tx, matches validProcessCloseCmd()
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{confirmationID: "conf-123"}

	h := newHandler(pool, repo, publisher, processor)
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}

	if publisher.buildBatchClosedCalls != 1 {
		t.Errorf("BuildBatchClosed calls = %d, want 1", publisher.buildBatchClosedCalls)
	}
	if processor.calls != 1 {
		t.Errorf("processor.Submit calls = %d, want 1", processor.calls)
	}
	if repo.saveCallCount != 2 {
		t.Fatalf("Save call count = %d, want 2 (close + submit)", repo.saveCallCount)
	}
	saved := repo.lastSaved()
	if saved.State() != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", saved.State(), valueobject.BatchStateSubmitted)
	}
	if saved.SubmittedAt() == nil {
		t.Error("SubmittedAt() is nil, want it set after a successful submit")
	}
}

// Si no se puede guardar el evento, el cierre tampoco se confirma: el mensaje se
// reintenta y el lote sigue abierto.
func TestProcessBatchClose_EventErrorRollsBackClose(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	publisher := &fakePublisher{buildBatchClosedErr: errors.New("marshal failed")}
	processor := &fakeProcessor{confirmationID: "conf-123"}

	h := newHandler(pool, repo, publisher, processor)
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want the event error")
	}
	if processor.calls != 0 {
		t.Errorf("processor.Submit calls = %d, want 0", processor.calls)
	}
}

func TestProcessBatchClose_WithDiscrepancy_MarksDisputedAndSkipsSubmit(t *testing.T) {
	batch := closableFoundBatch(t) // backend total = 3000/2 tx
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{}

	h := newHandler(pool, repo, publisher, processor)
	cmd := validProcessCloseCmd()
	cmd.TerminalCount = 5 // no coincide con el backend (2) → discrepancia

	if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}

	if processor.calls != 0 {
		t.Errorf("processor.Submit calls = %d, want 0 (no debe enviarse un batch en disputa)", processor.calls)
	}
	// Un solo guardado: el lote no puede quedar en CLOSED con discrepancias.
	if repo.saveCallCount != 1 {
		t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
	}
	if publisher.buildBatchClosedCalls != 1 {
		t.Errorf("BuildBatchClosed calls = %d, want 1", publisher.buildBatchClosedCalls)
	}
	saved := repo.lastSaved()
	if saved.State() != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want %v", saved.State(), valueobject.BatchStateDisputed)
	}
}

func TestProcessBatchClose_DisputedSaveErrorEnqueuesNothing(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch, saveErr: errors.New("connection reset")}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	publisher := &fakePublisher{}

	h := newHandler(pool, repo, publisher, &fakeProcessor{})
	cmd := validProcessCloseCmd()
	cmd.TerminalCount = 5

	if err := h.ProcessBatchClose(context.Background(), cmd); err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want the Save error")
	}
	if publisher.buildBatchClosedCalls != 0 {
		t.Errorf("BuildBatchClosed calls = %d, want 0", publisher.buildBatchClosedCalls)
	}
}

func TestProcessBatchClose_SubmitSaveError(t *testing.T) {
	batch := closableFoundBatch(t) // sin discrepancias — sigue a submitBatch
	repo := &fakeBatchRepoSaveErrOnCall{fakeBatchRepo: fakeBatchRepo{findOpenResult: batch}, errOnCall: 2}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	processor := &fakeProcessor{confirmationID: "conf-123"}

	h := newHandler(pool, repo, &fakePublisher{}, processor)
	err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd())
	if err == nil || !strings.Contains(err.Error(), "submitBatch: save") {
		t.Fatalf("error = %v, want it to mention submitBatch: save", err)
	}
	if processor.calls != 1 {
		t.Errorf("processor.Submit calls = %d, want 1 (debe intentarse antes de fallar al guardar)", processor.calls)
	}
}

func TestProcessBatchClose_ProcessorSubmitError(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	processor := &fakeProcessor{err: errors.New("processor unavailable")}

	h := newHandler(pool, repo, &fakePublisher{}, processor)
	err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd())
	if err == nil || !strings.Contains(err.Error(), "processor submit") {
		t.Fatalf("error = %v, want it to mention processor submit", err)
	}
	// El batch quedó CLOSED (guardado antes del intento de envío) — no SUBMITTED,
	// porque el envío al procesador falló y no se debe marcar como enviado.
	saved := repo.lastSaved()
	if saved.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v (el envío al procesador falló)", saved.State(), valueobject.BatchStateClosed)
	}
}

func TestProcessBatchClose_InvalidBatchDateFallsBackToNow(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)

	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "conf-1"})
	cmd := validProcessCloseCmd()
	cmd.BatchDate = "not-a-date"

	if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v, want nil (falls back to time.Now())", err)
	}
}

// fakeBatchRepoSaveErrOnCall falla únicamente en la llamada N a Save (1-based),
// para poder testear un error en un Save posterior sin impedir los anteriores
// (p.ej. el Save del cierre debe tener éxito para llegar al de MarkDisputed/submit).
type fakeBatchRepoSaveErrOnCall struct {
	fakeBatchRepo
	errOnCall int
}

func (f *fakeBatchRepoSaveErrOnCall) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	_ = f.fakeBatchRepo.Save(ctx, b)
	if f.fakeBatchRepo.saveCallCount == f.errOnCall {
		return errors.New("connection reset")
	}
	return nil
}

// Sin procesador configurado, un cierre correcto no debe provocar un panic: en un
// handler de NATS terminaría el proceso. El lote queda en CLOSED y el mensaje se confirma.
func TestProcessBatchClose_NoProcessorKeepsBatchClosed(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	publisher := &fakePublisher{}

	h := newHandler(pool, repo, publisher, nil)
	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v, want nil", err)
	}

	if repo.saveCallCount != 1 {
		t.Errorf("Save call count = %d, want 1 (solo el cierre)", repo.saveCallCount)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateClosed)
	}
	if publisher.buildBatchClosedCalls != 1 {
		t.Errorf("BuildBatchClosed calls = %d, want 1", publisher.buildBatchClosedCalls)
	}
}

// ─── CloseStaleBatches ────────────────────────────────────────────────────────

func TestCloseStaleBatches_FindError(t *testing.T) {
	repo := &fakeBatchRepo{staleErr: errors.New("connection reset")}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})

	_, _, err := h.CloseStaleBatches(context.Background(), time.Now())
	if err == nil || !strings.Contains(err.Error(), "CloseStaleBatches: find batches") {
		t.Fatalf("error = %v, want it to mention CloseStaleBatches: find batches", err)
	}
}

// El lote de ayer se cierra recién pasado el plazo: la búsqueda recibe now − closeGrace.
func TestCloseStaleBatches_AppliesTheCloseGrace(t *testing.T) {
	repo := &fakeBatchRepo{}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})
	h.SetCloseGrace(30 * time.Minute)
	now := time.Date(2026, 11, 9, 3, 10, 0, 0, time.UTC)

	if _, _, err := h.CloseStaleBatches(context.Background(), now); err != nil {
		t.Fatalf("CloseStaleBatches() error = %v", err)
	}
	if want := now.Add(-30 * time.Minute); !repo.staleNow.Equal(want) {
		t.Errorf("FindStaleOpenIDs now = %v, want %v", repo.staleNow, want)
	}
}

func TestCloseStaleBatches_ClosesPublishesAndSubmits(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{confirmationID: "conf-1"}

	closed, failed, err := newHandler(pool, repo, publisher, processor).CloseStaleBatches(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("CloseStaleBatches() error = %v", err)
	}
	if closed != 1 || failed != 0 {
		t.Errorf("closed = %d, failed = %d, want 1, 0", closed, failed)
	}
	if publisher.buildBatchClosedCalls != 1 {
		t.Errorf("BuildBatchClosed calls = %d, want 1", publisher.buildBatchClosedCalls)
	}
	if processor.calls != 1 {
		t.Errorf("processor.Submit calls = %d, want 1", processor.calls)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateSubmitted)
	}
	if batch.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", batch.Discrepancies())
	}
}

func TestCloseStaleBatches_NoProcessorKeepsClosed(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)

	closed, failed, err := newHandler(pool, repo, &fakePublisher{}, nil).CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 1 || failed != 0 {
		t.Fatalf("closed = %d, failed = %d, err = %v, want 1, 0, nil", closed, failed, err)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateClosed)
	}
}

func TestCloseStaleBatches_EmptyBatchIsClosedSilently(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{}

	closed, _, err := newHandler(pool, repo, publisher, processor).CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 1 {
		t.Fatalf("closed = %d, err = %v, want 1, nil", closed, err)
	}
	if publisher.buildBatchClosedCalls != 0 || processor.calls != 0 {
		t.Errorf("publish calls = %d, submit calls = %d, want 0, 0", publisher.buildBatchClosedCalls, processor.calls)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateClosed)
	}
}

// Otra instancia ya cerró el batch: no se publica ni se envía dos veces.
func TestCloseStaleBatches_AlreadyClaimedSkips(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 0)
	publisher := &fakePublisher{}
	processor := &fakeProcessor{}

	closed, failed, err := newHandler(pool, repo, publisher, processor).CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 0 || failed != 0 {
		t.Fatalf("closed = %d, failed = %d, err = %v, want 0, 0, nil", closed, failed, err)
	}
	if repo.saveCallCount != 0 || publisher.buildBatchClosedCalls != 0 || processor.calls != 0 {
		t.Errorf("save = %d, publish = %d, submit = %d, want 0, 0, 0", repo.saveCallCount, publisher.buildBatchClosedCalls, processor.calls)
	}
}

func TestCloseStaleBatches_SkipsBatchNoLongerOpen(t *testing.T) {
	repo := &fakeBatchRepo{staleIDs: []string{"batch-1"}, findByIDResult: nil}
	closed, failed, err := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{}).CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 0 || failed != 0 {
		t.Fatalf("closed = %d, failed = %d, err = %v, want 0, 0, nil", closed, failed, err)
	}
}

func TestCloseStaleBatches_CountsFailures(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch, saveErr: errors.New("connection reset")}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	closed, failed, err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).CloseStaleBatches(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("CloseStaleBatches() error = %v, want nil (un batch no frena a los demás)", err)
	}
	if closed != 0 || failed != 1 {
		t.Errorf("closed = %d, failed = %d, want 0, 1", closed, failed)
	}
}

// Si falla el envío, el lote ya quedó cerrado: no cuenta como fallo del cierre.
func TestCloseStaleBatches_SubmitErrorIsNotAFailure(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	processor := &fakeProcessor{err: errors.New("processor down")}

	closed, failed, err := newHandler(pool, repo, &fakePublisher{}, processor).CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 1 || failed != 0 {
		t.Fatalf("closed = %d, failed = %d, err = %v, want 1, 0, nil", closed, failed, err)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateClosed)
	}
}

// ─── Montos: reversas, monedas y día de negocio ───────────────────────────────

func TestRegisterReversal_UsesRealAmount(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	cmd := validReversalCmd()
	cmd.AmountCents = 4321
	if err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterReversal(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterReversal() error = %v", err)
	}

	txs := repo.lastSaved().Transactions()
	if len(txs) != 1 || txs[0].Type() != valueobject.BatchTxReversal || txs[0].AmountCents() != 4321 {
		t.Fatalf("transactions = %+v, want one REVERSAL of 4321", txs)
	}
}

func TestRegister_InvalidCurrencyOrAmountIsPermanent(t *testing.T) {
	var ve *pkgerrors.ValidationError
	h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})

	approvals := []func(*port.RegisterApprovalCommand){
		func(c *port.RegisterApprovalCommand) { c.Currency = "" },
		func(c *port.RegisterApprovalCommand) { c.AmountCents = 0 },
	}
	for _, mut := range approvals {
		cmd := validApprovalCmd()
		mut(&cmd)
		if err := h.RegisterApproval(context.Background(), cmd); !errors.As(err, &ve) {
			t.Errorf("RegisterApproval(%+v) error = %v, want ValidationError", cmd, err)
		}
	}

	reversals := []func(*port.RegisterReversalCommand){
		func(c *port.RegisterReversalCommand) { c.Currency = "" },
		func(c *port.RegisterReversalCommand) { c.AmountCents = -1 },
	}
	for _, mut := range reversals {
		cmd := validReversalCmd()
		mut(&cmd)
		if err := h.RegisterReversal(context.Background(), cmd); !errors.As(err, &ve) {
			t.Errorf("RegisterReversal(%+v) error = %v, want ValidationError", cmd, err)
		}
	}

	closeCmd := validProcessCloseCmd()
	closeCmd.Currency = ""
	if err := h.ProcessBatchClose(context.Background(), closeCmd); !errors.As(err, &ve) {
		t.Errorf("ProcessBatchClose() error = %v, want ValidationError", err)
	}
}

func TestRegisterApproval_LooksUpBatchByCurrency(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	cmd := validApprovalCmd()
	cmd.Currency = "USD"
	if err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterApproval(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}
	if repo.findOrCreateCurrency != "USD" {
		t.Errorf("currency = %q, want USD", repo.findOrCreateCurrency)
	}
}

// 22:30 del 5 en Argentina ya es el 6 en UTC: la venta pertenece al día 5.
func TestBusinessDate_UsesConfiguredLocation(t *testing.T) {
	loc, err := time.LoadLocation("America/Argentina/Buenos_Aires")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: batch}
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	h.SetBusinessLocation(loc)
	cmd := validApprovalCmd()
	cmd.AuthorizedAt = "2026-11-06T01:30:00Z"
	if err := h.RegisterApproval(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}
	if got := repo.findOrCreateDate.Format("2006-01-02"); got != "2026-11-05" {
		t.Errorf("batch date = %s, want 2026-11-05", got)
	}
}

func TestProcessBatchClose_ResolvesDate(t *testing.T) {
	loc, err := time.LoadLocation("America/Argentina/Buenos_Aires")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	tests := []struct {
		name, batchDate, requestedAt, want string
	}{
		{"fecha del terminal", "2026-11-04", "2026-11-06T01:30:00Z", "2026-11-04"},
		{"sin fecha usa el dia de negocio del pedido", "", "2026-11-06T01:30:00Z", "2026-11-05"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeBatchRepo{}
			h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})
			h.SetBusinessLocation(loc)
			cmd := validProcessCloseCmd()
			cmd.BatchDate, cmd.RequestedAt, cmd.Currency = tc.batchDate, tc.requestedAt, "USD"
			if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
				t.Fatalf("ProcessBatchClose() error = %v", err)
			}
			if got := repo.findOpenDate.Format("2006-01-02"); got != tc.want {
				t.Errorf("date = %s, want %s", got, tc.want)
			}
			if repo.findOpenCurrency != "USD" {
				t.Errorf("currency = %q, want USD", repo.findOpenCurrency)
			}
		})
	}
}

// fakeBatchRepoStale devuelve un conflicto de versión en las primeras llamadas a Save.
type fakeBatchRepoStale struct {
	fakeBatchRepo
	staleSaves int
}

func (f *fakeBatchRepoStale) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	if f.staleSaves > 0 {
		f.staleSaves--
		f.saveCallCount++
		return fmt.Errorf("save: %w", repository.ErrStaleBatch)
	}
	return f.fakeBatchRepo.Save(ctx, b)
}

func TestRegisterApproval_RetriesInternallyOnStaleBatch(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepoStale{fakeBatchRepo: fakeBatchRepo{findOrCreateResult: batch}, staleSaves: 2}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	expectClaimedThenFail(pool, 1)
	expectClaimed(pool, 1)

	if err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterApproval(context.Background(), validApprovalCmd()); err != nil {
		t.Fatalf("RegisterApproval() error = %v, want nil after retrying", err)
	}
	if repo.saveCallCount != 3 {
		t.Errorf("Save call count = %d, want 3", repo.saveCallCount)
	}
}

func TestRegisterReversal_RetriesInternallyOnStaleBatch(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepoStale{fakeBatchRepo: fakeBatchRepo{findOrCreateResult: batch}, staleSaves: 1}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	expectClaimed(pool, 1)

	if err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterReversal(context.Background(), validReversalCmd()); err != nil {
		t.Fatalf("RegisterReversal() error = %v, want nil after retrying", err)
	}
}

// Si entró una venta, el cierre se recalcula con el lote recargado.
func TestProcessBatchClose_ReloadsBatchOnStale(t *testing.T) {
	repo := &fakeBatchRepoStaleReload{builds: func() *aggregate.SettlementBatch { return closableFoundBatch(t) }, staleSaves: 1}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	expectClaimedWithEvent(pool)

	if err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "c"}).ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v, want nil after retrying", err)
	}
	if repo.findOpenCalls != 2 {
		t.Errorf("FindOpenByTerminal calls = %d, want 2 (debe recargar el lote)", repo.findOpenCalls)
	}
}

type fakeBatchRepoStaleReload struct {
	fakeBatchRepo
	builds        func() *aggregate.SettlementBatch
	staleSaves    int
	findOpenCalls int
}

func (f *fakeBatchRepoStaleReload) FindOpenByTerminal(context.Context, domain.TerminalID, time.Time, string) (*aggregate.SettlementBatch, error) {
	f.findOpenCalls++
	return f.builds(), nil
}

func (f *fakeBatchRepoStaleReload) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	if f.staleSaves > 0 {
		f.staleSaves--
		return repository.ErrStaleBatch
	}
	return f.fakeBatchRepo.Save(ctx, b)
}

// Si el conflicto persiste, el error llega al subscriber para que NATS reintente.
func TestRegisterApproval_StaleBatchExhaustedIsTransient(t *testing.T) {
	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepoStale{fakeBatchRepo: fakeBatchRepo{findOrCreateResult: batch}, staleSaves: 1000}
	pool := newMockPool(t)
	pool.MatchExpectationsInOrder(true)
	for range 8 {
		expectClaimedThenFail(pool, 1)
	}

	err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterApproval(context.Background(), validApprovalCmd())
	if !errors.Is(err, repository.ErrStaleBatch) {
		t.Fatalf("error = %v, want ErrStaleBatch", err)
	}
	var ve *pkgerrors.ValidationError
	if errors.As(err, &ve) {
		t.Error("error is a ValidationError, want a transient error so the message is retried")
	}
	if repo.saveCallCount != 8 {
		t.Errorf("Save call count = %d, want 8", repo.saveCallCount)
	}
}

func TestRegisterApproval_StopsRetryingWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepoCancelOnSave{fakeBatchRepo: fakeBatchRepo{findOrCreateResult: batch}, cancel: cancel}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)

	err := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{}).RegisterApproval(ctx, validApprovalCmd())
	if !errors.Is(err, repository.ErrStaleBatch) {
		t.Fatalf("error = %v, want ErrStaleBatch", err)
	}
	if repo.saves != 1 {
		t.Errorf("Save calls = %d, want 1 (no debe seguir reintentando)", repo.saves)
	}
}

// fakeBatchRepoCancelOnSave simula un apagado durante un conflicto de versión.
type fakeBatchRepoCancelOnSave struct {
	fakeBatchRepo
	cancel context.CancelFunc
	saves  int
}

func (f *fakeBatchRepoCancelOnSave) Save(context.Context, *aggregate.SettlementBatch) error {
	f.saves++
	f.cancel()
	return repository.ErrStaleBatch
}

func (f *fakeBatchRepoSaveErrOnCall) SaveTx(ctx context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(ctx, b)
}

func (f *fakeBatchRepoStale) SaveTx(ctx context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(ctx, b)
}

func (f *fakeBatchRepoStaleReload) SaveTx(ctx context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(ctx, b)
}

func (f *fakeBatchRepoCancelOnSave) SaveTx(ctx context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(ctx, b)
}

// ─── Revisión de reversas ─────────────────────────────────────────────────────

type fakeReviews struct {
	purchase *repository.ReversalPurchase
	findErr  error

	inserted  []repository.ReversalReview
	insertErr error

	reconcileResult repository.ReviewReconciliation
	reconcileErr    error
	reconciled      []repository.ReversalPurchase
	reconcileBatch  string // purchaseBatchID que recibió ReconcileReversalReviewTx

	openCount int
	openErr   error

	stored       *repository.StoredReversalReview
	lockErr      error
	updateAmount int64
	updateCalls  int
	deleteCalls  int
	closedBy     string
	closedNote   string
	closedAction string

	insertedReversals []string // "batch|tx|amount|currency" de InsertReversalTx

	releasable    []string
	releasableErr error
}

func (f *fakeReviews) CountOpenReversalReviews(context.Context, string) (int, error) {
	return f.openCount, f.openErr
}

func (f *fakeReviews) FindReleasableHeldBatchIDs(context.Context) ([]string, error) {
	return f.releasable, f.releasableErr
}

func (f *fakeReviews) ListOpenReversalReviews(context.Context, int) ([]repository.StoredReversalReview, error) {
	return nil, nil
}

func (f *fakeReviews) LockReversalReviewTx(context.Context, pgx.Tx, string) (*repository.StoredReversalReview, error) {
	return f.stored, f.lockErr
}

func (f *fakeReviews) UpdateReversalAmountTx(_ context.Context, _ pgx.Tx, _, _ string, amount int64) error {
	f.updateCalls++
	f.updateAmount = amount
	return nil
}

func (f *fakeReviews) DeleteReversalTx(context.Context, pgx.Tx, string, string) error {
	f.deleteCalls++
	return nil
}

func (f *fakeReviews) CloseReversalReviewTx(_ context.Context, _ pgx.Tx, _, operator, resolution, action string) error {
	f.closedBy, f.closedNote, f.closedAction = operator, resolution, action
	return nil
}

func (f *fakeReviews) InsertReversalTx(_ context.Context, _ pgx.Tx, batchID, txID string, amount int64, currency string) error {
	f.insertedReversals = append(f.insertedReversals, fmt.Sprintf("%s|%s|%d|%s", batchID, txID, amount, currency))
	return nil
}

func (f *fakeReviews) FindPurchaseTx(context.Context, pgx.Tx, string) (*repository.ReversalPurchase, error) {
	return f.purchase, f.findErr
}

func (f *fakeReviews) InsertReversalReviewTx(_ context.Context, _ pgx.Tx, r repository.ReversalReview) error {
	f.inserted = append(f.inserted, r)
	return f.insertErr
}

func (f *fakeReviews) ReconcileReversalReviewTx(_ context.Context, _ pgx.Tx, _, purchaseBatchID string, p repository.ReversalPurchase) (repository.ReviewReconciliation, error) {
	f.reconciled = append(f.reconciled, p)
	f.reconcileBatch = purchaseBatchID
	return f.reconcileResult, f.reconcileErr
}

func TestRegisterReversal_Review(t *testing.T) {
	cmd := validReversalCmd()
	match := repository.ReversalPurchase{MerchantID: cmd.MerchantID, AmountCents: cmd.AmountCents, Currency: cmd.Currency}
	with := func(change func(*repository.ReversalPurchase)) *repository.ReversalPurchase {
		p := match
		change(&p)
		return &p
	}
	tests := []struct {
		name       string
		purchase   *repository.ReversalPurchase
		wantReason string
	}{
		{"coincide", &match, ""},
		{"sin compra", nil, repository.ReversalReviewPurchaseNotFound},
		{"otro monto", with(func(p *repository.ReversalPurchase) { p.AmountCents++ }), repository.ReversalReviewAmountMismatch},
		{"otra moneda", with(func(p *repository.ReversalPurchase) { p.Currency = "USD" }), repository.ReversalReviewCurrencyMismatch},
		{"otro comercio", with(func(p *repository.ReversalPurchase) { p.MerchantID = domain.NewMerchantID().String() }), repository.ReversalReviewMerchantMismatch},
		{"el comercio pesa más que el monto", with(func(p *repository.ReversalPurchase) {
			p.MerchantID = domain.NewMerchantID().String()
			p.AmountCents++
		}), repository.ReversalReviewMerchantMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
			repo := &fakeBatchRepo{findOrCreateResult: batch}
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			reviews := &fakeReviews{purchase: tc.purchase}
			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
			h.SetReversalReviews(reviews)

			if err := h.RegisterReversal(context.Background(), cmd); err != nil {
				t.Fatalf("RegisterReversal() error = %v", err)
			}

			// La reversa se registra siempre, con el monto del evento.
			if repo.saveCallCount != 1 {
				t.Errorf("Save call count = %d, want 1", repo.saveCallCount)
			}
			if tc.wantReason == "" {
				if len(reviews.inserted) != 0 {
					t.Errorf("reviews = %+v, want none", reviews.inserted)
				}
				return
			}
			if len(reviews.inserted) != 1 {
				t.Fatalf("reviews = %d, want 1", len(reviews.inserted))
			}
			got := reviews.inserted[0]
			if got.Reason != tc.wantReason || got.BatchID != batch.ID() || got.TransactionID != cmd.OriginalTransactionID || got.AmountCents != cmd.AmountCents {
				t.Errorf("review = %+v", got)
			}
		})
	}
}

// Si no se puede validar, nada se confirma: el evento se reintenta.
func TestRegisterReversal_ReviewErrorsRollBack(t *testing.T) {
	tests := []struct {
		name    string
		reviews *fakeReviews
	}{
		{"lookup", &fakeReviews{findErr: errors.New("db down")}},
		{"insert", &fakeReviews{insertErr: errors.New("db down")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeBatchRepo{findOrCreateResult: newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())}
			pool := newMockPool(t)
			expectClaimedThenFail(pool, 1)
			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
			h.SetReversalReviews(tc.reviews)

			if err := h.RegisterReversal(context.Background(), validReversalCmd()); err == nil {
				t.Fatal("RegisterReversal() error = nil, want it to fail")
			}
		})
	}
}

func TestRegisterApproval_ReconcilesPendingReview(t *testing.T) {
	repo := &fakeBatchRepo{findOrCreateResult: newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	reviews := &fakeReviews{reconcileResult: repository.ReviewReconciliation{Resolved: true}}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	h.SetReversalReviews(reviews)
	cmd := validApprovalCmd()

	if err := h.RegisterApproval(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}

	want := repository.ReversalPurchase{MerchantID: cmd.MerchantID, AmountCents: cmd.AmountCents, Currency: cmd.Currency}
	if len(reviews.reconciled) != 1 || reviews.reconciled[0] != want {
		t.Errorf("reconciled = %+v, want [%+v]", reviews.reconciled, want)
	}
}

func TestRegisterApproval_ReconcileErrorRollsBack(t *testing.T) {
	repo := &fakeBatchRepo{findOrCreateResult: newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())}
	pool := newMockPool(t)
	expectClaimedThenFail(pool, 1)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	h.SetReversalReviews(&fakeReviews{reconcileErr: errors.New("db down")})

	if err := h.RegisterApproval(context.Background(), validApprovalCmd()); err == nil {
		t.Fatal("RegisterApproval() error = nil, want the reconcile error")
	}
}
