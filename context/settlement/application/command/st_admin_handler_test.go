package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

func TestForceClose_EmptyBatchID(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "", OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestForceClose_EmptyOperatorID(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "batch-1", OperatorID: ""})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestForceClose_FindByIDError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("connection reset")}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "batch-1", OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "ForceClose: find batch") {
		t.Fatalf("error = %v, want it to mention ForceClose: find batch", err)
	}
}

func TestForceClose_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "batch-999", OperatorID: "op-1"})
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestForceClose_NotOpenIsValidationError(t *testing.T) {
	// Un batch que no está OPEN no puede transicionar a PENDING_CLOSE.
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "batch-1",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateClosed,
		Currency:   "ARS",
		CreatedAt:  time.Now(),
	})
	repo := &fakeBatchRepo{findByIDResult: b}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "batch-1", OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestForceClose_SaveError(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error = %v, want it to propagate the Save error", err)
	}
}

func TestForceClose_Success(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findByIDResult: b}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}

	if repo.saveCallCount != 1 {
		t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
	}
	saved := repo.lastSaved()
	if saved.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", saved.State(), valueobject.BatchStateClosed)
	}
	if saved.Summary() == nil {
		t.Error("Summary() is nil, want it computed by Close()")
	}
}

// ─── ResubmitBatch ────────────────────────────────────────────────────────────

func newClosedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "batch-1",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateClosed,
		Currency:   "ARS",
		CreatedAt:  time.Now(),
	})
}

func TestResubmitBatch_EmptyBatchID(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "", OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestResubmitBatch_EmptyOperatorID(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-1", OperatorID: ""})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestResubmitBatch_FindByIDError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("connection reset")}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-1", OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "ResubmitBatch: find batch") {
		t.Fatalf("error = %v, want it to mention ResubmitBatch: find batch", err)
	}
}

func TestResubmitBatch_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-999", OperatorID: "op-1"})
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestResubmitBatch_WrongState(t *testing.T) {
	// Un batch que no está CLOSED no debe poder resubmitirse (ej: OPEN o ya SUBMITTED).
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findByIDResult: b}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestResubmitBatch_ProcessorError(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	processor := &fakeProcessor{err: errors.New("processor unavailable")}
	h := command.NewAdminHandler(repo, processor)

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "ResubmitBatch: processor submit") {
		t.Fatalf("error = %v, want it to mention ResubmitBatch: processor submit", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0 (no debe persistir si el procesador falla)", repo.saveCallCount)
	}
}

func TestResubmitBatch_SaveError(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
	processor := &fakeProcessor{confirmationID: "conf-123"}
	h := command.NewAdminHandler(repo, processor)

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "ResubmitBatch: save") {
		t.Fatalf("error = %v, want it to mention ResubmitBatch: save", err)
	}
}

func TestResubmitBatch_Success(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	processor := &fakeProcessor{confirmationID: "conf-123"}
	h := command.NewAdminHandler(repo, processor)

	if err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}

	if processor.calls != 1 {
		t.Fatalf("processor.Submit calls = %d, want 1", processor.calls)
	}
	if repo.saveCallCount != 1 {
		t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
	}
	saved := repo.lastSaved()
	if saved.State() != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", saved.State(), valueobject.BatchStateSubmitted)
	}
}

func TestResubmitBatch_NoProcessor(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	h := command.NewAdminHandler(repo, nil)

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if !errors.Is(err, service.ErrProcessorUnavailable) {
		t.Fatalf("error = %v, want ErrProcessorUnavailable", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
}

// ─── ReopenBatch / ResolveDispute ─────────────────────────────────────────────

func newDisputedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(5, 9999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.MarkDisputed("mismatch"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	return b
}

func TestDisputeOperations(t *testing.T) {
	ops := []struct {
		name      string
		run       func(*command.AdminHandler, string, string, string) error
		wantState valueobject.BatchState
	}{
		{
			"reopen",
			func(h *command.AdminHandler, id, op, reason string) error {
				return h.ReopenBatch(context.Background(), port.ReopenBatchCommand{BatchID: id, OperatorID: op, Reason: reason})
			},
			valueobject.BatchStateOpen,
		},
		{
			"resolve",
			func(h *command.AdminHandler, id, op, reason string) error {
				return h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: id, OperatorID: op, Reason: reason})
			},
			valueobject.BatchStateClosed,
		},
	}

	for _, op := range ops {
		t.Run(op.name+"/success", func(t *testing.T) {
			b := newDisputedBatch(t)
			repo := &fakeBatchRepo{findByIDResult: b}
			if err := op.run(command.NewAdminHandler(repo, nil), b.ID(), "op-1", "conciliado"); err != nil {
				t.Fatalf("error = %v", err)
			}
			if repo.saveCallCount != 1 {
				t.Fatalf("Save call count = %d, want 1", repo.saveCallCount)
			}
			if got := repo.lastSaved().State(); got != op.wantState {
				t.Errorf("State() = %v, want %v", got, op.wantState)
			}
		})

		t.Run(op.name+"/validation", func(t *testing.T) {
			for _, in := range [][3]string{{"", "op-1", "r"}, {"b", "", "r"}, {"b", "op-1", ""}} {
				err := op.run(command.NewAdminHandler(&fakeBatchRepo{}, nil), in[0], in[1], in[2])
				var ve *pkgerrors.ValidationError
				if !errors.As(err, &ve) {
					t.Errorf("input %v: error type = %T, want *pkgerrors.ValidationError", in, err)
				}
			}
		})

		t.Run(op.name+"/not found", func(t *testing.T) {
			err := op.run(command.NewAdminHandler(&fakeBatchRepo{}, nil), "b", "op-1", "r")
			var nf *pkgerrors.NotFoundError
			if !errors.As(err, &nf) {
				t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
			}
		})

		t.Run(op.name+"/wrong state", func(t *testing.T) {
			repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
			err := op.run(command.NewAdminHandler(repo, nil), "batch-1", "op-1", "r")
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
			}
			if repo.saveCallCount != 0 {
				t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
			}
		})

		t.Run(op.name+"/save error", func(t *testing.T) {
			b := newDisputedBatch(t)
			repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
			err := op.run(command.NewAdminHandler(repo, nil), b.ID(), "op-1", "r")
			if err == nil || !strings.Contains(err.Error(), "connection reset") {
				t.Fatalf("error = %v, want it to propagate the Save error", err)
			}
		})
	}
}

// ─── ForceClose: totales, notificación ────────────────────────────────────────

func TestForceClose_UsesBackendTotalsWithoutFalseDiscrepancies(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddTransaction(domain.NewTransactionID(), 500, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	h := command.NewAdminHandler(&fakeBatchRepo{findByIDResult: b}, &fakeProcessor{})

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if got := b.Summary().TotalAmount().Cents(); got != 1500 {
		t.Errorf("TotalAmount = %d, want 1500", got)
	}
}

func TestForceClose_CanBeSubmittedDirectly(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	repo := &fakeBatchRepo{findByIDResult: b}
	h := command.NewAdminHandler(repo, &fakeProcessor{confirmationID: "c"})

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if b.State() != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateSubmitted)
	}
}

// newEventAdmin arma un AdminHandler que encola eventos en un outbox sobre pool simulado.
func newEventAdmin(t *testing.T, repo *fakeBatchRepo, publisher *fakePublisher, mdr float64) (*command.AdminHandler, pgxmock.PgxPoolIface) {
	t.Helper()
	pool := newMockPool(t)
	h := command.NewAdminHandler(repo, nil)
	h.ConfigureSettlement(pool, repo, publisher, outbox.NewStore(idempotencySchema), mdr)
	return h, pool
}

func TestForceClose_EnqueuesBatchClosed(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: b}, publisher, 2.5)
	expectEventTx(pool, 1)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if publisher.buildBatchClosedCalls != 1 {
		t.Errorf("BuildBatchClosed calls = %d, want 1", publisher.buildBatchClosedCalls)
	}
}

func TestForceClose_EmptyBatchEnqueuesNothing(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: b}, publisher, 2.5)
	expectEventTx(pool, 0)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if publisher.buildBatchClosedCalls != 0 {
		t.Errorf("BuildBatchClosed calls = %d, want 0", publisher.buildBatchClosedCalls)
	}
}

// Sin el evento, el cierre tampoco se guarda: no puede quedar un lote cerrado sin avisar.
func TestForceClose_EventErrorRollsBack(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	publisher := &fakePublisher{buildBatchClosedErr: errors.New("marshal failed")}
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: b}, publisher, 2.5)
	expectRolledBackTx(pool)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
		t.Fatal("ForceClose() error = nil, want the event error")
	}
}

func TestForceClose_SaveErrorEnqueuesNothing(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}, publisher, 2.5)
	expectRolledBackTx(pool)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
		t.Fatal("ForceClose() error = nil, want the Save error")
	}
	if publisher.buildBatchClosedCalls != 0 {
		t.Errorf("BuildBatchClosed calls = %d, want 0", publisher.buildBatchClosedCalls)
	}
}

func TestResubmitBatch_WithDiscrepanciesMovesToDisputed(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(0, 0); err != nil { // lote con discrepancias, ej: un force-close anterior
		t.Fatalf("Close() error = %v", err)
	}

	repo := &fakeBatchRepo{findByIDResult: b}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	h := command.NewAdminHandler(repo, processor)

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
	if processor.calls != 0 {
		t.Errorf("processor.Submit calls = %d, want 0", processor.calls)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateDisputed)
	}

	// La salida es ResolveDispute, y de ahí un nuevo Resubmit.
	if err := h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: b.ID(), OperatorID: "op-1", Reason: "ok"}); err != nil {
		t.Fatalf("ResolveDispute() error = %v", err)
	}
	if err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if got := repo.lastSaved().State(); got != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateSubmitted)
	}
}

func TestResubmitBatch_WithDiscrepanciesSaveError(t *testing.T) {
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateClosed, Currency: "ARS", Discrepancies: 2,
	})
	repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
	h := command.NewAdminHandler(repo, &fakeProcessor{})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "ResubmitBatch: save") {
		t.Fatalf("error = %v, want it to mention ResubmitBatch: save", err)
	}
}

// ─── SettleBatch ──────────────────────────────────────────────────────────────

func newSubmittedBatch(t *testing.T, merchantID domain.MerchantID, currency string, amount int64) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), merchantID, time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), currency)
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	if err := b.AddTransaction(domain.NewTransactionID(), amount, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.CloseWithBackendTotals(); err != nil {
		t.Fatalf("CloseWithBackendTotals() error = %v", err)
	}
	if err := b.Submit(); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	return b
}

func TestSettleBatch_Validation(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, nil)
	for _, cmd := range []port.SettleBatchCommand{{BatchID: "", OperatorID: "op-1"}, {BatchID: "b", OperatorID: ""}} {
		err := h.SettleBatch(context.Background(), cmd)
		var ve *pkgerrors.ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("cmd %+v: error type = %T, want *pkgerrors.ValidationError", cmd, err)
		}
	}
}

func TestSettleBatch_NotFound(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, nil)
	err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: "b", OperatorID: "op-1"})
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestSettleBatch_FindError(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{findByIDErr: errors.New("connection reset")}, nil)
	err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: "b", OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "SettleBatch: find batch") {
		t.Fatalf("error = %v, want it to mention SettleBatch: find batch", err)
	}
}

func TestSettleBatch_WrongState(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	h := command.NewAdminHandler(repo, nil)
	err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: "batch-1", OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
}

func TestSettleBatch_SaveError(t *testing.T) {
	publisher := &fakePublisher{}
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
	repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	expectRolledBackTx(pool)

	err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	if err == nil || !strings.Contains(err.Error(), "SettleBatch: save") {
		t.Fatalf("error = %v, want it to mention SettleBatch: save", err)
	}
	if len(publisher.completed) != 0 {
		t.Errorf("completed events = %d, want 0", len(publisher.completed))
	}
}

func TestSettleBatch_EnqueuesWhenAllBatchesSettled(t *testing.T) {
	merchantID := domain.NewMerchantID()
	b := newSubmittedBatch(t, merchantID, "ARS", 10000)
	repo := &fakeBatchRepo{findByIDResult: b, dayResult: []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 10000},
		{Currency: "ARS", Settled: true, TotalAmount: 5000, AdjustmentAmount: -400},
	}}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	expectEventTx(pool, 1)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}

	if got := repo.lastSaved().State(); got != valueobject.BatchStateSettled {
		t.Errorf("State() = %v, want %v", got, valueobject.BatchStateSettled)
	}
	if b.SettledAt() == nil {
		t.Error("SettledAt() is nil, want it set")
	}
	if len(publisher.completed) != 1 {
		t.Fatalf("completed events = %d, want 1", len(publisher.completed))
	}
	want := completedEvent{merchantID.String(), "2026-01-15", 2, 15000, -400, 14625, "ARS", 2.5}
	if publisher.completed[0] != want {
		t.Errorf("completed = %+v, want %+v", publisher.completed[0], want)
	}
}

func TestSettleBatch_EnqueuesNothingWhilePending(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 10000)
	repo := &fakeBatchRepo{findByIDResult: b, dayResult: []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 10000},
		{Currency: "ARS", Settled: false, TotalAmount: 5000},
	}}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	expectEventTx(pool, 0)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}
	if len(publisher.completed) != 0 {
		t.Errorf("completed events = %d, want 0 (hay un batch pendiente)", len(publisher.completed))
	}
}

func TestSettleBatch_EnqueuesOnePerCurrency(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
	repo := &fakeBatchRepo{findByIDResult: b, dayResult: []repository.MerchantDayBatch{
		{Currency: "USD", Settled: true, TotalAmount: 200},
		{Currency: "ARS", Settled: true, TotalAmount: 1000},
	}}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, repo, publisher, 0)
	expectEventTx(pool, 2)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}
	if len(publisher.completed) != 2 {
		t.Fatalf("completed events = %d, want 2", len(publisher.completed))
	}
	if publisher.completed[0].currency != "ARS" || publisher.completed[1].currency != "USD" {
		t.Errorf("currencies = %q, %q, want ARS, USD", publisher.completed[0].currency, publisher.completed[1].currency)
	}
	if publisher.completed[1].net != 200 {
		t.Errorf("net = %d, want 200 (MDR 0)", publisher.completed[1].net)
	}
}

// La liquidación y su evento van juntos: si falla uno, no se confirma ninguno.
func TestSettleBatch_CompletionErrorsRollBack(t *testing.T) {
	tests := []struct {
		name      string
		dayErr    error
		publisher *fakePublisher
	}{
		{"query error", errors.New("connection reset"), &fakePublisher{}},
		{"build error", nil, &fakePublisher{buildCompletedErr: errors.New("marshal failed")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
			repo := &fakeBatchRepo{findByIDResult: b, dayErr: tc.dayErr, dayResult: []repository.MerchantDayBatch{
				{Currency: "ARS", Settled: true, TotalAmount: 1000},
			}}
			h, pool := newEventAdmin(t, repo, tc.publisher, 2.5)
			expectRolledBackTx(pool)

			if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
				t.Fatal("SettleBatch() error = nil, want it to fail")
			}
		})
	}
}

func TestReopenBatch_RejectedWhenAnotherBatchIsOpen(t *testing.T) {
	b := newDisputedBatch(t)
	other := newOpenBatch(t, b.TerminalID(), b.MerchantID())
	repo := &fakeBatchRepo{findByIDResult: b, findOpenResult: other}
	h := command.NewAdminHandler(repo, nil)

	err := h.ReopenBatch(context.Background(), port.ReopenBatchCommand{BatchID: b.ID(), OperatorID: "op-1", Reason: "r"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
	if repo.saveCallCount != 0 {
		t.Errorf("Save call count = %d, want 0", repo.saveCallCount)
	}
	if b.State() != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want it unchanged", b.State())
	}
}

func TestReopenBatch_FindOpenError(t *testing.T) {
	b := newDisputedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b, findOpenErr: errors.New("connection reset")}
	err := command.NewAdminHandler(repo, nil).ReopenBatch(context.Background(), port.ReopenBatchCommand{BatchID: b.ID(), OperatorID: "op-1", Reason: "r"})
	if err == nil || !strings.Contains(err.Error(), "ReopenBatch: find open batch") {
		t.Fatalf("error = %v, want it to mention ReopenBatch: find open batch", err)
	}
}

// ─── Auditoría ────────────────────────────────────────────────────────────────

func onlyAuditEntry(t *testing.T, repo *fakeBatchRepo) repository.BatchAuditEntry {
	t.Helper()
	if len(repo.auditEntries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(repo.auditEntries))
	}
	return repo.auditEntries[0]
}

func TestAudit_ForceClose(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	repo := &fakeBatchRepo{findByIDResult: b}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	expectEventTx(pool, 1)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}

	got := onlyAuditEntry(t, repo)
	if got.BatchID != b.ID() || got.Action != repository.AuditActionForceClose || got.OperatorID != "op-1" {
		t.Errorf("entry = %+v", got)
	}
	if got.FromState != "OPEN" || got.ToState != "CLOSED" {
		t.Errorf("states = %s → %s, want OPEN → CLOSED", got.FromState, got.ToState)
	}
	if got.TotalAmount == nil || *got.TotalAmount != 1000 {
		t.Errorf("TotalAmount = %v, want 1000", got.TotalAmount)
	}
}

func TestAudit_ResubmitKeepsConfirmationID(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.CloseWithBackendTotals(); err != nil {
		t.Fatalf("CloseWithBackendTotals() error = %v", err)
	}
	repo := &fakeBatchRepo{findByIDResult: b}
	pool := newMockPool(t)
	expectEventTx(pool, 0)
	h := command.NewAdminHandler(repo, &fakeProcessor{confirmationID: "conf-1"})
	h.ConfigureSettlement(pool, repo, &fakePublisher{}, outbox.NewStore(idempotencySchema), 2.5)

	if err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}

	got := onlyAuditEntry(t, repo)
	if got.Action != repository.AuditActionResubmit || got.ConfirmationID != "conf-1" || got.ToState != "SUBMITTED" {
		t.Errorf("entry = %+v", got)
	}
}

func TestAudit_ResubmitBlockedRecordsDispute(t *testing.T) {
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateClosed, Currency: "ARS", Discrepancies: 2,
	})
	repo := &fakeBatchRepo{findByIDResult: b}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	expectEventTx(pool, 0)

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: b.ID(), OperatorID: "op-1"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}

	got := onlyAuditEntry(t, repo)
	if got.Action != repository.AuditActionResubmitBlocked || got.FromState != "CLOSED" || got.ToState != "DISPUTED" || got.PreviousDiscrepancies != 2 {
		t.Errorf("entry = %+v", got)
	}
}

func TestAudit_ReopenAndResolveKeepReason(t *testing.T) {
	tests := []struct {
		name       string
		run        func(*command.AdminHandler, string) error
		wantAction string
		wantState  string
	}{
		{"reopen", func(h *command.AdminHandler, id string) error {
			return h.ReopenBatch(context.Background(), port.ReopenBatchCommand{BatchID: id, OperatorID: "op-1", Reason: "ticket 123"})
		}, repository.AuditActionReopen, "OPEN"},
		{"resolve", func(h *command.AdminHandler, id string) error {
			return h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: id, OperatorID: "op-1", Reason: "ticket 123"})
		}, repository.AuditActionResolveDispute, "CLOSED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newDisputedBatch(t)
			previous := b.Discrepancies()
			repo := &fakeBatchRepo{findByIDResult: b}
			h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
			expectEventTx(pool, 0)

			if err := tc.run(h, b.ID()); err != nil {
				t.Fatalf("operation error = %v", err)
			}

			got := onlyAuditEntry(t, repo)
			if got.Action != tc.wantAction || got.Reason != "ticket 123" || got.FromState != "DISPUTED" || got.ToState != tc.wantState {
				t.Errorf("entry = %+v", got)
			}
			if previous == 0 || got.PreviousDiscrepancies != previous {
				t.Errorf("PreviousDiscrepancies = %d, want %d", got.PreviousDiscrepancies, previous)
			}
		})
	}
}

func TestAudit_Settle(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
	repo := &fakeBatchRepo{findByIDResult: b}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	expectEventTx(pool, 0)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}

	got := onlyAuditEntry(t, repo)
	if got.Action != repository.AuditActionSettle || got.FromState != "SUBMITTED" || got.ToState != "SETTLED" {
		t.Errorf("entry = %+v", got)
	}
}

// Sin el registro de auditoría, el cambio de estado no se confirma.
func TestAudit_InsertErrorRollsBack(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
	publisher := &fakePublisher{}
	repo := &fakeBatchRepo{findByIDResult: b, auditErr: errors.New("connection reset")}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	expectRolledBackTx(pool)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
		t.Fatal("SettleBatch() error = nil, want the audit error")
	}
	if len(publisher.completed) != 0 {
		t.Errorf("completed events = %d, want 0", len(publisher.completed))
	}
}

func TestAudit_SaveErrorRecordsNothing(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 1000)
	repo := &fakeBatchRepo{findByIDResult: b, saveErr: errors.New("connection reset")}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	expectRolledBackTx(pool)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
		t.Fatal("SettleBatch() error = nil, want the Save error")
	}
	if len(repo.auditEntries) != 0 {
		t.Errorf("audit entries = %d, want 0", len(repo.auditEntries))
	}
}

// Una validación rechazada no cambia nada y no se audita.
func TestAudit_RejectedOperationRecordsNothing(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	h, _ := newEventAdmin(t, repo, &fakePublisher{}, 2.5)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: "batch-1", OperatorID: "op-1"}); err == nil {
		t.Fatal("SettleBatch() error = nil, want a validation error")
	}
	if len(repo.auditEntries) != 0 {
		t.Errorf("audit entries = %d, want 0", len(repo.auditEntries))
	}
}
