package aggregate_test

import (
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/event"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── builders ────────────────────────────────────────────────────────────────

func newOpenBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(
		domain.NewTerminalID(), domain.NewMerchantID(),
		time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC), "ARS",
	)
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	b.ClearDomainEvents()
	return b
}

// ─── NewSettlementBatch ───────────────────────────────────────────────────────

func TestNewSettlementBatch_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	merchantID := domain.NewMerchantID()
	batchDate := time.Date(2026, 1, 15, 14, 30, 0, 0, time.UTC)

	b, err := aggregate.NewSettlementBatch(terminalID, merchantID, batchDate, "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}

	if b.ID() == "" {
		t.Error("ID() is empty, want a generated UUID")
	}
	if !b.TerminalID().Equals(terminalID) {
		t.Errorf("TerminalID() = %v, want %v", b.TerminalID(), terminalID)
	}
	if !b.MerchantID().Equals(merchantID) {
		t.Errorf("MerchantID() = %v, want %v", b.MerchantID(), merchantID)
	}
	if b.Currency() != "ARS" {
		t.Errorf("Currency() = %q, want %q", b.Currency(), "ARS")
	}
	if b.State() != valueobject.BatchStateOpen {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateOpen)
	}
	if len(b.Transactions()) != 0 {
		t.Errorf("Transactions() len = %d, want 0", len(b.Transactions()))
	}
	if b.Summary() != nil {
		t.Error("Summary() is not nil, want nil while OPEN")
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if time.Since(b.CreatedAt()) > 5*time.Second {
		t.Errorf("CreatedAt() = %v, want close to now", b.CreatedAt())
	}
	if b.ClosedAt() != nil || b.SubmittedAt() != nil || b.SettledAt() != nil {
		t.Error("ClosedAt/SubmittedAt/SettledAt should be nil right after creation")
	}

	events := b.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("DomainEvents() len = %d, want 1", len(events))
	}
	opened, ok := events[0].(event.BatchOpened)
	if !ok {
		t.Fatalf("event type = %T, want event.BatchOpened", events[0])
	}
	if opened.BatchID != b.ID() {
		t.Errorf("BatchOpened.BatchID = %q, want %q", opened.BatchID, b.ID())
	}
	if !opened.TerminalID.Equals(terminalID) {
		t.Errorf("BatchOpened.TerminalID = %v, want %v", opened.TerminalID, terminalID)
	}
	if !opened.BatchDate.Equal(batchDate) {
		t.Errorf("BatchOpened.BatchDate = %v, want %v", opened.BatchDate, batchDate)
	}
}

func TestNewSettlementBatch_TruncatesBatchDateToDay(t *testing.T) {
	batchDate := time.Date(2026, 1, 15, 14, 30, 45, 0, time.UTC)
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), batchDate, "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}

	want := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !b.BatchDate().Equal(want) {
		t.Errorf("BatchDate() = %v, want %v (truncated to day)", b.BatchDate(), want)
	}
}

func TestNewSettlementBatch_ValidationErrors(t *testing.T) {
	tests := []struct {
		name       string
		terminalID domain.TerminalID
		merchantID domain.MerchantID
		currency   string
		wantSubstr string
	}{
		{"zero terminal_id", domain.TerminalID{}, domain.NewMerchantID(), "ARS", "terminal_id"},
		{"zero merchant_id", domain.NewTerminalID(), domain.MerchantID{}, "ARS", "merchant_id"},
		{"empty currency", domain.NewTerminalID(), domain.NewMerchantID(), "", "currency"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := aggregate.NewSettlementBatch(tc.terminalID, tc.merchantID, time.Now(), tc.currency)
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantSubstr)
			}
		})
	}
}

// ─── AddTransaction ────────────────────────────────────────────────────────────

func TestAddTransaction_Success(t *testing.T) {
	b := newOpenBatch(t)
	tx1 := domain.NewTransactionID()
	tx2 := domain.NewTransactionID()

	if err := b.AddTransaction(tx1, 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddTransaction(tx2, 2000, valueobject.BatchTxOffline); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}

	txs := b.Transactions()
	if len(txs) != 2 {
		t.Fatalf("Transactions() len = %d, want 2", len(txs))
	}
	if !txs[0].TransactionID().Equals(tx1) || txs[0].AmountCents() != 1000 || txs[0].Type() != valueobject.BatchTxPurchase {
		t.Errorf("txs[0] = %+v, want tx1/1000/PURCHASE", txs[0])
	}
	if !txs[1].TransactionID().Equals(tx2) || txs[1].AmountCents() != 2000 || txs[1].Type() != valueobject.BatchTxOffline {
		t.Errorf("txs[1] = %+v, want tx2/2000/OFFLINE", txs[1])
	}
	if txs[0].Currency() != "ARS" {
		t.Errorf("txs[0].Currency() = %q, want %q", txs[0].Currency(), "ARS")
	}
}

func TestAddTransaction_ErrorWhenNotOpen(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}

	err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase)
	if err == nil || !strings.Contains(err.Error(), "cannot add transaction in state") {
		t.Fatalf("error = %v, want it to mention the invalid state", err)
	}
}

func TestAddTransaction_PropagatesEntityError(t *testing.T) {
	b := newOpenBatch(t)
	err := b.AddTransaction(domain.NewTransactionID(), 0, valueobject.BatchTxPurchase)
	if err == nil || !strings.Contains(err.Error(), "create batch transaction") {
		t.Fatalf("error = %v, want it to wrap the entity validation error", err)
	}
}

// ─── RemoveTransaction ─────────────────────────────────────────────────────────

func TestRemoveTransaction_Success(t *testing.T) {
	b := newOpenBatch(t)
	txID := domain.NewTransactionID()

	if err := b.RemoveTransaction(txID); err != nil {
		t.Fatalf("RemoveTransaction() error = %v", err)
	}

	txs := b.Transactions()
	if len(txs) != 1 {
		t.Fatalf("Transactions() len = %d, want 1", len(txs))
	}
	if txs[0].Type() != valueobject.BatchTxReversal {
		t.Errorf("Type() = %v, want %v", txs[0].Type(), valueobject.BatchTxReversal)
	}
	if txs[0].AmountCents() != 1 {
		t.Errorf("AmountCents() = %d, want 1", txs[0].AmountCents())
	}
	if !txs[0].TransactionID().Equals(txID) {
		t.Errorf("TransactionID() = %v, want %v", txs[0].TransactionID(), txID)
	}
}

func TestRemoveTransaction_ErrorWhenNotOpen(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}

	err := b.RemoveTransaction(domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "cannot remove transaction in state") {
		t.Fatalf("error = %v, want it to mention the invalid state", err)
	}
}

func TestRemoveTransaction_PropagatesEntityError(t *testing.T) {
	b := newOpenBatch(t)
	err := b.RemoveTransaction(domain.TransactionID{})
	if err == nil {
		t.Fatal("RemoveTransaction() error = nil, want the entity's zero-id validation error")
	}
}

// ─── RequestClose ──────────────────────────────────────────────────────────────

func TestRequestClose_Success(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if b.State() != valueobject.BatchStatePendingClose {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStatePendingClose)
	}

	events := b.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("DomainEvents() len = %d, want 1", len(events))
	}
	if _, ok := events[0].(event.BatchCloseRequested); !ok {
		t.Fatalf("event type = %T, want event.BatchCloseRequested", events[0])
	}
}

func TestRequestClose_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}

	err := b.RequestClose()
	if err == nil || !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("error = %v, want it to mention the invalid transition", err)
	}
}

// ─── Close ─────────────────────────────────────────────────────────────────────

func closableBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddTransaction(domain.NewTransactionID(), 2000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RemoveTransaction(domain.NewTransactionID()); err != nil { // reversal, amountCents=1
		t.Fatalf("RemoveTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	b.ClearDomainEvents()
	return b
}

func TestClose_Success_NoDiscrepancy(t *testing.T) {
	b := closableBatch(t) // 2 purchases (1000+2000) + 1 reversal (1) → total 3 tx, net 2999

	if err := b.Close(3, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if b.ClosedAt() == nil {
		t.Fatal("ClosedAt() is nil, want it set")
	}

	summary := b.Summary()
	if summary == nil {
		t.Fatal("Summary() is nil, want it set")
	}
	if summary.TotalCount() != 3 {
		t.Errorf("Summary.TotalCount() = %d, want 3", summary.TotalCount())
	}
	if summary.TotalAmount().Cents() != 2999 {
		t.Errorf("Summary.TotalAmount().Cents() = %d, want 2999", summary.TotalAmount().Cents())
	}
	if summary.PurchaseCount() != 2 {
		t.Errorf("Summary.PurchaseCount() = %d, want 2", summary.PurchaseCount())
	}
	if summary.PurchaseAmount().Cents() != 3000 {
		t.Errorf("Summary.PurchaseAmount().Cents() = %d, want 3000", summary.PurchaseAmount().Cents())
	}
	if summary.ReversalCount() != 1 {
		t.Errorf("Summary.ReversalCount() = %d, want 1", summary.ReversalCount())
	}
	if summary.ReversalAmount().Cents() != 1 {
		t.Errorf("Summary.ReversalAmount().Cents() = %d, want 1", summary.ReversalAmount().Cents())
	}

	events := b.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("DomainEvents() len = %d, want 1", len(events))
	}
	closed, ok := events[0].(event.BatchClosed)
	if !ok {
		t.Fatalf("event type = %T, want event.BatchClosed", events[0])
	}
	if closed.Discrepancies != 0 {
		t.Errorf("BatchClosed.Discrepancies = %d, want 0", closed.Discrepancies)
	}
}

func TestClose_DiscrepancyOnCountMismatch(t *testing.T) {
	b := closableBatch(t) // backend total count = 3

	if err := b.Close(5, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() != 2 {
		t.Errorf("Discrepancies() = %d, want 2 (abs(5-3))", b.Discrepancies())
	}
}

func TestClose_DiscrepancyOnCountMismatch_NegativeDiff(t *testing.T) {
	b := closableBatch(t) // backend total count = 3

	if err := b.Close(1, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() != 2 {
		t.Errorf("Discrepancies() = %d, want 2 (abs(1-3))", b.Discrepancies())
	}
}

func TestClose_EmptyBatch(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}

	if err := b.Close(0, 0); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if !b.Summary().IsZero() {
		t.Error("Summary().IsZero() = false, want true for an empty batch")
	}
}

func TestClose_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t) // still OPEN — never requested close
	err := b.Close(0, 0)
	if err == nil || !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("error = %v, want it to mention the invalid transition", err)
	}
}

// TestClose_AmountOnlyMismatchIsReportedAsDiscrepancy cubre el caso donde
// terminalCount coincide pero terminalAmount no: antes de la corrección
// Discrepancies() daba 0 (countDiff==0), enmascarando el desajuste de monto
// frente a callers que solo chequean Discrepancies() > 0 (p.ej. st_batch_handler.go).
func TestClose_AmountOnlyMismatchIsReportedAsDiscrepancy(t *testing.T) {
	b := closableBatch(t) // backend total count = 3, total amount = 2999

	if err := b.Close(3, 999999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() == 0 {
		t.Error("Discrepancies() = 0, want non-zero — el monto no coincide aunque el conteo sí")
	}
}

// ─── Submit ────────────────────────────────────────────────────────────────────

func TestSubmit_Success(t *testing.T) {
	b := closableBatch(t)
	if err := b.Close(3, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := b.Submit(); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if b.State() != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateSubmitted)
	}
	if b.SubmittedAt() == nil {
		t.Error("SubmittedAt() is nil, want it set")
	}
}

func TestSubmit_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t)
	err := b.Submit()
	if err == nil || !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("error = %v, want it to mention the invalid transition", err)
	}
}

// ─── MarkSettled ───────────────────────────────────────────────────────────────

func TestMarkSettled_Success(t *testing.T) {
	b := closableBatch(t)
	if err := b.Close(3, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.Submit(); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	b.ClearDomainEvents()

	if err := b.MarkSettled(); err != nil {
		t.Fatalf("MarkSettled() error = %v", err)
	}
	if b.State() != valueobject.BatchStateSettled {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateSettled)
	}
	if b.SettledAt() == nil {
		t.Fatal("SettledAt() is nil, want it set")
	}

	events := b.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("DomainEvents() len = %d, want 1", len(events))
	}
	settled, ok := events[0].(event.BatchSettled)
	if !ok {
		t.Fatalf("event type = %T, want event.BatchSettled", events[0])
	}
	if settled.Summary.TotalCount() != b.Summary().TotalCount() {
		t.Errorf("BatchSettled.Summary.TotalCount() = %d, want %d", settled.Summary.TotalCount(), b.Summary().TotalCount())
	}
}

func TestMarkSettled_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t)
	err := b.MarkSettled()
	if err == nil || !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("error = %v, want it to mention the invalid transition", err)
	}
}

// ─── MarkDisputed ──────────────────────────────────────────────────────────────

func TestMarkDisputed_FromPendingClose(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	b.ClearDomainEvents()

	if err := b.MarkDisputed("manual review requested"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	if b.State() != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateDisputed)
	}

	events := b.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("DomainEvents() len = %d, want 1", len(events))
	}
	disputed, ok := events[0].(event.BatchDisputed)
	if !ok {
		t.Fatalf("event type = %T, want event.BatchDisputed", events[0])
	}
	if disputed.Reason != "manual review requested" {
		t.Errorf("BatchDisputed.Reason = %q, want %q", disputed.Reason, "manual review requested")
	}
}

func TestMarkDisputed_FromSubmitted(t *testing.T) {
	b := closableBatch(t)
	if err := b.Close(3, 2999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.Submit(); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	if err := b.MarkDisputed("terminal count/amount mismatch"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	if b.State() != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateDisputed)
	}
}

// TestMarkDisputed_FromClosed cubre el flujo real de ProcessBatchClose:
// Close() detecta discrepancias y transiciona a CLOSED, y MarkDisputed()
// debe poder marcarlo disputado inmediatamente después, sin pasar por SUBMITTED.
func TestMarkDisputed_FromClosed(t *testing.T) {
	b := closableBatch(t)
	if err := b.Close(999, 999999); err != nil { // fuerza discrepancia
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() == 0 {
		t.Fatal("Discrepancies() = 0, want non-zero to set up this scenario")
	}

	if err := b.MarkDisputed("terminal count/amount mismatch"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	if b.State() != valueobject.BatchStateDisputed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateDisputed)
	}
}

func TestMarkDisputed_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t)
	err := b.MarkDisputed("reason")
	if err == nil || !strings.Contains(err.Error(), "invalid transition") {
		t.Fatalf("error = %v, want it to mention the invalid transition", err)
	}
}

// ─── ClearDomainEvents ──────────────────────────────────────────────────────────

func TestClearDomainEvents(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if len(b.DomainEvents()) == 0 {
		t.Fatal("DomainEvents() is empty before ClearDomainEvents(), want at least 1")
	}

	b.ClearDomainEvents()
	if len(b.DomainEvents()) != 0 {
		t.Errorf("DomainEvents() len = %d after ClearDomainEvents(), want 0", len(b.DomainEvents()))
	}
}

// ─── Reopen / ResolveDispute ───────────────────────────────────────────────────

func newDisputedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(5, 9999); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.MarkDisputed("terminal count/amount mismatch"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	return b
}

func TestReopen_ClearsCloseDataAndCanCloseAgain(t *testing.T) {
	b := newDisputedBatch(t)

	if err := b.Reopen(); err != nil {
		t.Fatalf("Reopen() error = %v", err)
	}
	if b.State() != valueobject.BatchStateOpen {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateOpen)
	}
	if b.Summary() != nil || b.ClosedAt() != nil || b.Discrepancies() != 0 {
		t.Errorf("summary=%v closedAt=%v discrepancies=%d, want them cleared", b.Summary(), b.ClosedAt(), b.Discrepancies())
	}
	if len(b.Transactions()) != 1 {
		t.Errorf("Transactions() len = %d, want 1 (no debe perder transacciones)", len(b.Transactions()))
	}

	// Una vez abierto, acepta transacciones y vuelve a conciliar contra el terminal.
	if err := b.AddTransaction(domain.NewTransactionID(), 500, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(2, 1500); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
}

func TestReopen_InvalidTransition(t *testing.T) {
	if err := newOpenBatch(t).Reopen(); err == nil {
		t.Fatal("Reopen() error = nil on an OPEN batch, want an invalid transition error")
	}
}

func TestResolveDispute_AcceptsBackendTotals(t *testing.T) {
	b := newDisputedBatch(t)

	if err := b.ResolveDispute(); err != nil {
		t.Fatalf("ResolveDispute() error = %v", err)
	}
	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if got := b.Summary().TotalAmount().Cents(); got != 1000 {
		t.Errorf("TotalAmount = %d, want 1000 (totales del backend)", got)
	}
	if b.ClosedAt() == nil {
		t.Error("ClosedAt() is nil, want it set")
	}
}

func TestResolveDispute_InvalidTransition(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.ResolveDispute(); err == nil {
		t.Fatal("ResolveDispute() error = nil on an OPEN batch, want an invalid transition error")
	}
	if b.State() != valueobject.BatchStateOpen {
		t.Errorf("State() = %v, want it unchanged", b.State())
	}
}

// ─── CloseWithBackendTotals ───────────────────────────────────────────────────

func TestCloseWithBackendTotals(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}

	if err := b.CloseWithBackendTotals(); err != nil {
		t.Fatalf("CloseWithBackendTotals() error = %v", err)
	}
	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("Discrepancies() = %d, want 0", b.Discrepancies())
	}
	if got := b.Summary().TotalAmount().Cents(); got != 1000 {
		t.Errorf("TotalAmount = %d, want 1000", got)
	}
}

func TestCloseWithBackendTotals_InvalidTransition(t *testing.T) {
	b := newDisputedBatch(t)
	if err := b.CloseWithBackendTotals(); err == nil {
		t.Fatal("CloseWithBackendTotals() error = nil on a DISPUTED batch, want an error")
	}
}

func TestMarkSettled_WithoutSummary(t *testing.T) {
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateSubmitted, Currency: "ARS",
	})
	if err := b.MarkSettled(); err == nil {
		t.Fatal("MarkSettled() error = nil without a summary, want an error instead of a panic")
	}
	if b.State() != valueobject.BatchStateSubmitted {
		t.Errorf("State() = %v, want it unchanged", b.State())
	}
}
