package aggregate_test

import (
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// heldBatch reconstruye un lote DISPUTED con una compra de 1000, como lo carga la base después de
// cerrarse la revisión que lo retenía.
func heldBatch(t *testing.T, state valueobject.BatchState, reconciledWith string, terminalCount *int, terminalAmount *int64) *aggregate.SettlementBatch {
	t.Helper()
	purchase, err := entity.NewBatchTransaction("b-1", domain.NewTransactionID(), 1000, "ARS", valueobject.BatchTxPurchase)
	if err != nil {
		t.Fatalf("NewBatchTransaction() error = %v", err)
	}
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "b-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC), State: state, Currency: "ARS",
		Transactions: []*entity.BatchTransaction{purchase}, CreatedAt: time.Now(),
		ReconciledWith: reconciledWith, TerminalCount: terminalCount, TerminalAmount: terminalAmount,
	})
}

func ptrInt(v int) *int     { return &v }
func ptrI64(v int64) *int64 { return &v }

func TestRelease(t *testing.T) {
	tests := []struct {
		name           string
		state          valueobject.BatchState
		reconciledWith string
		terminalCount  *int
		terminalAmount *int64
		wantReleased   bool
		wantChanged    bool
		wantState      valueobject.BatchState
		wantDisc       int
	}{
		{"terminal totals match", valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, ptrInt(1), ptrI64(1000), true, true, valueobject.BatchStateClosed, 0},
		// El terminal contó la compra y su reversa, que un operador excluyó después.
		{"terminal totals no longer match", valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, ptrInt(2), ptrI64(0), false, true, valueobject.BatchStateDisputed, 1},
		{"reconciled with the backend", valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, nil, nil, true, true, valueobject.BatchStateClosed, 0},
		{"closed before the reference was recorded", valueobject.BatchStateDisputed, "", nil, nil, false, false, valueobject.BatchStateDisputed, 0},
		{"not disputed", valueobject.BatchStateOpen, aggregate.ReconciledWithBackend, nil, nil, false, false, valueobject.BatchStateOpen, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := heldBatch(t, tc.state, tc.reconciledWith, tc.terminalCount, tc.terminalAmount)

			released, changed, err := b.Release()
			if err != nil {
				t.Fatalf("Release() error = %v", err)
			}
			if released != tc.wantReleased || changed != tc.wantChanged {
				t.Errorf("Release() = released %v, changed %v, want %v, %v", released, changed, tc.wantReleased, tc.wantChanged)
			}
			if b.State() != tc.wantState || b.Discrepancies() != tc.wantDisc {
				t.Errorf("state = %s, discrepancies = %d, want %s, %d", b.State(), b.Discrepancies(), tc.wantState, tc.wantDisc)
			}
			if tc.wantChanged && (b.Summary() == nil || b.Summary().TotalAmount().Cents() != 1000) {
				t.Errorf("summary = %+v, want the current totals (1000)", b.Summary())
			}
		})
	}
}

// Cada forma de cerrar deja su referencia, y reabrir la borra.
func TestCloseRecordsTheReconciliationReference(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if count, amount := b.TerminalTotals(); b.ReconciledWith() != aggregate.ReconciledWithTerminal || count == nil || *count != 1 || amount == nil || *amount != 1000 {
		t.Errorf("after Close: %q, %v, %v, want TERMINAL with 1 and 1000", b.ReconciledWith(), count, amount)
	}

	auto := newOpenBatch(t)
	if err := auto.CloseWithBackendTotals(); err != nil {
		t.Fatalf("CloseWithBackendTotals() error = %v", err)
	}
	if count, _ := auto.TerminalTotals(); auto.ReconciledWith() != aggregate.ReconciledWithBackend || count != nil {
		t.Errorf("after CloseWithBackendTotals: %q, %v, want BACKEND without terminal totals", auto.ReconciledWith(), count)
	}

	disputed := newDisputedBatch(t)
	if err := disputed.Reopen(); err != nil {
		t.Fatalf("Reopen() error = %v", err)
	}
	if count, _ := disputed.TerminalTotals(); disputed.ReconciledWith() != "" || count != nil {
		t.Errorf("after Reopen: %q, %v, want no reference", disputed.ReconciledWith(), count)
	}

	resolved := newDisputedBatch(t)
	if err := resolved.ResolveDispute(); err != nil {
		t.Fatalf("ResolveDispute() error = %v", err)
	}
	if resolved.ReconciledWith() != aggregate.ReconciledWithBackend {
		t.Errorf("after ResolveDispute: %q, want BACKEND", resolved.ReconciledWith())
	}
}

// Un ajuste entra en el total que se liquida, pero no en lo que se compara con el terminal.
func TestAdjustment_CountsInTheTotalButNotInTheReconciliation(t *testing.T) {
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.AddAdjustment(domain.NewTransactionID(), -400); err != nil {
		t.Fatalf("AddAdjustment() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	// El terminal solo conoce la compra.
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if b.Discrepancies() != 0 {
		t.Errorf("discrepancies = %d, want 0: the terminal does not know the adjustment", b.Discrepancies())
	}
	s := b.Summary()
	if s.TotalCount() != 1 || s.TotalAmount().Cents() != 600 || s.AdjustmentCount() != 1 || s.AdjustmentAmount() != -400 {
		t.Errorf("summary = count %d, total %d, adjustments %d / %d, want 1, 600, 1 / -400",
			s.TotalCount(), s.TotalAmount().Cents(), s.AdjustmentCount(), s.AdjustmentAmount())
	}
	if count, amount, _ := b.BackendTotals(); count != 1 || amount != 1000 {
		t.Errorf("BackendTotals() = %d, %d, want 1, 1000", count, amount)
	}
}

func TestAdjustment_CannotBeZero(t *testing.T) {
	if err := newOpenBatch(t).AddAdjustment(domain.NewTransactionID(), 0); err == nil {
		t.Fatal("AddAdjustment(0) error = nil, want an error")
	}
}
