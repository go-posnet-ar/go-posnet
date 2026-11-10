package aggregate_test

import (
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

var lateClosedAt = time.Date(2026, 11, 10, 23, 0, 0, 0, time.UTC)

// disputedByTerminal es un lote con una compra de 1000, cerrado a las 23:00 contra un terminal que
// informó terminalCount ventas por terminalAmount.
func disputedByTerminal(t *testing.T, reconciledWith string, terminalCount int, terminalAmount int64) (*aggregate.SettlementBatch, domain.TransactionID) {
	t.Helper()
	existing := domain.NewTransactionID()
	purchase, err := entity.NewBatchTransaction("b-1", existing, 1000, "ARS", valueobject.BatchTxPurchase)
	if err != nil {
		t.Fatalf("NewBatchTransaction() error = %v", err)
	}
	closedAt := lateClosedAt
	p := aggregate.ReconstituteParams{
		ID: "b-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), State: valueobject.BatchStateDisputed, Currency: "ARS",
		Transactions: []*entity.BatchTransaction{purchase}, CreatedAt: closedAt.Add(-12 * time.Hour), ClosedAt: &closedAt,
		Discrepancies: 1, ReconciledWith: reconciledWith,
	}
	if reconciledWith == aggregate.ReconciledWithTerminal {
		p.TerminalCount, p.TerminalAmount = &terminalCount, &terminalAmount
	}
	return aggregate.Reconstitute(p), existing
}

func TestAddLateSale(t *testing.T) {
	before := lateClosedAt.Add(-time.Minute)
	tests := []struct {
		name          string
		reconciled    string
		terminalCount int
		amount        int64
		authorizedAt  time.Time
		wantAdded     bool
		wantDiscrepa  int
	}{
		{"the missing sale: reconciles", aggregate.ReconciledWithTerminal, 2, 1500, before, true, 0},
		{"a missing sale with another amount: still disputed", aggregate.ReconciledWithTerminal, 2, 1700, before, true, 1},
		{"one of two missing sales", aggregate.ReconciledWithTerminal, 3, 2200, before, true, 1},
		{"authorized after the close", aggregate.ReconciledWithTerminal, 2, 1500, lateClosedAt.Add(time.Second), false, 1},
		{"the terminal did not count more sales", aggregate.ReconciledWithTerminal, 1, 900, before, false, 1},
		{"closed without terminal totals", aggregate.ReconciledWithBackend, 0, 0, before, false, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := disputedByTerminal(t, tc.reconciled, tc.terminalCount, tc.amount)
			added, err := b.AddLateSale(domain.NewTransactionID(), 500, tc.authorizedAt)
			if err != nil {
				t.Fatalf("AddLateSale() error = %v", err)
			}
			if added != tc.wantAdded || b.Discrepancies() != tc.wantDiscrepa {
				t.Errorf("added = %v, discrepancies = %d, want %v, %d", added, b.Discrepancies(), tc.wantAdded, tc.wantDiscrepa)
			}
			if wantTx := map[bool]int{true: 2, false: 1}[tc.wantAdded]; len(b.Transactions()) != wantTx {
				t.Errorf("transactions = %d, want %d", len(b.Transactions()), wantTx)
			}
			if b.State() != valueobject.BatchStateDisputed {
				t.Errorf("state = %s, want DISPUTED: the release is separate", b.State())
			}
		})
	}
}

// Una vez agregada la venta que faltaba, el lote se libera con Release, como al cerrar una revisión.
func TestAddLateSale_ThenRelease(t *testing.T) {
	b, _ := disputedByTerminal(t, aggregate.ReconciledWithTerminal, 2, 1500)
	if _, err := b.AddLateSale(domain.NewTransactionID(), 500, lateClosedAt.Add(-time.Minute)); err != nil {
		t.Fatalf("AddLateSale() error = %v", err)
	}
	released, _, err := b.Release()
	if err != nil || !released || b.State() != valueobject.BatchStateClosed {
		t.Fatalf("Release() = %v, %v, state %s, want released and CLOSED", released, err, b.State())
	}
	if b.Summary().TotalCount() != 2 || b.Summary().TotalAmount().Cents() != 1500 {
		t.Errorf("summary = %d/%d, want 2/1500", b.Summary().TotalCount(), b.Summary().TotalAmount().Cents())
	}
}

// La misma venta dos veces no se agrega: la segunda es un reintento del evento.
func TestAddLateSale_AlreadyInTheBatch(t *testing.T) {
	b, existing := disputedByTerminal(t, aggregate.ReconciledWithTerminal, 2, 2000)
	added, err := b.AddLateSale(existing, 1000, lateClosedAt.Add(-time.Minute))
	if err != nil || added {
		t.Errorf("AddLateSale(existing) = %v, %v, want false", added, err)
	}
}

// Una reversa tardía se descuenta: con ella, el neto baja al que informó el terminal.
func TestAddLateReversal(t *testing.T) {
	// Una compra de 1000; el terminal informó la compra y su reversa: 2 transacciones, 0 neto.
	b, existing := disputedByTerminal(t, aggregate.ReconciledWithTerminal, 2, 0)

	added, err := b.AddLateReversal(existing, 1000, lateClosedAt.Add(-time.Minute))
	if err != nil || !added {
		t.Fatalf("AddLateReversal() = %v, %v, want true", added, err)
	}
	if b.Discrepancies() != 0 || b.Summary().ReversalCount() != 1 || b.Summary().TotalAmount().Cents() != 0 {
		t.Errorf("discrepancies = %d, summary = %+v, want 0 with one reversal and 0 net", b.Discrepancies(), b.Summary())
	}
	// La misma reversa otra vez no se agrega: ya está en el lote.
	if added, _ := b.AddLateReversal(existing, 1000, lateClosedAt.Add(-time.Minute)); added {
		t.Error("AddLateReversal() added the same reversal twice")
	}
}
