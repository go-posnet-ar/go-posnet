package aggregate_test

import (
	"errors"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// Un lote cargado sin sus transacciones admite agregar una, pero no calcular totales: con las
// transacciones que faltan, cerraría o liberaría con totales equivocados.
func TestPartialBatch_AppendsButCannotComputeTotals(t *testing.T) {
	partial := func(state valueobject.BatchState) *aggregate.SettlementBatch {
		count, amount := 2, int64(2000)
		closed := time.Now().UTC()
		return aggregate.Reconstitute(aggregate.ReconstituteParams{
			ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
			State: state, Currency: "ARS", ClosedAt: &closed,
			ReconciledWith: aggregate.ReconciledWithTerminal, TerminalCount: &count, TerminalAmount: &amount,
			TransactionsNotLoaded: true,
		})
	}

	b := partial(valueobject.BatchStateOpen)
	if err := b.AddTransaction(domain.NewTransactionID(), 500, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if got := len(b.NewTransactions()); got != 1 {
		t.Errorf("NewTransactions() = %d, want 1", got)
	}
	if _, _, err := b.BackendTotals(); !errors.Is(err, aggregate.ErrTransactionsNotLoaded) {
		t.Errorf("BackendTotals() error = %v, want ErrTransactionsNotLoaded", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 500); !errors.Is(err, aggregate.ErrTransactionsNotLoaded) {
		t.Errorf("Close() error = %v, want ErrTransactionsNotLoaded", err)
	}

	d := partial(valueobject.BatchStateDisputed)
	if _, _, err := d.Release(); !errors.Is(err, aggregate.ErrTransactionsNotLoaded) {
		t.Errorf("Release() error = %v, want ErrTransactionsNotLoaded", err)
	}
	if _, err := d.AddLateSale(domain.NewTransactionID(), 1000, time.Now().Add(-time.Minute)); !errors.Is(err, aggregate.ErrTransactionsNotLoaded) {
		t.Errorf("AddLateSale() error = %v, want ErrTransactionsNotLoaded", err)
	}
}

// Las transacciones cargadas ya están guardadas; las agregadas, hasta que se guarda el lote.
func TestNewTransactions(t *testing.T) {
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := b.AddTransaction(domain.NewTransactionID(), 100, valueobject.BatchTxPurchase); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(b.NewTransactions()); got != 2 {
		t.Fatalf("NewTransactions() = %d, want 2", got)
	}
	b.MarkPersisted()
	if got := len(b.NewTransactions()); got != 0 {
		t.Errorf("after MarkPersisted, NewTransactions() = %d, want 0", got)
	}

	loaded := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		State: valueobject.BatchStateOpen, Currency: "ARS", Transactions: b.Transactions(),
	})
	if got := len(loaded.NewTransactions()); got != 0 {
		t.Errorf("reconstituted NewTransactions() = %d, want 0", got)
	}
}
