package aggregate_test

import (
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

func TestReconstitute(t *testing.T) {
	terminalID := domain.NewTerminalID()
	merchantID := domain.NewMerchantID()
	batchDate := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	closedAt := time.Date(2026, 1, 15, 20, 0, 0, 0, time.UTC)
	submittedAt := time.Date(2026, 1, 15, 20, 5, 0, 0, time.UTC)
	settledAt := time.Date(2026, 1, 16, 8, 0, 0, 0, time.UTC)

	tx := entity.ReconstituteBatchTransaction(
		"tx-1", "batch-1", domain.NewTransactionID(), 1000, "ARS",
		valueobject.BatchTxPurchase, createdAt,
	)
	cur, err := domain.ParseCurrency("ARS")
	if err != nil {
		t.Fatalf("ParseCurrency() error = %v", err)
	}
	money, err := domain.NewMoney(1000, cur)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	summary, err := valueobject.NewBatchSummary(1, money, 1, money, 0, domain.Money{})
	if err != nil {
		t.Fatalf("NewBatchSummary() error = %v", err)
	}

	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:            "batch-1",
		TerminalID:    terminalID,
		MerchantID:    merchantID,
		BatchDate:     batchDate,
		State:         valueobject.BatchStateSettled,
		Currency:      "ARS",
		Transactions:  []*entity.BatchTransaction{tx},
		Summary:       &summary,
		Discrepancies: 2,
		CreatedAt:     createdAt,
		ClosedAt:      &closedAt,
		SubmittedAt:   &submittedAt,
		SettledAt:     &settledAt,
	})

	if b.ID() != "batch-1" {
		t.Errorf("ID() = %q, want %q", b.ID(), "batch-1")
	}
	if !b.TerminalID().Equals(terminalID) {
		t.Errorf("TerminalID() = %v, want %v", b.TerminalID(), terminalID)
	}
	if !b.MerchantID().Equals(merchantID) {
		t.Errorf("MerchantID() = %v, want %v", b.MerchantID(), merchantID)
	}
	if !b.BatchDate().Equal(batchDate) {
		t.Errorf("BatchDate() = %v, want %v", b.BatchDate(), batchDate)
	}
	if b.State() != valueobject.BatchStateSettled {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateSettled)
	}
	if b.Currency() != "ARS" {
		t.Errorf("Currency() = %q, want %q", b.Currency(), "ARS")
	}
	if len(b.Transactions()) != 1 || b.Transactions()[0].ID() != "tx-1" {
		t.Errorf("Transactions() = %+v, want a single tx with ID %q", b.Transactions(), "tx-1")
	}
	if b.Summary() == nil || b.Summary().TotalCount() != 1 {
		t.Errorf("Summary() = %+v, want a summary with TotalCount() = 1", b.Summary())
	}
	if b.Discrepancies() != 2 {
		t.Errorf("Discrepancies() = %d, want 2", b.Discrepancies())
	}
	if !b.CreatedAt().Equal(createdAt) {
		t.Errorf("CreatedAt() = %v, want %v", b.CreatedAt(), createdAt)
	}
	if b.ClosedAt() == nil || !b.ClosedAt().Equal(closedAt) {
		t.Errorf("ClosedAt() = %v, want %v", b.ClosedAt(), closedAt)
	}
	if b.SubmittedAt() == nil || !b.SubmittedAt().Equal(submittedAt) {
		t.Errorf("SubmittedAt() = %v, want %v", b.SubmittedAt(), submittedAt)
	}
	if b.SettledAt() == nil || !b.SettledAt().Equal(settledAt) {
		t.Errorf("SettledAt() = %v, want %v", b.SettledAt(), settledAt)
	}
}

func TestReconstitute_NilOptionalFields(t *testing.T) {
	b := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         "batch-2",
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		BatchDate:  time.Now(),
		State:      valueobject.BatchStateOpen,
		Currency:   "ARS",
	})

	if b.Summary() != nil {
		t.Errorf("Summary() = %+v, want nil", b.Summary())
	}
	if b.ClosedAt() != nil {
		t.Errorf("ClosedAt() = %v, want nil", b.ClosedAt())
	}
	if b.SubmittedAt() != nil {
		t.Errorf("SubmittedAt() = %v, want nil", b.SubmittedAt())
	}
	if b.SettledAt() != nil {
		t.Errorf("SettledAt() = %v, want nil", b.SettledAt())
	}
	if len(b.Transactions()) != 0 {
		t.Errorf("Transactions() len = %d, want 0", len(b.Transactions()))
	}
}
