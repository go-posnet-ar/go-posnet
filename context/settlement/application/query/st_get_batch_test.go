package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── fakeBatchRepo ───────────────────────────────────────────────────────────

type fakeBatchRepo struct {
	findByIDResult *aggregate.SettlementBatch
	findByIDErr    error

	listResult       []*aggregate.SettlementBatch
	listErr          error
	listedMerchantID domain.MerchantID
	listedDate       time.Time
}

func (f *fakeBatchRepo) Save(context.Context, *aggregate.SettlementBatch) error { return nil }

func (f *fakeBatchRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	return f.findByIDResult, f.findByIDErr
}

func (f *fakeBatchRepo) FindOpenByTerminal(context.Context, domain.TerminalID, time.Time) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) FindOrCreateOpen(context.Context, domain.TerminalID, domain.MerchantID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) ListByMerchantDate(_ context.Context, merchantID domain.MerchantID, date time.Time) ([]*aggregate.SettlementBatch, error) {
	f.listedMerchantID = merchantID
	f.listedDate = date
	return f.listResult, f.listErr
}

func (f *fakeBatchRepo) FindStaleOpenIDs(context.Context, time.Time) ([]string, error) {
	return nil, nil
}

// ─── builders ────────────────────────────────────────────────────────────────

func newBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}

// ─── GetBatch ─────────────────────────────────────────────────────────────────

func TestGetBatch_EmptyID(t *testing.T) {
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})

	_, err := h.GetBatch(context.Background(), "")
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestGetBatch_RepoError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("connection reset")}
	h := query.NewBatchQueryHandler(repo)

	_, err := h.GetBatch(context.Background(), "batch-1")
	if err == nil || !strings.Contains(err.Error(), "GetBatch") {
		t.Fatalf("error = %v, want it to mention GetBatch", err)
	}
}

func TestGetBatch_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	h := query.NewBatchQueryHandler(repo)

	_, err := h.GetBatch(context.Background(), "batch-999")
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
}

func TestGetBatch_OpenBatch_NoOptionalFields(t *testing.T) {
	b := newBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	h := query.NewBatchQueryHandler(repo)

	got, err := h.GetBatch(context.Background(), b.ID())
	if err != nil {
		t.Fatalf("GetBatch() error = %v", err)
	}
	if got.ID != b.ID() {
		t.Errorf("ID = %q, want %q", got.ID, b.ID())
	}
	if got.TerminalID != b.TerminalID().String() {
		t.Errorf("TerminalID = %q, want %q", got.TerminalID, b.TerminalID().String())
	}
	if got.MerchantID != b.MerchantID().String() {
		t.Errorf("MerchantID = %q, want %q", got.MerchantID, b.MerchantID().String())
	}
	if got.BatchDate != "2026-01-15" {
		t.Errorf("BatchDate = %q, want %q", got.BatchDate, "2026-01-15")
	}
	if got.State != "OPEN" {
		t.Errorf("State = %q, want %q", got.State, "OPEN")
	}
	if got.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", got.Currency, "ARS")
	}
	if got.Discrepancies != 0 {
		t.Errorf("Discrepancies = %d, want 0", got.Discrepancies)
	}
	if got.TotalCount != 0 || got.TotalAmount != 0 {
		t.Errorf("TotalCount/TotalAmount = %d/%d, want 0/0 while OPEN (no summary)", got.TotalCount, got.TotalAmount)
	}
	if got.ClosedAt != "" || got.SubmittedAt != "" || got.SettledAt != "" {
		t.Error("ClosedAt/SubmittedAt/SettledAt should be empty for a batch that never closed")
	}
}

func TestGetBatch_ClosedBatch_MapsSummaryAndTimestamps(t *testing.T) {
	b := newBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b.Close(1, 1000); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := b.Submit(); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := b.MarkSettled(); err != nil {
		t.Fatalf("MarkSettled() error = %v", err)
	}

	repo := &fakeBatchRepo{findByIDResult: b}
	h := query.NewBatchQueryHandler(repo)

	got, err := h.GetBatch(context.Background(), b.ID())
	if err != nil {
		t.Fatalf("GetBatch() error = %v", err)
	}
	if got.State != "SETTLED" {
		t.Errorf("State = %q, want %q", got.State, "SETTLED")
	}
	if got.TotalCount != 1 {
		t.Errorf("TotalCount = %d, want 1", got.TotalCount)
	}
	if got.TotalAmount != 1000 {
		t.Errorf("TotalAmount = %d, want 1000", got.TotalAmount)
	}
	if got.ClosedAt == "" {
		t.Error("ClosedAt is empty, want it set")
	}
	if got.SubmittedAt == "" {
		t.Error("SubmittedAt is empty, want it set")
	}
	if got.SettledAt == "" {
		t.Error("SettledAt is empty, want it set")
	}
}

// ─── ListBatchesByMerchant ────────────────────────────────────────────────────

func TestListBatchesByMerchant_InvalidMerchantID(t *testing.T) {
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})

	_, err := h.ListBatchesByMerchant(context.Background(), port.ListBatchesCommand{MerchantID: "not-a-uuid", Date: "2026-01-15"})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestListBatchesByMerchant_InvalidDate(t *testing.T) {
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})

	_, err := h.ListBatchesByMerchant(context.Background(), port.ListBatchesCommand{
		MerchantID: domain.NewMerchantID().String(), Date: "15-01-2026",
	})
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type = %T, want *pkgerrors.ValidationError", err)
	}
}

func TestListBatchesByMerchant_RepoError(t *testing.T) {
	repo := &fakeBatchRepo{listErr: errors.New("connection reset")}
	h := query.NewBatchQueryHandler(repo)

	_, err := h.ListBatchesByMerchant(context.Background(), port.ListBatchesCommand{
		MerchantID: domain.NewMerchantID().String(), Date: "2026-01-15",
	})
	if err == nil || !strings.Contains(err.Error(), "ListBatchesByMerchant") {
		t.Fatalf("error = %v, want it to mention ListBatchesByMerchant", err)
	}
}

func TestListBatchesByMerchant_Empty(t *testing.T) {
	repo := &fakeBatchRepo{listResult: nil}
	h := query.NewBatchQueryHandler(repo)

	merchantID := domain.NewMerchantID()
	got, err := h.ListBatchesByMerchant(context.Background(), port.ListBatchesCommand{
		MerchantID: merchantID.String(), Date: "2026-01-15",
	})
	if err != nil {
		t.Fatalf("ListBatchesByMerchant() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
	if !repo.listedMerchantID.Equals(merchantID) {
		t.Errorf("listedMerchantID = %v, want %v", repo.listedMerchantID, merchantID)
	}
	wantDate := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	if !repo.listedDate.Equal(wantDate) {
		t.Errorf("listedDate = %v, want %v", repo.listedDate, wantDate)
	}
}

func TestListBatchesByMerchant_Success(t *testing.T) {
	b1 := newBatch(t)
	b2 := newBatch(t)
	if err := b2.AddTransaction(domain.NewTransactionID(), 500, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	if err := b2.RequestClose(); err != nil {
		t.Fatalf("RequestClose() error = %v", err)
	}
	if err := b2.Close(1, 500); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	repo := &fakeBatchRepo{listResult: []*aggregate.SettlementBatch{b1, b2}}
	h := query.NewBatchQueryHandler(repo)

	got, err := h.ListBatchesByMerchant(context.Background(), port.ListBatchesCommand{
		MerchantID: domain.NewMerchantID().String(), Date: "2026-01-15",
	})
	if err != nil {
		t.Fatalf("ListBatchesByMerchant() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	if got[0].ID != b1.ID() || got[1].ID != b2.ID() {
		t.Errorf("results not mapped in order: got[0].ID=%q got[1].ID=%q", got[0].ID, got[1].ID)
	}
	if got[0].TotalCount != 0 {
		t.Errorf("got[0].TotalCount = %d, want 0 (OPEN, no summary)", got[0].TotalCount)
	}
	if got[1].TotalCount != 1 || got[1].TotalAmount != 500 {
		t.Errorf("got[1].TotalCount/TotalAmount = %d/%d, want 1/500", got[1].TotalCount, got[1].TotalAmount)
	}
	if got[1].ClosedAt == "" {
		t.Error("got[1].ClosedAt is empty, want it set")
	}
}
