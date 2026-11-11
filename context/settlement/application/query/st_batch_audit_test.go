package query_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

type fakeAuditReader struct {
	entries []repository.StoredAuditEntry
	err     error
	limit   int
}

func (f *fakeAuditReader) ListBatchAudit(_ context.Context, _ string, limit int) ([]repository.StoredAuditEntry, error) {
	f.limit = limit
	if len(f.entries) > limit {
		return f.entries[:limit], f.err
	}
	return f.entries, f.err
}

func auditEntries(n int) []repository.StoredAuditEntry {
	amount := int64(1500)
	out := make([]repository.StoredAuditEntry, n)
	for i := range out {
		out[i] = repository.StoredAuditEntry{
			ID: fmt.Sprintf("a-%d", i), CreatedAt: time.Date(2026, 11, 5, 12, 0, i, 0, time.FixedZone("BA", -3*3600)),
			BatchAuditEntry: repository.BatchAuditEntry{
				BatchID: "b-1", Action: repository.AuditActionResubmit, OperatorID: "op-1",
				FromState: "CLOSED", ToState: "SUBMITTED", TotalAmount: &amount, ConfirmationID: "conf-1",
			},
		}
	}
	return out
}

func TestGetBatchAudit_MapsEntries(t *testing.T) {
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})
	h.ConfigureAudit(&fakeAuditReader{entries: auditEntries(1)})

	got, truncated, err := h.GetBatchAudit(context.Background(), "b-1")
	if err != nil || truncated || len(got) != 1 {
		t.Fatalf("GetBatchAudit() = %v, %v, %v, want one entry", got, truncated, err)
	}
	e := got[0]
	if e.Action != "RESUBMIT" || e.OperatorID != "op-1" || *e.TotalAmount != 1500 || e.ConfirmationID != "conf-1" ||
		e.CreatedAt != "2026-11-05T15:00:00Z" {
		t.Errorf("entry = %+v", e)
	}
}

// Sin registros, un lote que no existe es 404, y uno que existe, una lista vacía.
func TestGetBatchAudit_EmptyDistinguishesMissingBatch(t *testing.T) {
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})
	h.ConfigureAudit(&fakeAuditReader{})
	_, _, err := h.GetBatchAudit(context.Background(), "b-1")
	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Errorf("GetBatchAudit() error = %v, want NotFoundError", err)
	}

	h = query.NewBatchQueryHandler(&fakeBatchRepo{findByIDResult: newBatch(t)})
	h.ConfigureAudit(&fakeAuditReader{})
	got, _, err := h.GetBatchAudit(context.Background(), "b-1")
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("GetBatchAudit() = %v, %v, want an empty list", got, err)
	}
}

func TestGetBatchAudit_Truncated(t *testing.T) {
	reader := &fakeAuditReader{entries: auditEntries(query.MaxListedAuditEntries + 3)}
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})
	h.ConfigureAudit(reader)

	got, truncated, err := h.GetBatchAudit(context.Background(), "b-1")
	if err != nil || !truncated || len(got) != query.MaxListedAuditEntries {
		t.Errorf("GetBatchAudit() = %d entries, truncated %v, %v, want %d and true", len(got), truncated, err, query.MaxListedAuditEntries)
	}
	if reader.limit != query.MaxListedAuditEntries+1 {
		t.Errorf("limit = %d, want one more than the maximum", reader.limit)
	}
}

func TestGetBatchAudit_Errors(t *testing.T) {
	if _, _, err := query.NewBatchQueryHandler(&fakeBatchRepo{}).GetBatchAudit(context.Background(), "b-1"); err == nil {
		t.Error("GetBatchAudit() without audit configured: error = nil")
	}
	h := query.NewBatchQueryHandler(&fakeBatchRepo{})
	h.ConfigureAudit(&fakeAuditReader{err: errors.New("db down")})
	if _, _, err := h.GetBatchAudit(context.Background(), "b-1"); err == nil {
		t.Error("GetBatchAudit() with a failing reader: error = nil")
	}
	var ve *pkgerrors.ValidationError
	if _, _, err := h.GetBatchAudit(context.Background(), ""); !errors.As(err, &ve) {
		t.Errorf("GetBatchAudit(\"\") error = %v, want ValidationError", err)
	}
}
