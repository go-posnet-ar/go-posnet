package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

func newTestHandler(batchRepo *fakeBatchRepo, pool *fakePool) *Handler {
	queryHandler := query.NewBatchQueryHandler(batchRepo)
	adminHandler := command.NewAdminHandler(batchRepo, &fakeProcessor{})
	return NewHandler(queryHandler, adminHandler, pool)
}

func newTestMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func decodeJSON(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", body, err)
	}
	return m
}

// ─── healthz / readyz ─────────────────────────────────────────────────────────

func TestHandleHealth(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "ok" {
		t.Errorf("body[status] = %v, want %q", body["status"], "ok")
	}
}

func TestHandleReady_Success(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleReady_DatabaseUnavailable(t *testing.T) {
	pool := &fakePool{pingErr: errors.New("connection refused")}
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, pool))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// ─── GET /batches/{id} ────────────────────────────────────────────────────────

func TestHandleGetBatch_Success(t *testing.T) {
	b := newBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/batches/"+b.ID(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["ID"] != b.ID() {
		t.Errorf("ID = %v, want %q", body["ID"], b.ID())
	}
}

func TestHandleGetBatch_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/batches/batch-999", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "NOT_FOUND" {
		t.Errorf("body[error] = %v, want %q", body["error"], "NOT_FOUND")
	}
}

func TestHandleGetBatch_InternalError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/batches/batch-1", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if strings.Contains(rec.Body.String(), "db unreachable") {
		t.Errorf("internal error message leaked to client: %s", rec.Body.String())
	}
}

// ─── GET /merchants/{merchant_id}/batches ─────────────────────────────────────

func TestHandleListBatches_Success(t *testing.T) {
	b := newBatch(t)
	repo := &fakeBatchRepo{listResult: []*aggregate.SettlementBatch{b}}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	merchantID := domain.NewMerchantID()
	req := httptest.NewRequest(http.MethodGet, "/merchants/"+merchantID.String()+"/batches?date=2026-01-15", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if count, ok := body["count"].(float64); !ok || count != 1 {
		t.Errorf("body[count] = %v, want 1", body["count"])
	}
}

func TestHandleListBatches_ValidationError(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/merchants/not-a-uuid/batches?date=2026-01-15", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "VALIDATION_ERROR" {
		t.Errorf("body[error] = %v, want %q", body["error"], "VALIDATION_ERROR")
	}
}

func TestHandleListBatches_InternalError(t *testing.T) {
	repo := &fakeBatchRepo{listErr: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/merchants/"+domain.NewMerchantID().String()+"/batches?date=2026-01-15", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// ─── POST /batches/{id}/force-close ───────────────────────────────────────────

func TestHandleForceClose_Success(t *testing.T) {
	b := newBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/force-close",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "closed" {
		t.Errorf("body[status] = %v, want %q", body["status"], "closed")
	}
}

func TestHandleForceClose_InvalidBody(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/force-close", strings.NewReader(`not-json`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INVALID_BODY" {
		t.Errorf("body[error] = %v, want %q", body["error"], "INVALID_BODY")
	}
}

func TestHandleForceClose_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-999/force-close",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleForceClose_ValidationError(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	// operator_id vacío — el path {id} sigue teniendo un valor no vacío.
	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/force-close", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "VALIDATION_ERROR" {
		t.Errorf("body[error] = %v, want %q", body["error"], "VALIDATION_ERROR")
	}
}

func TestHandleForceClose_InternalError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/force-close",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

// ─── POST /batches/{id}/resubmit ──────────────────────────────────────────────

func TestHandleResubmitBatch_Success(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/resubmit",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "submitted" {
		t.Errorf("body[status] = %v, want %q", body["status"], "submitted")
	}
}

func TestHandleResubmitBatch_InvalidBody(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/resubmit", strings.NewReader(`not-json`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INVALID_BODY" {
		t.Errorf("body[error] = %v, want %q", body["error"], "INVALID_BODY")
	}
}

func TestHandleResubmitBatch_NotFound(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: nil}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-999/resubmit",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleResubmitBatch_ValidationError(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeBatchRepo{}, &fakePool{}))

	// operator_id vacío — el path {id} sigue teniendo un valor no vacío.
	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/resubmit", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "VALIDATION_ERROR" {
		t.Errorf("body[error] = %v, want %q", body["error"], "VALIDATION_ERROR")
	}
}

func TestHandleResubmitBatch_InternalError(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batches/batch-1/resubmit",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleResubmitBatch_NoProcessor(t *testing.T) {
	b := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: b}
	h := NewHandler(query.NewBatchQueryHandler(repo), command.NewAdminHandler(repo, nil), &fakePool{})
	mux := newTestMux(h)

	req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/resubmit",
		strings.NewReader(`{"operator_id":"op-1"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "PROCESSOR_UNAVAILABLE" {
		t.Errorf("body[error] = %v, want %q", body["error"], "PROCESSOR_UNAVAILABLE")
	}
}

// ─── POST /batches/{id}/reopen y /resolve-dispute ─────────────────────────────

func TestHandleDisputeRoutes(t *testing.T) {
	const validBody = `{"operator_id":"op-1","reason":"conciliado"}`
	newDisputed := func() *aggregate.SettlementBatch {
		return aggregate.Reconstitute(aggregate.ReconstituteParams{
			ID: "batch-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
			State: valueobject.BatchStateDisputed, Currency: "ARS",
		})
	}

	routes := []struct{ path, wantStatus string }{
		{"/batches/batch-1/reopen", "reopened"},
		{"/batches/batch-1/resolve-dispute", "closed"},
	}

	for _, rt := range routes {
		tests := []struct {
			name     string
			repo     func() *fakeBatchRepo
			body     string
			wantCode int
			wantErr  string
		}{
			{"invalid body", func() *fakeBatchRepo { return &fakeBatchRepo{} }, `not-json`, http.StatusBadRequest, "INVALID_BODY"},
			{"missing reason", func() *fakeBatchRepo { return &fakeBatchRepo{} }, `{"operator_id":"op-1"}`, http.StatusBadRequest, "VALIDATION_ERROR"},
			{"not found", func() *fakeBatchRepo { return &fakeBatchRepo{} }, validBody, http.StatusNotFound, "NOT_FOUND"},
			{"wrong state", func() *fakeBatchRepo { return &fakeBatchRepo{findByIDResult: newClosedBatch(t)} }, validBody, http.StatusBadRequest, "VALIDATION_ERROR"},
			{"internal", func() *fakeBatchRepo { return &fakeBatchRepo{findByIDErr: errors.New("db unreachable")} }, validBody, http.StatusInternalServerError, "INTERNAL"},
		}
		for _, tc := range tests {
			t.Run(rt.path+"/"+tc.name, func(t *testing.T) {
				mux := newTestMux(newTestHandler(tc.repo(), &fakePool{}))
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, rt.path, strings.NewReader(tc.body)))
				if rec.Code != tc.wantCode {
					t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.wantCode, rec.Body.String())
				}
				if got := decodeJSON(t, rec.Body.Bytes())["error"]; got != tc.wantErr {
					t.Errorf("body[error] = %v, want %q", got, tc.wantErr)
				}
			})
		}

		t.Run(rt.path+"/success", func(t *testing.T) {
			mux := newTestMux(newTestHandler(&fakeBatchRepo{findByIDResult: newDisputed()}, &fakePool{}))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, rt.path, strings.NewReader(validBody)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if got := decodeJSON(t, rec.Body.Bytes())["status"]; got != rt.wantStatus {
				t.Errorf("body[status] = %v, want %q", got, rt.wantStatus)
			}
		})
	}
}

// ─── POST /batches/{id}/settle ────────────────────────────────────────────────

func TestHandleSettleBatch(t *testing.T) {
	newSubmitted := func() *aggregate.SettlementBatch {
		b := newBatch(t)
		if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
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

	const validBody = `{"operator_id":"op-1"}`
	tests := []struct {
		name     string
		repo     *fakeBatchRepo
		body     string
		wantCode int
		wantErr  string
	}{
		{"success", &fakeBatchRepo{findByIDResult: newSubmitted()}, validBody, http.StatusOK, ""},
		{"invalid body", &fakeBatchRepo{}, `not-json`, http.StatusBadRequest, "INVALID_BODY"},
		{"missing operator", &fakeBatchRepo{}, `{}`, http.StatusBadRequest, "VALIDATION_ERROR"},
		{"not found", &fakeBatchRepo{}, validBody, http.StatusNotFound, "NOT_FOUND"},
		{"wrong state", &fakeBatchRepo{findByIDResult: newClosedBatch(t)}, validBody, http.StatusBadRequest, "VALIDATION_ERROR"},
		{"internal", &fakeBatchRepo{findByIDErr: errors.New("db unreachable")}, validBody, http.StatusInternalServerError, "INTERNAL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mux := newTestMux(newTestHandler(tc.repo, &fakePool{}))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/batches/batch-1/settle", strings.NewReader(tc.body)))
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.wantCode, rec.Body.String())
			}
			body := decodeJSON(t, rec.Body.Bytes())
			if tc.wantErr == "" {
				if body["status"] != "settled" {
					t.Errorf("body[status] = %v, want %q", body["status"], "settled")
				}
				return
			}
			if body["error"] != tc.wantErr {
				t.Errorf("body[error] = %v, want %q", body["error"], tc.wantErr)
			}
		})
	}
}

func TestAdminRoutes_StaleBatchReturnsConflict(t *testing.T) {
	newSubmitted := func() *aggregate.SettlementBatch {
		b := newBatch(t)
		if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
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
	tests := []struct {
		path  string
		batch *aggregate.SettlementBatch
	}{
		{"/batches/b/force-close", newBatch(t)},
		{"/batches/b/resubmit", newClosedBatch(t)},
		{"/batches/b/settle", newSubmitted()},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			repo := &fakeBatchRepo{findByIDResult: tc.batch, saveErr: repository.ErrStaleBatch}
			mux := newTestMux(newTestHandler(repo, &fakePool{}))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{"operator_id":"op-1"}`)))
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusConflict, rec.Body.String())
			}
		})
	}
}
