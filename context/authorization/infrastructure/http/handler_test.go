package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

func newTestMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	return mux
}

func decodeJSON(t *testing.T, body []byte) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", body, err)
	}
	return m
}

func TestHandleHealth(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeQueryService{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "ok" {
		t.Errorf("body[status] = %q, want %q", body["status"], "ok")
	}
}

func TestHandleReady_Success(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeQueryService{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "ready" {
		t.Errorf("body[status] = %q, want %q", body["status"], "ready")
	}
}

func TestHandleReady_DatabaseUnavailable(t *testing.T) {
	pool := &fakePool{pingErr: errors.New("connection refused")}
	mux := newTestMux(newTestHandler(&fakeQueryService{}, pool))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "not ready" || body["reason"] != "database unavailable" {
		t.Errorf("body = %+v, want status=not ready reason=database unavailable", body)
	}
}

func TestHandleGetTransaction_InvalidID(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeQueryService{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/transactions/not-a-uuid", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INVALID_ID" {
		t.Errorf("body[error] = %q, want %q", body["error"], "INVALID_ID")
	}
}

func TestHandleGetTransaction_NotFound(t *testing.T) {
	id := domain.NewTransactionID()
	qs := &fakeQueryService{err: pkgerrors.NewNotFoundError("Transaction", id.String())}
	mux := newTestMux(newTestHandler(qs, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/transactions/"+id.String(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "NOT_FOUND" {
		t.Errorf("body[error] = %q, want %q", body["error"], "NOT_FOUND")
	}
}

func TestHandleGetTransaction_InternalError(t *testing.T) {
	id := domain.NewTransactionID()
	qs := &fakeQueryService{err: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(qs, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/transactions/"+id.String(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INTERNAL" {
		t.Errorf("body[error] = %q, want %q", body["error"], "INTERNAL")
	}
	if strings.Contains(rec.Body.String(), "db unreachable") {
		t.Errorf("internal error message leaked to client: %s", rec.Body.String())
	}
}

func TestHandleGetTransaction_Success(t *testing.T) {
	id := domain.NewTransactionID()
	result := &port.TransactionStatusResult{
		TransactionID: id.String(),
		State:         "APPROVED",
		AuthCode:      "AB1234",
		AmountCents:   5000,
		Currency:      "ARS",
	}
	qs := &fakeQueryService{result: result}
	mux := newTestMux(newTestHandler(qs, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/transactions/"+id.String(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var body port.TransactionStatusResult
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if body.TransactionID != id.String() {
		t.Errorf("TransactionID = %q, want %q", body.TransactionID, id.String())
	}
	if body.State != "APPROVED" {
		t.Errorf("State = %q, want %q", body.State, "APPROVED")
	}
	if body.AuthCode != "AB1234" {
		t.Errorf("AuthCode = %q, want %q", body.AuthCode, "AB1234")
	}
	if body.AmountCents != 5000 {
		t.Errorf("AmountCents = %d, want 5000", body.AmountCents)
	}
}

func TestHandleReady_Schema(t *testing.T) {
	cases := []struct {
		name       string
		missing    []string
		err        error
		want       int
		wantReason string
	}{
		{"completo", nil, nil, http.StatusOK, ""},
		{"falta una columna", []string{"pn_authorization.transactions.reversal_requested_at"}, nil, http.StatusServiceUnavailable, "database schema incomplete"},
		{"no se puede consultar", nil, errors.New("connection reset"), http.StatusServiceUnavailable, "database unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := NewRouter(&fakeQueryService{}, &fakePool{}, func(context.Context) ([]string, error) { return tc.missing, tc.err })
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.wantReason != "" && !strings.Contains(rec.Body.String(), tc.wantReason) {
				t.Errorf("body = %s, want %q", rec.Body, tc.wantReason)
			}
			if strings.Contains(rec.Body.String(), "pn_authorization") {
				t.Errorf("body = %s: the endpoint has no token, it should not name the schema", rec.Body)
			}
		})
	}
}
