package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

func newTestHandler(svc *fakeSessionService, repo *fakeSessionRepo, pool *fakePool) *Handler {
	return NewHandler(svc, query.NewSessionQueryHandler(repo), pool)
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

func mustMoney(t *testing.T) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(1000, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return m
}

func mustSTAN(t *testing.T) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(123456)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
	}
	return s
}

func newSession(t *testing.T) *aggregate.PaymentSession {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelQR,
		State:      valueobject.StateAwaitingPayment,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}

// ─── healthz / readyz ─────────────────────────────────────────────────────────

func TestHandleHealth(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{}))

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
	mux := newTestMux(newTestHandler(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestHandleReady_DatabaseUnavailable(t *testing.T) {
	pool := &fakePool{pingErr: errors.New("connection refused")}
	mux := newTestMux(newTestHandler(&fakeSessionService{}, &fakeSessionRepo{}, pool))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// ─── GET /sessions/{id} ────────────────────────────────────────────────────────

func TestHandleGetSession_InvalidID(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/sessions/not-a-uuid", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INVALID_ID" {
		t.Errorf("body[error] = %v, want %q", body["error"], "INVALID_ID")
	}
}

func TestHandleGetSession_NotFound(t *testing.T) {
	repo := &fakeSessionRepo{findResult: nil}
	mux := newTestMux(newTestHandler(&fakeSessionService{}, repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/sessions/"+domain.NewTransactionID().String(), nil)
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

func TestHandleGetSession_InternalError(t *testing.T) {
	repo := &fakeSessionRepo{findErr: errors.New("db unreachable")}
	mux := newTestMux(newTestHandler(&fakeSessionService{}, repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/sessions/"+domain.NewTransactionID().String(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if strings.Contains(rec.Body.String(), "db unreachable") {
		t.Errorf("internal error message leaked to client: %s", rec.Body.String())
	}
}

func TestHandleGetSession_Success(t *testing.T) {
	session := newSession(t)
	repo := &fakeSessionRepo{findResult: session}
	mux := newTestMux(newTestHandler(&fakeSessionService{}, repo, &fakePool{}))

	req := httptest.NewRequest(http.MethodGet, "/sessions/"+session.ID().String(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["TransactionID"] != session.ID().String() {
		t.Errorf("TransactionID = %v, want %q", body["TransactionID"], session.ID().String())
	}
}

// ─── POST /sessions/{id}/cancel ─────────────────────────────────────────────────

func TestHandleCancelSession_Success(t *testing.T) {
	svc := &fakeSessionService{}
	mux := newTestMux(newTestHandler(svc, &fakeSessionRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/sessions/tx-1/cancel", nil)
	req.Header.Set("X-Terminal-ID", "term-1")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "cancelled" {
		t.Errorf("body[status] = %v, want %q", body["status"], "cancelled")
	}
	if len(svc.cancelSessionCalls) != 1 {
		t.Fatalf("CancelSession calls = %d, want 1", len(svc.cancelSessionCalls))
	}
	if svc.cancelSessionCalls[0].TransactionID != "tx-1" {
		t.Errorf("TransactionID = %q, want %q", svc.cancelSessionCalls[0].TransactionID, "tx-1")
	}
	if svc.cancelSessionCalls[0].TerminalID != "term-1" {
		t.Errorf("TerminalID = %q, want %q (del header X-Terminal-ID)", svc.cancelSessionCalls[0].TerminalID, "term-1")
	}
}

func TestHandleCancelSession_ServiceError(t *testing.T) {
	testSessionCommandErrors(t, "cancel", func(err error) *fakeSessionService {
		return &fakeSessionService{cancelSessionErr: err}
	})
}

// ─── POST /sessions/{id}/reversal ───────────────────────────────────────────────

func TestHandleRequestReversal_Success(t *testing.T) {
	svc := &fakeSessionService{}
	mux := newTestMux(newTestHandler(svc, &fakeSessionRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/sessions/tx-1/reversal", nil)
	req.Header.Set("X-Terminal-ID", "term-1")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "reversal_requested" {
		t.Errorf("body[status] = %v, want %q", body["status"], "reversal_requested")
	}
	if len(svc.requestReversalCalls) != 1 {
		t.Fatalf("RequestReversal calls = %d, want 1", len(svc.requestReversalCalls))
	}
	if svc.requestReversalCalls[0].OriginalTransactionID != "tx-1" {
		t.Errorf("OriginalTransactionID = %q, want %q", svc.requestReversalCalls[0].OriginalTransactionID, "tx-1")
	}
	if svc.requestReversalCalls[0].TerminalID != "term-1" {
		t.Errorf("TerminalID = %q, want %q", svc.requestReversalCalls[0].TerminalID, "term-1")
	}
}

func TestHandleRequestReversal_ServiceError(t *testing.T) {
	testSessionCommandErrors(t, "reversal", func(err error) *fakeSessionService {
		return &fakeSessionService{requestReversalErr: err}
	})
}

// testSessionCommandErrors verifica cómo responden cancel y reversal a cada error del caso de
// uso. Un terminal que no es un UUID, como TRM-0001, es un 400 y no un 500.
func testSessionCommandErrors(t *testing.T, action string, newService func(error) *fakeSessionService) {
	t.Helper()
	cases := []struct {
		name     string
		err      error
		wantCode int
		wantErr  string
	}{
		{"invalid terminal", pkgerrors.NewValidationError("invalid terminal_id"), http.StatusBadRequest, "INVALID_REQUEST"},
		{"missing or foreign session", pkgerrors.NewNotFoundError("PaymentSession", "tx-1"), http.StatusNotFound, "NOT_FOUND"},
		{"state does not allow it", fmt.Errorf("CancelSession: cancel: %w", aggregate.ErrInvalidTransition), http.StatusConflict, "INVALID_STATE"},
		{"infrastructure", errors.New("nats unavailable"), http.StatusInternalServerError, "INTERNAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := newTestMux(newTestHandler(newService(tc.err), &fakeSessionRepo{}, &fakePool{}))

			req := httptest.NewRequest(http.MethodPost, "/sessions/tx-1/"+action, nil)
			req.Header.Set("X-Terminal-ID", "TRM-0001")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			if body := decodeJSON(t, rec.Body.Bytes()); body["error"] != tc.wantErr {
				t.Errorf("body[error] = %v, want %q", body["error"], tc.wantErr)
			}
		})
	}
}

// ─── POST /batch-close ──────────────────────────────────────────────────────────

func TestHandleBatchClose_InvalidBody(t *testing.T) {
	mux := newTestMux(newTestHandler(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{}))

	req := httptest.NewRequest(http.MethodPost, "/batch-close", strings.NewReader("not-json"))
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

func TestHandleBatchClose_ServiceError(t *testing.T) {
	svc := &fakeSessionService{requestBatchCloseErr: errors.New("nats unavailable")}
	mux := newTestMux(newTestHandler(svc, &fakeSessionRepo{}, &fakePool{}))

	reqBody := `{"terminal_id":"term-1","merchant_id":"merch-1","terminal_count":5,"terminal_amount":5000,"currency":"ARS"}`
	req := httptest.NewRequest(http.MethodPost, "/batch-close", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleBatchClose_Success(t *testing.T) {
	svc := &fakeSessionService{}
	mux := newTestMux(newTestHandler(svc, &fakeSessionRepo{}, &fakePool{}))

	reqBody := `{"terminal_id":"term-1","merchant_id":"merch-1","batch_date":"2026-01-15","terminal_count":5,"terminal_amount":5000,"currency":"ARS"}`
	req := httptest.NewRequest(http.MethodPost, "/batch-close", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "batch_close_requested" {
		t.Errorf("body[status] = %v, want %q", body["status"], "batch_close_requested")
	}
	if len(svc.requestBatchCloseCalls) != 1 {
		t.Fatalf("RequestBatchClose calls = %d, want 1", len(svc.requestBatchCloseCalls))
	}
	call := svc.requestBatchCloseCalls[0]
	if call.TerminalID != "term-1" || call.MerchantID != "merch-1" || call.TerminalCount != 5 || call.TerminalAmount != 5000 || call.Currency != "ARS" {
		t.Errorf("call = %+v, want mapped fields from the request body", call)
	}
}
