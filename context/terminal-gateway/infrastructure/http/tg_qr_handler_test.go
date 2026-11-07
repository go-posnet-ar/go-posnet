package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

func mustSessionCreatedResult(t *testing.T) *port.SessionCreatedResult {
	t.Helper()
	return &port.SessionCreatedResult{
		TransactionID: domain.NewTransactionID().String(),
		ExpiresAt:     "2026-01-15T12:05:00Z",
		TTLSeconds:    300,
		Channel:       "QR",
		Amount:        mustMoney(t),
	}
}

func newTestQRHandler(svc *fakeSessionService, repo *fakeSessionRepo) *QRHandler {
	return NewQRHandler(svc, query.NewSessionQueryHandler(repo))
}

func newQRTestMux(h *QRHandler) *http.ServeMux {
	mux := http.NewServeMux()
	h.RegisterQRRoutes(mux)
	return mux
}

// ─── POST /api/sessions/create ────────────────────────────────────────────────

func TestHandleCreateQRSession_InvalidBody(t *testing.T) {
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader("not-json"))
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

func TestHandleCreateQRSession_InvalidAmount(t *testing.T) {
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader(`{"amount_cents":0}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["error"] != "INVALID_AMOUNT" {
		t.Errorf("body[error] = %v, want %q", body["error"], "INVALID_AMOUNT")
	}
}

func TestHandleCreateQRSession_DefaultsAppliedWhenFieldsEmpty(t *testing.T) {
	svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader(`{"amount_cents":1000}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if len(svc.createSessionCalls) != 1 {
		t.Fatalf("CreateSession calls = %d, want 1", len(svc.createSessionCalls))
	}
	call := svc.createSessionCalls[0]
	if call.TerminalID != "a1b2c3d4-e5f6-7890-abcd-ef1234567890" {
		t.Errorf("TerminalID = %q, want the default UUID", call.TerminalID)
	}
	if call.MerchantID != "b2c3d4e5-f6a7-8901-bcde-f12345678901" {
		t.Errorf("MerchantID = %q, want the default UUID", call.MerchantID)
	}
	if call.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", call.Currency, "ARS")
	}
	if call.STAN < 1 || call.STAN > 999999 {
		t.Errorf("STAN = %d, want in range [1, 999999]", call.STAN)
	}
	if call.PaymentChannel != "QR" {
		t.Errorf("PaymentChannel = %q, want %q", call.PaymentChannel, "QR")
	}
}

func TestHandleCreateQRSession_ServiceError(t *testing.T) {
	svc := &fakeSessionService{createSessionErr: errors.New("terminal is not active")}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader(`{"amount_cents":1000}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleCreateQRSession_Success(t *testing.T) {
	svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/create", strings.NewReader(
		`{"terminal_id":"term-1","merchant_id":"merch-1","amount_cents":1000,"currency":"ARS"}`,
	))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["transaction_id"] != svc.createSessionResult.TransactionID {
		t.Errorf("transaction_id = %v, want %q", body["transaction_id"], svc.createSessionResult.TransactionID)
	}
	qrContent, ok := body["qr_content"].(string)
	if !ok || !strings.Contains(qrContent, "/pay/"+svc.createSessionResult.TransactionID) {
		t.Errorf("qr_content = %v, want it to contain /pay/%s", body["qr_content"], svc.createSessionResult.TransactionID)
	}
	if svc.createSessionCalls[0].TerminalID != "term-1" {
		t.Errorf("TerminalID = %q, want %q", svc.createSessionCalls[0].TerminalID, "term-1")
	}
}

// ─── POST /api/sessions/{id}/pay ───────────────────────────────────────────────

func TestHandleSimulatePay_InvalidTransactionID(t *testing.T) {
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/not-a-uuid/pay", nil)
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

func TestHandleSimulatePay_DefaultsAppliedWhenBodyEmpty(t *testing.T) {
	svc := &fakeSessionService{}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	txID := domain.NewTransactionID().String()
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+txID+"/pay", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if len(svc.processPaymentCalls) != 1 {
		t.Fatalf("ProcessPayment calls = %d, want 1", len(svc.processPaymentCalls))
	}
	call := svc.processPaymentCalls[0]
	if call.TransactionID != txID {
		t.Errorf("TransactionID = %q, want %q", call.TransactionID, txID)
	}
	if call.CardLast4 != "0000" {
		t.Errorf("CardLast4 = %q, want %q (default)", call.CardLast4, "0000")
	}
	if call.CardNetwork != "VISA" {
		t.Errorf("CardNetwork = %q, want %q (default)", call.CardNetwork, "VISA")
	}
}

func TestHandleSimulatePay_NotFound(t *testing.T) {
	svc := &fakeSessionService{processPaymentErr: pkgerrors.NewNotFoundError("PaymentSession", "tx-1")}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+domain.NewTransactionID().String()+"/pay", nil)
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

func TestHandleSimulatePay_ServiceError(t *testing.T) {
	svc := &fakeSessionService{processPaymentErr: errors.New("connection reset")}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+domain.NewTransactionID().String()+"/pay", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestHandleSimulatePay_Success(t *testing.T) {
	svc := &fakeSessionService{}
	mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

	txID := domain.NewTransactionID().String()
	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+txID+"/pay", strings.NewReader(
		`{"card_last4":"1234","card_network":"MASTERCARD","entry_mode":"CHIP"}`,
	))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["status"] != "processing" {
		t.Errorf("body[status] = %v, want %q", body["status"], "processing")
	}
	if body["transaction_id"] != txID {
		t.Errorf("body[transaction_id] = %v, want %q", body["transaction_id"], txID)
	}
	if svc.processPaymentCalls[0].CardLast4 != "1234" {
		t.Errorf("CardLast4 = %q, want %q", svc.processPaymentCalls[0].CardLast4, "1234")
	}
}

// ─── GET /api/sessions/{id}/status ─────────────────────────────────────────────

func TestHandleSessionStatus_InvalidTransactionID(t *testing.T) {
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, &fakeSessionRepo{}))

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/not-a-uuid/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleSessionStatus_NotFound(t *testing.T) {
	repo := &fakeSessionRepo{findResult: nil}
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, repo))

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+domain.NewTransactionID().String()+"/status", nil)
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

func TestHandleSessionStatus_InternalError(t *testing.T) {
	repo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, repo))

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+domain.NewTransactionID().String()+"/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if strings.Contains(rec.Body.String(), "connection reset") {
		t.Errorf("internal error message leaked to client: %s", rec.Body.String())
	}
}

func TestHandleSessionStatus_Success(t *testing.T) {
	session := newSession(t)
	repo := &fakeSessionRepo{findResult: session}
	mux := newQRTestMux(newTestQRHandler(&fakeSessionService{}, repo))

	req := httptest.NewRequest(http.MethodGet, "/api/sessions/"+session.ID().String()+"/status", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["transaction_id"] != session.ID().String() {
		t.Errorf("transaction_id = %v, want %q", body["transaction_id"], session.ID().String())
	}
	if body["state"] != valueobject.StateAwaitingPayment.String() {
		t.Errorf("state = %v, want %q", body["state"], valueobject.StateAwaitingPayment.String())
	}
}

// ─── getLocalIP ───────────────────────────────────────────────────────────────

func TestGetLocalIP_UsesEnvOverride(t *testing.T) {
	t.Setenv("POSNET_HOST", "192.168.1.100")

	if got := getLocalIP(); got != "192.168.1.100" {
		t.Errorf("getLocalIP() = %q, want %q", got, "192.168.1.100")
	}
}

func TestGetLocalIP_ReturnsNonEmptyWithoutOverride(t *testing.T) {
	t.Setenv("POSNET_HOST", "")

	if got := getLocalIP(); got == "" {
		t.Error("getLocalIP() = empty, want a non-empty IP or \"localhost\" fallback")
	}
}

func TestHandleSimulatePay_CardToken(t *testing.T) {
	txID := domain.NewTransactionID().String()
	token := strings.Repeat("a", 64)
	cases := []struct {
		name      string
		body      string
		want      int
		wantToken string
	}{
		{"con token", `{"card_token":"` + token + `"}`, http.StatusAccepted, token},
		{"sin token", `{}`, http.StatusAccepted, ""},
		{"token inválido", `{"card_token":"corto"}`, http.StatusBadRequest, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeSessionService{}
			mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/sessions/"+txID+"/pay", strings.NewReader(tc.body)))
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want != http.StatusAccepted {
				if len(svc.processPaymentCalls) != 0 {
					t.Error("an invalid token should not start the payment")
				}
				return
			}
			if len(svc.processPaymentCalls) != 1 || svc.processPaymentCalls[0].CardToken != tc.wantToken {
				t.Errorf("calls = %+v, want token %q", svc.processPaymentCalls, tc.wantToken)
			}
		})
	}
}

func TestHandleSimulatePay_CardData(t *testing.T) {
	const pan = "4111111111111111"
	cases := []struct {
		name     string
		body     string
		want     int
		wantCode string
	}{
		{"válidos", `{"card_last4":"4242","card_network":"MASTERCARD"}`, http.StatusAccepted, ""},
		{"red desconocida permitida", `{"card_last4":"4242","card_network":"UNKNOWN"}`, http.StatusAccepted, ""},
		{"PAN en card_last4", `{"card_last4":"` + pan + `"}`, http.StatusBadRequest, "INVALID_CARD_LAST4"},
		{"PAN en card_network", `{"card_network":"` + pan + `"}`, http.StatusBadRequest, "INVALID_CARD_NETWORK"},
		{"menos de 4 dígitos", `{"card_last4":"12"}`, http.StatusBadRequest, "INVALID_CARD_LAST4"},
		{"no numérico", `{"card_last4":"12a4"}`, http.StatusBadRequest, "INVALID_CARD_LAST4"},
		{"red en minúscula", `{"card_network":"visa"}`, http.StatusBadRequest, "INVALID_CARD_NETWORK"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeSessionService{}
			mux := newQRTestMux(newTestQRHandler(svc, &fakeSessionRepo{}))
			txID := domain.NewTransactionID().String()

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/sessions/"+txID+"/pay", strings.NewReader(tc.body)))

			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.want, rec.Body)
			}
			if tc.want == http.StatusAccepted {
				if len(svc.processPaymentCalls) != 1 {
					t.Errorf("ProcessPayment calls = %d, want 1", len(svc.processPaymentCalls))
				}
				return
			}
			// Sin empezar el pago, la sesión sigue disponible para reintentar.
			if len(svc.processPaymentCalls) != 0 {
				t.Error("datos inválidos no deben empezar el pago")
			}
			if !strings.Contains(rec.Body.String(), tc.wantCode) {
				t.Errorf("body = %s, want code %s", rec.Body, tc.wantCode)
			}
			if strings.Contains(rec.Body.String(), pan) {
				t.Errorf("body = %s: la respuesta no debe repetir el valor recibido", rec.Body)
			}
		})
	}
}
