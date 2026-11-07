package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/juantevez/go-posnet/gateway/internal/config"
)

// La cadena completa: CORS del gateway, encabezados de seguridad y el servicio detrás.
func TestHandler_Chain(t *testing.T) {
	var reached int
	tg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached++
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer tg.Close()

	const front = "http://localhost:5173"
	h, err := newHandler(&config.Config{
		TerminalGatewayURL: tg.URL,
		CORSAllowedOrigins: []string{front},
		PayPerMinute:       10,
		StatusPerMinute:    10,
		MaxBodyBytes:       1 << 20,
	})
	if err != nil {
		t.Fatalf("newHandler() error = %v", err)
	}
	path := "/api/sessions/7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10/pay"

	// El preflight del navegador lo responde el gateway.
	pre := httptest.NewRequest(http.MethodOptions, path, nil)
	pre.Header.Set("Origin", front)
	pre.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, pre)
	if rec.Code != http.StatusNoContent || reached != 0 {
		t.Fatalf("preflight: status = %d, reached = %d", rec.Code, reached)
	}

	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.Header.Set("Origin", front)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || reached != 1 {
		t.Fatalf("pay: status = %d, reached = %d", rec.Code, reached)
	}
	if got := rec.Header().Values("Access-Control-Allow-Origin"); len(got) != 1 || got[0] != front {
		t.Errorf("Access-Control-Allow-Origin = %v, want solo el del gateway", got)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("falta X-Content-Type-Options")
	}

	// Sin SETTLEMENT_URL, sus rutas no existen.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/reversal-reviews", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("settlement desactivado: status = %d, want 404", rec.Code)
	}
}
