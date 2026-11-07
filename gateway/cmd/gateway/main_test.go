package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

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

// El WebSocket pasa por toda la cadena del gateway, y no lo cortan los timeouts del servidor
// público: net/http limpia los plazos de la conexión cuando el proxy la toma.
func TestHandler_WebSocketOutlivesServerTimeouts(t *testing.T) {
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			typ, msg, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			_ = conn.Write(r.Context(), typ, msg)
		}
	}))
	defer echo.Close()

	h, err := newHandler(&config.Config{
		TerminalGatewayURL:   echo.URL,
		TerminalGatewayWSURL: echo.URL,
		CORSAllowedOrigins:   []string{"http://localhost:5173"},
		PayPerMinute:         10,
		StatusPerMinute:      10,
		MaxBodyBytes:         1 << 20,
	})
	if err != nil {
		t.Fatalf("newHandler() error = %v", err)
	}
	gw := httptest.NewUnstartedServer(h)
	gw.Config.ReadTimeout = 200 * time.Millisecond
	gw.Config.WriteTimeout = 200 * time.Millisecond
	gw.Start()
	defer gw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(gw.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.CloseNow()

	time.Sleep(800 * time.Millisecond) // más que los timeouts del servidor
	if err := conn.Write(ctx, websocket.MessageText, []byte("hola")); err != nil {
		t.Fatalf("Write() después de los timeouts: %v", err)
	}
	_, msg, err := conn.Read(ctx)
	if err != nil || string(msg) != "hola" {
		t.Fatalf("Read() = %q, %v: la conexión se cortó", msg, err)
	}
}
