package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/service"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// fakeAuth acepta un token y simula el límite de intentos.
type fakeAuth struct {
	token      string
	terminalID domain.TerminalID
	blocked    bool
}

func (f fakeAuth) Verify(_ *http.Request, token string) (domain.TerminalID, time.Duration, error) {
	if f.blocked {
		return domain.TerminalID{}, time.Minute, errors.New("too many failed attempts")
	}
	if token != f.token {
		return domain.TerminalID{}, 0, errors.New("invalid token")
	}
	return f.terminalID, 0, nil
}

const testOrigin = "http://192.168.0.2:5173"

func startServer(t *testing.T, auth Authenticator) (*Hub, *Server, string) {
	t.Helper()
	hub := NewHub()
	srv := NewServer(hub, auth, []string{testOrigin})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() {
		srv.Close()
		ts.Close()
	})
	return hub, srv, "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {testOrigin}}})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func send(t *testing.T, conn *websocket.Conn, v any) {
	t.Helper()
	b, _ := json.Marshal(v)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
}

func receive(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", data, err)
	}
	return msg
}

// connected autentica una conexión y espera el auth.ok.
func connected(t *testing.T, url string, auth any) *websocket.Conn {
	t.Helper()
	conn := dial(t, url)
	send(t, conn, auth)
	if msg := receive(t, conn); msg["type"] != TypeAuthOK {
		t.Fatalf("respuesta = %v, want auth.ok", msg)
	}
	return conn
}

func rejectedSession(t *testing.T, terminalID domain.TerminalID, code string) *aggregate.PaymentSession {
	t.Helper()
	money, err := domain.NewMoney(150000, domain.ARS)
	if err != nil {
		t.Fatal(err)
	}
	stan, err := domain.NewSTAN(1)
	if err != nil {
		t.Fatal(err)
	}
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: domain.NewTransactionID(), TerminalID: terminalID, MerchantID: domain.NewMerchantID(),
		Amount: money, STAN: stan, Channel: valueobject.ChannelQR,
		State: valueobject.StateRejected, RejectionCode: code, RejectionReason: "Lost Card - Pick Up",
		ExpiresAt: time.Now().Add(time.Minute), CreatedAt: time.Now(),
	})
}

func TestServer_TokenAuthAndResult(t *testing.T) {
	terminalID := domain.NewTerminalID()
	hub, _, url := startServer(t, fakeAuth{token: "secreto", terminalID: terminalID})
	conn := connected(t, url, map[string]string{"type": TypeAuth, "token": "secreto"})

	if _, ok := hub.ConnectedSince(terminalID); !ok {
		t.Fatal("ConnectedSince() = false después de autenticar")
	}
	session := rejectedSession(t, terminalID, "41")
	if err := hub.NotifyResult(context.Background(), session); err != nil {
		t.Fatalf("NotifyResult() error = %v", err)
	}
	msg := receive(t, conn)
	if msg["type"] != TypeSessionResult || msg["transaction_id"] != session.ID().String() ||
		msg["state"] != "REJECTED" || msg["rejection_code"] != "41" || msg["capture_card"] != true {
		t.Errorf("mensaje = %v", msg)
	}
}

func TestServer_DevelopmentAuthWithTerminalID(t *testing.T) {
	terminalID := domain.NewTerminalID()
	hub, _, url := startServer(t, nil)
	conn := connected(t, url, map[string]string{"type": TypeAuth, "terminal_id": terminalID.String()})

	if err := hub.NotifySessionExpired(context.Background(), terminalID, domain.NewTransactionID()); err != nil {
		t.Fatalf("NotifySessionExpired() error = %v", err)
	}
	if msg := receive(t, conn); msg["type"] != TypeSessionExpired {
		t.Errorf("mensaje = %v, want session.expired", msg)
	}
}

func TestServer_AuthFailures(t *testing.T) {
	cases := []struct {
		name     string
		auth     Authenticator
		first    any
		wantCode string
	}{
		{"token inválido", fakeAuth{token: "secreto"}, map[string]string{"type": TypeAuth, "token": "otro"}, "UNAUTHORIZED"},
		{"sin token", fakeAuth{token: "secreto"}, map[string]string{"type": TypeAuth}, "UNAUTHORIZED"},
		{"límite de intentos", fakeAuth{token: "secreto", blocked: true}, map[string]string{"type": TypeAuth, "token": "secreto"}, "TOO_MANY_ATTEMPTS"},
		{"primer mensaje que no es auth", fakeAuth{token: "secreto"}, map[string]string{"type": "hola"}, "AUTH_REQUIRED"},
		{"terminal_id inválido sin tokens", nil, map[string]string{"type": TypeAuth, "terminal_id": "TRM-0001"}, "INVALID_TERMINAL_ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub, _, url := startServer(t, tc.auth)
			conn := dial(t, url)
			send(t, conn, tc.first)
			if msg := receive(t, conn); msg["type"] != TypeError || msg["code"] != tc.wantCode {
				t.Errorf("respuesta = %v, want error %s", msg, tc.wantCode)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Errorf("cierre = %v, want StatusPolicyViolation", err)
			}
			if hub.Connections() != 0 {
				t.Errorf("Connections() = %d, want 0", hub.Connections())
			}
		})
	}
}

func TestServer_AuthTimeout(t *testing.T) {
	old := authTimeout
	authTimeout = 200 * time.Millisecond
	t.Cleanup(func() { authTimeout = old })

	_, _, url := startServer(t, nil)
	conn := dial(t, url)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Errorf("cierre = %v, want StatusPolicyViolation por no autenticarse a tiempo", err)
	}
}

func TestServer_RejectsOtherOrigins(t *testing.T) {
	_, _, url := startServer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": {"https://evil.example"}}})
	if err == nil {
		t.Fatal("Dial() error = nil, want rechazado por el origen")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("respuesta = %v, want 403", resp)
	}
}

func TestServer_EveryConnectionOfTheTerminalReceives(t *testing.T) {
	terminalID := domain.NewTerminalID()
	hub, _, url := startServer(t, nil)
	auth := map[string]string{"type": TypeAuth, "terminal_id": terminalID.String()}
	a, b := connected(t, url, auth), connected(t, url, auth)
	other := connected(t, url, map[string]string{"type": TypeAuth, "terminal_id": domain.NewTerminalID().String()})

	if err := hub.SendReceipt(context.Background(), terminalID, service.Receipt{TransactionID: "x", Result: "APPROVED", AmountCents: 100}); err != nil {
		t.Fatalf("SendReceipt() error = %v", err)
	}
	for name, conn := range map[string]*websocket.Conn{"a": a, "b": b} {
		if msg := receive(t, conn); msg["type"] != TypeReceipt || msg["result"] != "APPROVED" {
			t.Errorf("%s: mensaje = %v", name, msg)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, _, err := other.Read(ctx); err == nil {
		t.Error("otro terminal recibió el comprobante")
	}
}

func TestServer_DisconnectAndClose(t *testing.T) {
	terminalID := domain.NewTerminalID()
	hub, srv, url := startServer(t, nil)
	auth := map[string]string{"type": TypeAuth, "terminal_id": terminalID.String()}

	conn := connected(t, url, auth)
	_ = conn.Close(websocket.StatusNormalClosure, "")
	waitFor(t, func() bool { return hub.Connections() == 0 })
	if err := hub.NotifyResult(context.Background(), rejectedSession(t, terminalID, "51")); !errors.Is(err, service.ErrTerminalNotConnected) {
		t.Errorf("NotifyResult() error = %v, want ErrTerminalNotConnected", err)
	}

	conn = connected(t, url, auth)
	srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Errorf("cierre = %v, want StatusGoingAway al apagar", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("la condición no se cumplió a tiempo")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Un terminal que no lee sus mensajes llena su buffer: el hub lo desconecta en lugar de frenar.
func TestHub_DropsSlowClients(t *testing.T) {
	hub := NewHub()
	terminalID := domain.NewTerminalID()
	dropped := false
	c := &client{terminalID: terminalID, connectedAt: time.Now(), send: make(chan []byte, 1), drop: func() { dropped = true }}
	hub.register(c)

	for i := 0; i < 2; i++ {
		if err := hub.NotifySessionExpired(context.Background(), terminalID, domain.NewTransactionID()); err != nil {
			t.Fatalf("NotifySessionExpired() error = %v", err)
		}
	}
	if !dropped {
		t.Error("el cliente lento no se desconectó")
	}
}

func TestOriginPatterns(t *testing.T) {
	got := originPatterns([]string{"http://localhost:5173", "http://192.168.0.2:5173", "no es una url"})
	if len(got) != 2 || got[0] != "localhost:5173" || got[1] != "192.168.0.2:5173" {
		t.Errorf("originPatterns() = %v", got)
	}
	if got := originPatterns([]string{"http://a", "*"}); len(got) != 1 || got[0] != "*" {
		t.Errorf("originPatterns(*) = %v", got)
	}
}
