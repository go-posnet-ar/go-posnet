package http

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

const testToken = "4d8f0c1e9b7a6352f1e0d9c8b7a69584736251403f2e1d0c9b8a796857463521"

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// writeTokensFile escribe un archivo de tokens y devuelve su path.
func writeTokensFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "terminal-tokens.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

// tokensFor carga un archivo con testToken para terminalID.
func tokensFor(t *testing.T, terminalID domain.TerminalID) *TerminalTokens {
	t.Helper()
	tokens, err := LoadTerminalTokens(writeTokensFile(t,
		`[{"terminal_id":"`+terminalID.String()+`","sha256":"`+sha256Hex(testToken)+`"}]`))
	if err != nil {
		t.Fatalf("LoadTerminalTokens() error = %v", err)
	}
	return tokens
}

func authRequest(method, path, body, token string) *http.Request {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

// ─── Archivo de tokens ────────────────────────────────────────────────────────

func TestLoadTerminalTokens(t *testing.T) {
	id := domain.NewTerminalID().String()
	hash := sha256Hex(testToken)
	cases := []struct {
		name    string
		content string
		wantErr bool
	}{
		{"válido", `[{"terminal_id":"` + id + `","sha256":"` + hash + `","expires_at":"2030-01-01T00:00:00Z"}]`, false},
		{"vacío deja todo cerrado", `[]`, false},
		{"terminal_id inválido", `[{"terminal_id":"TRM-0001","sha256":"` + hash + `"}]`, true},
		{"hash corto", `[{"terminal_id":"` + id + `","sha256":"abc"}]`, true},
		{"hash repetido", `[{"terminal_id":"` + id + `","sha256":"` + hash + `"},{"terminal_id":"` + id + `","sha256":"` + hash + `"}]`, true},
		{"campo desconocido, como el token en claro", `[{"terminal_id":"` + id + `","sha256":"` + hash + `","token":"x"}]`, true},
		{"JSON inválido", `{`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tokens, err := LoadTerminalTokens(writeTokensFile(t, tc.content))
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadTerminalTokens() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && !tokens.Configured() {
				t.Error("Configured() = false: con archivo, la API exige token")
			}
		})
	}

	t.Run("sin archivo no hay tokens", func(t *testing.T) {
		tokens, err := LoadTerminalTokens("")
		if err != nil || tokens.Configured() {
			t.Errorf("LoadTerminalTokens(\"\") = %v, %v, want sin tokens", tokens, err)
		}
	})
	t.Run("archivo inexistente", func(t *testing.T) {
		if _, err := LoadTerminalTokens(filepath.Join(t.TempDir(), "no-existe.json")); err == nil {
			t.Error("LoadTerminalTokens() error = nil, want error")
		}
	})
}

func TestTerminalTokens_Authenticate(t *testing.T) {
	id := domain.NewTerminalID()
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	tokens, err := LoadTerminalTokens(writeTokensFile(t, `[
		{"terminal_id":"`+id.String()+`","sha256":"`+sha256Hex(testToken)+`"},
		{"terminal_id":"`+id.String()+`","sha256":"`+sha256Hex("vencido")+`","expires_at":"2026-11-01T00:00:00Z"}
	]`))
	if err != nil {
		t.Fatalf("LoadTerminalTokens() error = %v", err)
	}
	tokens.now = func() time.Time { return now }

	if got, ok := tokens.Authenticate(testToken); !ok || got != id {
		t.Errorf("Authenticate(válido) = %v, %v, want %v", got, ok, id)
	}
	if _, ok := tokens.Authenticate("vencido"); ok {
		t.Error("Authenticate(vencido) = ok, want rechazado")
	}
	if _, ok := tokens.Authenticate("otro"); ok {
		t.Error("Authenticate(otro) = ok, want rechazado")
	}
}

// ─── Endpoints protegidos ─────────────────────────────────────────────────────

func TestRouter_TerminalEndpointsRequireToken(t *testing.T) {
	terminal := domain.NewTerminalID()
	router := NewRouter(&fakeSessionService{createSessionResult: mustSessionCreatedResult(t)},
		query.NewSessionQueryHandler(&fakeSessionRepo{}), &fakePool{}, nil, tokensFor(t, terminal))
	id := domain.NewTransactionID().String()

	protected := []struct{ method, path, body string }{
		{http.MethodPost, "/api/sessions/create", `{"amount_cents":1000}`},
		{http.MethodGet, "/sessions/" + id, ""},
		{http.MethodPost, "/sessions/" + id + "/cancel", ""},
		{http.MethodPost, "/sessions/" + id + "/reversal", ""},
		{http.MethodPost, "/batch-close", `{"currency":"ARS"}`},
	}
	for _, p := range protected {
		for _, token := range []string{"", "incorrecto"} {
			t.Run(p.method+" "+p.path+" token="+token, func(t *testing.T) {
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, authRequest(p.method, p.path, p.body, token))
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401, body = %s", rec.Code, rec.Body)
				}
				if !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
					t.Errorf("WWW-Authenticate = %q", rec.Header().Get("WWW-Authenticate"))
				}
			})
		}
	}

	// Los usa el teléfono del cliente, sin el token del terminal.
	public := []struct{ method, path, body string }{
		{http.MethodGet, "/healthz", ""},
		{http.MethodPost, "/api/sessions/" + id + "/pay", `{"card_last4":"4242"}`},
		{http.MethodGet, "/api/sessions/" + id + "/status", ""},
	}
	for _, p := range public {
		t.Run("público "+p.method+" "+p.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, authRequest(p.method, p.path, p.body, ""))
			if rec.Code == http.StatusUnauthorized {
				t.Errorf("status = 401, want público")
			}
		})
	}
}

func TestRouter_TerminalComesFromToken(t *testing.T) {
	terminal := domain.NewTerminalID()
	other := domain.NewTerminalID().String()
	id := domain.NewTransactionID().String()

	newRouter := func(svc *fakeSessionService) http.Handler {
		return NewRouter(svc, query.NewSessionQueryHandler(&fakeSessionRepo{}), &fakePool{}, nil, tokensFor(t, terminal))
	}

	t.Run("create usa el terminal del token, sin comercio por defecto", func(t *testing.T) {
		svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
		rec := httptest.NewRecorder()
		newRouter(svc).ServeHTTP(rec, authRequest(http.MethodPost, "/api/sessions/create", `{"amount_cents":1000}`, testToken))
		if rec.Code != http.StatusCreated || len(svc.createSessionCalls) != 1 {
			t.Fatalf("status = %d, calls = %d, body = %s", rec.Code, len(svc.createSessionCalls), rec.Body)
		}
		if c := svc.createSessionCalls[0]; c.TerminalID != terminal.String() || c.MerchantID != "" {
			t.Errorf("cmd = %+v, want el terminal del token y el comercio vacío", c)
		}
	})

	t.Run("create para otro terminal", func(t *testing.T) {
		svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
		rec := httptest.NewRecorder()
		newRouter(svc).ServeHTTP(rec, authRequest(http.MethodPost, "/api/sessions/create", `{"amount_cents":1000,"terminal_id":"`+other+`"}`, testToken))
		if rec.Code != http.StatusForbidden || len(svc.createSessionCalls) != 0 {
			t.Errorf("status = %d, calls = %d, want 403 sin crear", rec.Code, len(svc.createSessionCalls))
		}
	})

	t.Run("cancel y reversal ignoran X-Terminal-ID", func(t *testing.T) {
		svc := &fakeSessionService{}
		router := newRouter(svc)
		for _, path := range []string{"/sessions/" + id + "/cancel", "/sessions/" + id + "/reversal"} {
			req := authRequest(http.MethodPost, path, "", testToken)
			req.Header.Set("X-Terminal-ID", other)
			router.ServeHTTP(httptest.NewRecorder(), req)
		}
		if len(svc.cancelSessionCalls) != 1 || svc.cancelSessionCalls[0].TerminalID != terminal.String() {
			t.Errorf("cancel = %+v, want el terminal del token", svc.cancelSessionCalls)
		}
		if len(svc.requestReversalCalls) != 1 || svc.requestReversalCalls[0].TerminalID != terminal.String() {
			t.Errorf("reversal = %+v, want el terminal del token", svc.requestReversalCalls)
		}
	})

	t.Run("batch-close", func(t *testing.T) {
		svc := &fakeSessionService{}
		router := newRouter(svc)

		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, authRequest(http.MethodPost, "/batch-close", `{"currency":"ARS","terminal_id":"`+other+`"}`, testToken))
		if rec.Code != http.StatusForbidden {
			t.Errorf("otro terminal: status = %d, want 403", rec.Code)
		}

		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, authRequest(http.MethodPost, "/batch-close", `{"currency":"ARS"}`, testToken))
		if rec.Code != http.StatusAccepted || len(svc.requestBatchCloseCalls) != 1 || svc.requestBatchCloseCalls[0].TerminalID != terminal.String() {
			t.Errorf("status = %d, calls = %+v, want el terminal del token", rec.Code, svc.requestBatchCloseCalls)
		}
	})
}

func TestRouter_SessionOfAnotherTerminalIsNotFound(t *testing.T) {
	session := newSession(t)
	repo := &fakeSessionRepo{findResult: session}

	ownRouter := NewRouter(&fakeSessionService{}, query.NewSessionQueryHandler(repo), &fakePool{}, nil, tokensFor(t, session.TerminalID()))
	rec := httptest.NewRecorder()
	ownRouter.ServeHTTP(rec, authRequest(http.MethodGet, "/sessions/"+session.ID().String(), "", testToken))
	if rec.Code != http.StatusOK {
		t.Errorf("propia: status = %d, want 200", rec.Code)
	}

	otherRouter := NewRouter(&fakeSessionService{}, query.NewSessionQueryHandler(repo), &fakePool{}, nil, tokensFor(t, domain.NewTerminalID()))
	rec = httptest.NewRecorder()
	otherRouter.ServeHTTP(rec, authRequest(http.MethodGet, "/sessions/"+session.ID().String(), "", testToken))
	if rec.Code != http.StatusNotFound {
		t.Errorf("ajena: status = %d, want 404", rec.Code)
	}
}

// Sin tokens, solo en desarrollo, todo sigue como antes: X-Terminal-ID y el terminal de prueba.
func TestRouter_WithoutTokensKeepsLegacyBehavior(t *testing.T) {
	svc := &fakeSessionService{createSessionResult: mustSessionCreatedResult(t)}
	router := newRouterFor(svc, &fakeSessionRepo{}, &fakePool{})

	router.ServeHTTP(httptest.NewRecorder(), authRequest(http.MethodPost, "/api/sessions/create", `{"amount_cents":1000}`, ""))
	req := authRequest(http.MethodPost, "/sessions/"+domain.NewTransactionID().String()+"/cancel", "", "")
	req.Header.Set("X-Terminal-ID", "term-1")
	router.ServeHTTP(httptest.NewRecorder(), req)

	if len(svc.createSessionCalls) != 1 || svc.createSessionCalls[0].TerminalID != "a1b2c3d4-e5f6-7890-abcd-ef1234567890" ||
		svc.createSessionCalls[0].MerchantID != "b2c3d4e5-f6a7-8901-bcde-f12345678901" {
		t.Errorf("create = %+v, want el terminal de prueba", svc.createSessionCalls)
	}
	if len(svc.cancelSessionCalls) != 1 || svc.cancelSessionCalls[0].TerminalID != "term-1" {
		t.Errorf("cancel = %+v, want X-Terminal-ID", svc.cancelSessionCalls)
	}
}

// Un error de validación del caso de uso es del cliente: 400 con su motivo, no 500.
func TestRouter_ValidationErrorsAreBadRequests(t *testing.T) {
	svc := &fakeSessionService{
		createSessionErr:     pkgerrors.NewValidationError("merchant_id does not belong to the terminal"),
		requestBatchCloseErr: pkgerrors.NewValidationError("merchant_id does not belong to the terminal"),
	}
	router := newRouterFor(svc, &fakeSessionRepo{}, &fakePool{})

	for _, r := range []*http.Request{
		authRequest(http.MethodPost, "/api/sessions/create", `{"amount_cents":1000}`, ""),
		authRequest(http.MethodPost, "/batch-close", `{"currency":"ARS"}`, ""),
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, r)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "does not belong") {
			t.Errorf("%s: status = %d, body = %s, want 400 con el motivo", r.URL.Path, rec.Code, rec.Body)
		}
	}

	// Un error que no es de validación sigue siendo un 500 sin detalle.
	svc.createSessionErr = errors.New("pq: connection refused")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, authRequest(http.MethodPost, "/api/sessions/create", `{"amount_cents":1000}`, ""))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "pq:") {
		t.Errorf("status = %d, body = %s", rec.Code, rec.Body)
	}
}
