package http

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sha(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func tokensFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}

func namedTokens(t *testing.T) *TokenStore {
	t.Helper()
	store, err := NewTokenStore("", tokensFile(t, fmt.Sprintf(`[
		{"name":"lectora","role":"reader","sha256":%q},
		{"name":"soporte","role":"operator","sha256":%q},
		{"name":"jefa","role":"supervisor","sha256":%q}
	]`, sha("tok-r"), sha("tok-o"), sha("tok-s"))))
	if err != nil {
		t.Fatalf("NewTokenStore() error = %v", err)
	}
	return store
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := principalFrom(r.Context())
		_ = json.NewEncoder(w).Encode(map[string]string{"name": p.Name})
	})
}

func doAuth(h http.Handler, method, path, token, remote string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if remote != "" {
		req.RemoteAddr = remote
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestParseTokens_Invalid(t *testing.T) {
	good := sha("x")
	cases := map[string]string{
		"no es JSON":      `{`,
		"sin nombre":      fmt.Sprintf(`[{"role":"reader","sha256":%q}]`, good),
		"rol inválido":    fmt.Sprintf(`[{"name":"a","role":"root","sha256":%q}]`, good),
		"hash corto":      `[{"name":"a","role":"reader","sha256":"abc"}]`,
		"token en claro":  `[{"name":"a","role":"reader","token":"secreto","sha256":"` + good + `"}]`,
		"nombre repetido": fmt.Sprintf(`[{"name":"a","role":"reader","sha256":%q},{"name":"a","role":"reader","sha256":%q}]`, good, sha("y")),
		"hash repetido":   fmt.Sprintf(`[{"name":"a","role":"reader","sha256":%q},{"name":"b","role":"reader","sha256":%q}]`, good, good),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := NewTokenStore("", tokensFile(t, content)); err == nil {
				t.Fatal("NewTokenStore() error = nil, want an error")
			}
		})
	}
}

func TestNewTokenStore_MissingFileIsAnError(t *testing.T) {
	if _, err := NewTokenStore("", filepath.Join(t.TempDir(), "no-existe.json")); err == nil {
		t.Fatal("NewTokenStore() error = nil, want an error")
	}
}

func TestAuthenticate_Expiry(t *testing.T) {
	expires := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	store, err := NewTokenStore("", tokensFile(t, fmt.Sprintf(`[{"name":"a","role":"reader","sha256":%q,"expires_at":%q}]`, sha("tok"), expires.Format(time.RFC3339))))
	if err != nil {
		t.Fatalf("NewTokenStore() error = %v", err)
	}
	store.now = func() time.Time { return expires.Add(-time.Second) }
	if _, ok := store.Authenticate("tok"); !ok {
		t.Error("token rejected before it expires")
	}
	store.now = func() time.Time { return expires }
	if _, ok := store.Authenticate("tok"); ok {
		t.Error("token accepted once expired")
	}
}

func TestReload_RotatesAndKeepsTokensOnBadFile(t *testing.T) {
	path := tokensFile(t, fmt.Sprintf(`[{"name":"a","role":"reader","sha256":%q}]`, sha("old")))
	store, err := NewTokenStore("", path)
	if err != nil {
		t.Fatalf("NewTokenStore() error = %v", err)
	}
	if changed, err := store.Reload(); changed || err != nil {
		t.Errorf("Reload() without changes = %v, %v", changed, err)
	}

	// Durante la rotación valen los dos.
	must := func(content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	must(fmt.Sprintf(`[{"name":"a","role":"reader","sha256":%q},{"name":"a2","role":"reader","sha256":%q}]`, sha("old"), sha("new")))
	if changed, err := store.Reload(); !changed || err != nil {
		t.Fatalf("Reload() = %v, %v", changed, err)
	}
	for _, token := range []string{"old", "new"} {
		if _, ok := store.Authenticate(token); !ok {
			t.Errorf("%s should work during the rotation", token)
		}
	}

	// Baja del token viejo.
	must(fmt.Sprintf(`[{"name":"a2","role":"reader","sha256":%q}]`, sha("new")))
	if _, err := store.Reload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Authenticate("old"); ok {
		t.Error("revoked token still works")
	}

	// Un archivo roto no deja afuera a todos.
	must(`[{"name":`)
	if _, err := store.Reload(); err == nil {
		t.Error("Reload() error = nil, want an error")
	}
	if _, ok := store.Authenticate("new"); !ok {
		t.Error("a broken file should keep the current tokens")
	}
}

func TestRoles(t *testing.T) {
	h := authMiddleware(Security{Tokens: namedTokens(t)}, okHandler())
	id := "/batches/11111111-1111-4111-8111-111111111111"
	const ok, no = http.StatusOK, http.StatusForbidden
	cases := []struct {
		method, path           string
		reader, op, supervisor int
	}{
		{http.MethodGet, id, ok, ok, ok},
		{http.MethodGet, "/merchants/x/batches", ok, ok, ok},
		{http.MethodGet, "/reversal-reviews", ok, ok, ok},
		{http.MethodPost, id + "/force-close", no, ok, ok},
		{http.MethodPost, id + "/resubmit", no, ok, ok},
		{http.MethodPost, id + "/reopen", no, ok, ok},
		{http.MethodPost, id + "/resolve-dispute", no, no, ok},
		{http.MethodPost, id + "/settle", no, no, ok},
		{http.MethodPost, "/reversal-reviews/x/resolve", no, no, ok},
		{http.MethodPost, "/reversal-reviews/x/force-close", no, no, ok},
		{http.MethodPut, "/merchants/x/timezone", no, no, ok},
		{http.MethodPut, id + "/force-close", no, no, ok},
		{http.MethodDelete, id, no, no, ok},
		{http.MethodPost, "/ruta-futura", no, no, ok},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			for token, want := range map[string]int{"tok-r": tc.reader, "tok-o": tc.op, "tok-s": tc.supervisor} {
				if got := doAuth(h, tc.method, tc.path, token, "").Code; got != want {
					t.Errorf("%s = %d, want %d", token, got, want)
				}
			}
		})
	}
}

func TestPublicPathsAndDefaultDeny(t *testing.T) {
	h := authMiddleware(Security{Tokens: namedTokens(t)}, okHandler())
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		if got := doAuth(h, http.MethodGet, path, "", "").Code; got != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, got)
		}
	}
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/batches/x"},
		{http.MethodGet, "/merchants/x/batches"},
		{http.MethodGet, "/reversal-reviews"},
		{http.MethodGet, "/ruta-futura"},
		{http.MethodGet, "/metrics/"},
		{http.MethodPost, "/metrics"},
		{http.MethodPost, "/healthz"},
	} {
		if got := doAuth(h, r.method, r.path, "", "").Code; got != http.StatusUnauthorized {
			t.Errorf("%s %s without token = %d, want 401", r.method, r.path, got)
		}
	}
}

func TestNoTokensDisablesTheAPIButNotHealth(t *testing.T) {
	store, err := NewTokenStore("", "")
	if err != nil {
		t.Fatal(err)
	}
	h := authMiddleware(Security{Tokens: store}, okHandler())
	if got := doAuth(h, http.MethodGet, "/batches/x", "algo", "").Code; got != http.StatusServiceUnavailable {
		t.Errorf("read = %d, want 503", got)
	}
	if got := doAuth(h, http.MethodGet, "/healthz", "", "").Code; got != http.StatusOK {
		t.Errorf("healthz = %d, want 200", got)
	}
}

func TestLegacyKeyIsNotVerified(t *testing.T) {
	store, err := NewTokenStore("clave", "")
	if err != nil {
		t.Fatal(err)
	}
	p, ok := store.Authenticate("clave")
	if !ok || p.Verified || p.Role != RoleSupervisor {
		t.Errorf("principal = %+v, %v", p, ok)
	}
}

func TestResolveOperator(t *testing.T) {
	store := namedTokens(t)
	var gotCode int
	var gotName string
	h := authMiddleware(Security{Tokens: store}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, ok := operator(w, r, r.URL.Query().Get("declared"))
		if ok {
			gotName = name
			w.WriteHeader(http.StatusOK)
		}
	}))
	for _, tc := range []struct {
		declared string
		want     int
		name     string
	}{
		{"", http.StatusOK, "soporte"},
		{"soporte", http.StatusOK, "soporte"},
		{"otra-persona", http.StatusForbidden, ""},
	} {
		gotName = ""
		gotCode = doAuth(h, http.MethodPost, "/batches/x/reopen?declared="+tc.declared, "tok-o", "").Code
		if gotCode != tc.want || gotName != tc.name {
			t.Errorf("declared %q = %d, %q; want %d, %q", tc.declared, gotCode, gotName, tc.want, tc.name)
		}
	}

	legacy, err := NewTokenStore("clave", "")
	if err != nil {
		t.Fatal(err)
	}
	h = authMiddleware(Security{Tokens: legacy}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name, _ := operator(w, r, "declarado")
		gotName = name
	}))
	doAuth(h, http.MethodPost, "/batches/x/settle", "clave", "")
	if gotName != "declarado" {
		t.Errorf("with the legacy key, operator = %q, want the declared one", gotName)
	}
}

func TestLimiter(t *testing.T) {
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	limiter := newAuthLimiter()
	limiter.now = func() time.Time { return clock }
	h := authMiddlewareWith(Security{Tokens: namedTokens(t)}, limiter, okHandler())
	const attacker, other = "203.0.113.9:1234", "203.0.113.10:1234"

	for i := range authMaxFailures {
		if got := doAuth(h, http.MethodGet, "/batches/x", "malo", attacker).Code; got != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401", i, got)
		}
	}
	rec := doAuth(h, http.MethodGet, "/batches/x", "tok-r", attacker)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("blocked client = %d, Retry-After %q; want 429 even with a valid token", rec.Code, rec.Header().Get("Retry-After"))
	}
	if got := doAuth(h, http.MethodGet, "/batches/x", "tok-r", other).Code; got != http.StatusOK {
		t.Errorf("another client = %d, want 200", got)
	}
	if got := doAuth(h, http.MethodGet, "/healthz", "", attacker).Code; got != http.StatusOK {
		t.Errorf("health while blocked = %d, want 200", got)
	}

	clock = clock.Add(authBlock + time.Second)
	if got := doAuth(h, http.MethodGet, "/batches/x", "tok-r", attacker).Code; got != http.StatusOK {
		t.Errorf("after the block = %d, want 200", got)
	}
}

func TestLimiter_WindowAndSuccessReset(t *testing.T) {
	clock := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	limiter := newAuthLimiter()
	limiter.now = func() time.Time { return clock }
	h := authMiddlewareWith(Security{Tokens: namedTokens(t)}, limiter, okHandler())
	const client = "203.0.113.9:1234"
	fail := func(n int) {
		for range n {
			doAuth(h, http.MethodGet, "/batches/x", "malo", client)
		}
	}

	// Fallos espaciados: la ventana se reinicia.
	fail(authMaxFailures - 1)
	clock = clock.Add(authWindow + time.Second)
	fail(authMaxFailures - 1)
	if got := doAuth(h, http.MethodGet, "/batches/x", "tok-r", client).Code; got != http.StatusOK {
		t.Fatalf("after window = %d, want 200", got)
	}

	// Un acceso correcto borra los fallos.
	fail(authMaxFailures - 1)
	doAuth(h, http.MethodGet, "/batches/x", "tok-r", client)
	fail(authMaxFailures - 1)
	if got := doAuth(h, http.MethodGet, "/batches/x", "tok-r", client).Code; got != http.StatusOK {
		t.Errorf("after success = %d, want 200", got)
	}

	// Pedir sin token no es adivinar: no bloquea.
	for range authMaxFailures * 2 {
		doAuth(h, http.MethodGet, "/batches/x", "", client)
	}
	if _, blocked := limiter.blocked(netip.MustParseAddr("203.0.113.9")); blocked {
		t.Error("requests without a token should not block the client")
	}
}

func TestLimiter_MemoryIsBounded(t *testing.T) {
	limiter := newAuthLimiter()
	for i := range authMaxClients + 500 {
		limiter.fail(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}))
	}
	if len(limiter.clients) > authMaxClients {
		t.Errorf("clients = %d, want at most %d", len(limiter.clients), authMaxClients)
	}
}

func TestClientKey_GroupsIPv6(t *testing.T) {
	a := clientKey(netip.MustParseAddr("2001:db8:1:2::1"))
	b := clientKey(netip.MustParseAddr("2001:db8:1:2::ffff"))
	c := clientKey(netip.MustParseAddr("2001:db8:1:3::1"))
	if a != b || a == c {
		t.Errorf("keys = %v, %v, %v", a, b, c)
	}
}

func TestClientIP(t *testing.T) {
	proxies := Security{TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}}
	cases := []struct {
		name   string
		sec    Security
		remote string
		xff    string
		want   string
	}{
		{"sin proxy se ignora el encabezado", Security{}, "203.0.113.9:1", "198.51.100.1", "203.0.113.9"},
		{"cliente directo que falsifica", proxies, "203.0.113.9:1", "198.51.100.1", "203.0.113.9"},
		{"por un proxy de confianza", proxies, "10.0.0.1:1", "198.51.100.1", "198.51.100.1"},
		{"el cliente antepone una IP falsa", proxies, "10.0.0.1:1", "1.2.3.4, 198.51.100.1", "198.51.100.1"},
		{"varios proxies", proxies, "10.0.0.1:1", "198.51.100.1, 10.0.0.2", "198.51.100.1"},
		{"proxy sin encabezado", proxies, "10.0.0.1:1", "", "10.0.0.1"},
		{"encabezado inválido", proxies, "10.0.0.1:1", "basura", "10.0.0.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remote
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := tc.sec.clientIP(req).String(); got != tc.want {
				t.Errorf("clientIP = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestOperatorMismatch_HTTP(t *testing.T) {
	b := newBatch(t)
	store := namedTokens(t)
	mux := newTestMux(newTestHandler(&fakeBatchRepo{findByIDResult: b}, &fakePool{}))
	h := authMiddleware(Security{Tokens: store}, mux)

	req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/force-close", strings.NewReader(`{"operator_id":"otra"}`))
	req.Header.Set("Authorization", "Bearer tok-o")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "OPERATOR_MISMATCH") {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}

	// Con un token nombrado, el body puede no traer operator_id.
	req = httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/force-close", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer tok-o")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("got %d %s, want 200", rec.Code, rec.Body)
	}
}
