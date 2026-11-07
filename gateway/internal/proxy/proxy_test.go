package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/juantevez/go-posnet/gateway/internal/middleware"
)

// fakeService registra lo que recibe y responde con un CORS propio, que el gateway debe quitar.
type fakeService struct {
	mu   sync.Mutex
	reqs []*http.Request
	srv  *httptest.Server
}

func newFakeService(t *testing.T) *fakeService {
	t.Helper()
	f := &fakeService{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs = append(f.reqs, r.Clone(r.Context()))
		f.mu.Unlock()
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"path": r.URL.Path})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeService) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func (f *fakeService) last() *http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[len(f.reqs)-1]
}

const validID = "7f6c0c5e-3f55-4a53-9d5b-8a1e6a4b2c10"

func newTestRouter(t *testing.T, tg, st *fakeService, clientIP middleware.ClientIP) http.Handler {
	t.Helper()
	transport, err := NewTransport("")
	if err != nil {
		t.Fatalf("NewTransport() error = %v", err)
	}
	routes := Routes{
		PayLimit:    middleware.NewRateLimiter(2, clientIP),
		StatusLimit: middleware.NewRateLimiter(100, clientIP),
	}
	if routes.TerminalGateway, err = NewUpstream("terminal-gateway", tg.srv.URL, transport, clientIP); err != nil {
		t.Fatalf("NewUpstream() error = %v", err)
	}
	if st != nil {
		if routes.Settlement, err = NewUpstream("settlement", st.srv.URL, transport, clientIP); err != nil {
			t.Fatalf("NewUpstream() error = %v", err)
		}
	}
	return NewRouter(routes)
}

func serve(h http.Handler, method, path, remote string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(`{}`))
	r.RemoteAddr = remote + ":40000"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestRouter_PublishedRoutesKeepTheirPath(t *testing.T) {
	tg, st := newFakeService(t), newFakeService(t)
	h := newTestRouter(t, tg, st, middleware.ClientIP{})

	cases := []struct {
		method, path string
		to           *fakeService
	}{
		{http.MethodPost, "/api/sessions/create", tg},
		{http.MethodPost, "/api/sessions/" + validID + "/pay", tg},
		{http.MethodGet, "/api/sessions/" + validID + "/status", tg},
		{http.MethodGet, "/sessions/" + validID, tg},
		{http.MethodPost, "/sessions/" + validID + "/cancel", tg},
		{http.MethodPost, "/sessions/" + validID + "/reversal", tg},
		{http.MethodPost, "/batch-close", tg},
		{http.MethodGet, "/batches/" + validID, st},
		{http.MethodPost, "/batches/" + validID + "/settle", st},
		{http.MethodGet, "/merchants/" + validID + "/batches", st},
		{http.MethodPut, "/merchants/" + validID + "/timezone", st},
		{http.MethodGet, "/reversal-reviews", st},
		{http.MethodPost, "/reversal-reviews/" + validID + "/resolve", st},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			before := tc.to.count()
			rec := serve(h, tc.method, tc.path, "203.0.113.7", map[string]string{"Authorization": "Bearer t"})
			if rec.Code != http.StatusOK || tc.to.count() != before+1 {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
			}
			got := tc.to.last()
			if got.URL.Path != tc.path || got.Method != tc.method {
				t.Errorf("el servicio recibió %s %s", got.Method, got.URL.Path)
			}
			if got.Header.Get("Authorization") != "Bearer t" {
				t.Errorf("Authorization = %q: el token tiene que llegar al servicio", got.Header.Get("Authorization"))
			}
			if rec.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Error("el CORS del servicio llegó al cliente")
			}
		})
	}
}

func TestRouter_UnpublishedRoutesAreNotFound(t *testing.T) {
	tg := newFakeService(t)
	h := newTestRouter(t, tg, nil, middleware.ClientIP{})
	for _, path := range []string{"/metrics", "/readyz", "/transactions/" + validID, "/rules", "/notifications/dead", "/batches/" + validID} {
		if rec := serve(h, http.MethodGet, path, "203.0.113.7", nil); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, rec.Code)
		}
	}
	if rec := serve(h, http.MethodDelete, "/batch-close", "203.0.113.7", nil); rec.Code == http.StatusOK {
		t.Errorf("DELETE /batch-close: status = %d, want rechazado", rec.Code)
	}
	if tg.count() != 0 {
		t.Errorf("el servicio recibió %d pedidos, want 0", tg.count())
	}
}

func TestRouter_InvalidIDNeverReachesTheService(t *testing.T) {
	tg := newFakeService(t)
	h := newTestRouter(t, tg, nil, middleware.ClientIP{})
	rec := serve(h, http.MethodPost, "/api/sessions/not-a-uuid/pay", "203.0.113.7", nil)
	if rec.Code != http.StatusBadRequest || tg.count() != 0 {
		t.Errorf("status = %d, pedidos = %d, want 400 sin llegar al servicio", rec.Code, tg.count())
	}
}

func TestRouter_PayIsRateLimited(t *testing.T) {
	tg := newFakeService(t)
	h := newTestRouter(t, tg, nil, middleware.ClientIP{})
	path := "/api/sessions/" + validID + "/pay"
	for i := 0; i < 2; i++ {
		serve(h, http.MethodPost, path, "203.0.113.7", nil)
	}
	if rec := serve(h, http.MethodPost, path, "203.0.113.7", nil); rec.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", rec.Code)
	}
	if tg.count() != 2 {
		t.Errorf("pedidos = %d, want 2", tg.count())
	}
}

func TestUpstream_ForwardsTheClientIP(t *testing.T) {
	t.Run("sin proxy de confianza, se descarta el X-Forwarded-For del cliente", func(t *testing.T) {
		tg := newFakeService(t)
		h := newTestRouter(t, tg, nil, middleware.ClientIP{})
		serve(h, http.MethodPost, "/api/sessions/create", "203.0.113.7", map[string]string{"X-Forwarded-For": "1.2.3.4"})
		if got := tg.last().Header.Get("X-Forwarded-For"); got != "203.0.113.7" {
			t.Errorf("X-Forwarded-For = %q, want solo la IP de la conexión", got)
		}
	})
	t.Run("detrás de un proxy de confianza, se conserva la cadena", func(t *testing.T) {
		tg := newFakeService(t)
		lb := middleware.ClientIP{TrustedProxies: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}}
		h := newTestRouter(t, tg, nil, lb)
		serve(h, http.MethodPost, "/api/sessions/create", "10.0.0.5", map[string]string{"X-Forwarded-For": "198.51.100.1"})
		if got := tg.last().Header.Get("X-Forwarded-For"); got != "198.51.100.1, 10.0.0.5" {
			t.Errorf("X-Forwarded-For = %q", got)
		}
	})
}

func TestUpstream_Unavailable(t *testing.T) {
	down := newFakeService(t)
	down.srv.Close()
	h := newTestRouter(t, down, nil, middleware.ClientIP{})
	rec := serve(h, http.MethodPost, "/api/sessions/create", "203.0.113.7", nil)
	body, _ := io.ReadAll(rec.Body)
	if rec.Code != http.StatusBadGateway || !strings.Contains(string(body), "UPSTREAM_UNAVAILABLE") {
		t.Errorf("status = %d, body = %s, want 502", rec.Code, body)
	}
	if strings.Contains(string(body), "127.0.0.1") {
		t.Errorf("body = %s: no debe exponer la dirección interna", body)
	}
}

func TestAdminRouter(t *testing.T) {
	h := NewAdminRouter(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "metrics") }))
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		if rec := serve(h, http.MethodGet, path, "127.0.0.1", nil); rec.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rec.Code)
		}
	}
}
