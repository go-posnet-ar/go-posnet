package middleware

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func requestFrom(ip string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = ip + ":40000"
	return r
}

func TestRateLimiter(t *testing.T) {
	l := NewRateLimiter(3, ClientIP{})
	now := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	h := l.Limit(okHandler())
	do := func(ip string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, requestFrom(ip))
		return rec
	}

	for i := 0; i < 3; i++ {
		if rec := do("10.0.0.1"); rec.Code != http.StatusOK {
			t.Fatalf("pedido %d: status = %d, want 200", i, rec.Code)
		}
	}
	rec := do("10.0.0.1")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After = %q, want 429", rec.Code, rec.Header().Get("Retry-After"))
	}
	if rec := do("10.0.0.2"); rec.Code != http.StatusOK {
		t.Errorf("otra IP: status = %d, want 200", rec.Code)
	}
	now = now.Add(20 * time.Second) // 3 por minuto: una ficha cada 20 s
	if rec := do("10.0.0.1"); rec.Code != http.StatusOK {
		t.Errorf("después de recargar: status = %d, want 200", rec.Code)
	}
	if rec := do("10.0.0.1"); rec.Code != http.StatusTooManyRequests {
		t.Errorf("sin fichas: status = %d, want 429", rec.Code)
	}
}

func TestRateLimiter_IPv6SharesThe64(t *testing.T) {
	l := NewRateLimiter(1, ClientIP{})
	h := l.Limit(okHandler())
	for i, ip := range []string{"[2001:db8::1]", "[2001:db8::2]"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, requestFrom(ip))
		want := http.StatusOK
		if i == 1 {
			want = http.StatusTooManyRequests
		}
		if rec.Code != want {
			t.Errorf("%s: status = %d, want %d", ip, rec.Code, want)
		}
	}
}

func TestClientIP(t *testing.T) {
	proxy := netip.MustParsePrefix("10.1.0.0/16")
	cases := []struct {
		name    string
		proxies []netip.Prefix
		remote  string
		xff     string
		want    string
	}{
		{"sin proxy se ignora X-Forwarded-For", nil, "203.0.113.7", "198.51.100.1", "203.0.113.7"},
		{"desde un proxy de confianza", []netip.Prefix{proxy}, "10.1.0.5", "198.51.100.1", "198.51.100.1"},
		{"el cliente no puede agregar hops falsos", []netip.Prefix{proxy}, "10.1.0.5", "1.2.3.4, 198.51.100.1", "198.51.100.1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := requestFrom(tc.remote)
			r.Header.Set("X-Forwarded-For", tc.xff)
			if got := (ClientIP{TrustedProxies: tc.proxies}).Of(r).String(); got != tc.want {
				t.Errorf("Of() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCORS(t *testing.T) {
	const front = "http://192.168.0.2:5173"
	var reached bool
	h := CORS([]string{front}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))

	t.Run("origen permitido", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Origin", front)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Header().Get("Access-Control-Allow-Origin") != front ||
			!strings.Contains(rec.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
			t.Errorf("headers = %v", rec.Header())
		}
	})
	t.Run("otro origen", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Origin", "https://evil.example")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("Access-Control-Allow-Origin = %q, want vacío", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	})
	t.Run("el preflight no llega al servicio", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest(http.MethodOptions, "/api/sessions/x/pay", nil)
		r.Header.Set("Origin", front)
		r.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		if rec.Code != http.StatusNoContent || reached || rec.Header().Get("Access-Control-Allow-Origin") != front {
			t.Errorf("status = %d, reached = %v, headers = %v", rec.Code, reached, rec.Header())
		}
	})
}

func TestMaxBody(t *testing.T) {
	h := MaxBody(4, okHandler())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("12345")))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("1234")))
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
