package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/observability"
)

func newRouterFor(svc *fakeSessionService, repo *fakeSessionRepo, pool *fakePool) http.Handler {
	return NewRouter(svc, query.NewSessionQueryHandler(repo), pool, nil, nil, nil)
}

// panickySessionRepo simula un bug en la capa de aplicación para probar que
// recoverMiddleware protege incluso detrás de toda la cadena de middlewares
// (cors → observability → recover → mux).
type panickySessionRepo struct{}

func (p *panickySessionRepo) Save(context.Context, *aggregate.PaymentSession) error { return nil }
func (p *panickySessionRepo) SaveTx(context.Context, pgx.Tx, *aggregate.PaymentSession) error {
	return nil
}

func (p *panickySessionRepo) FindByID(context.Context, domain.TransactionID) (*aggregate.PaymentSession, error) {
	panic("boom")
}

func (p *panickySessionRepo) FindActiveByTerminal(context.Context, domain.TerminalID) (*aggregate.PaymentSession, error) {
	return nil, nil
}

func (p *panickySessionRepo) DeleteExpired(context.Context) (int64, error) { return 0, nil }

func TestNewRouter_Routes(t *testing.T) {
	router := newRouterFor(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{})

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"healthz", http.MethodGet, "/healthz", "", http.StatusOK},
		{"readyz", http.MethodGet, "/readyz", "", http.StatusOK},
		{"metrics", http.MethodGet, "/metrics", "", http.StatusOK},
		{"get session", http.MethodGet, "/sessions/tx-1", "", http.StatusBadRequest}, // "tx-1" no es un UUID válido
		{"cancel session", http.MethodPost, "/sessions/tx-1/cancel", "", http.StatusOK},
		{"request reversal", http.MethodPost, "/sessions/tx-1/reversal", "", http.StatusAccepted},
		{"batch close", http.MethodPost, "/batch-close", "{}", http.StatusAccepted},
		{"qr create", http.MethodPost, "/api/sessions/create", `{"amount_cents":0}`, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestNewRouter_ReadyzReflectsPoolFailure(t *testing.T) {
	pool := &fakePool{pingErr: errors.New("connection refused")}
	router := newRouterFor(&fakeSessionService{}, &fakeSessionRepo{}, pool)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestNewRouter_RecoversFromPanicInHandler(t *testing.T) {
	router := NewRouter(&fakeSessionService{}, query.NewSessionQueryHandler(&panickySessionRepo{}), &fakePool{}, nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/sessions/00000000-0000-0000-0000-000000000000", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestNewRouter_UnknownRouteReturns404(t *testing.T) {
	router := newRouterFor(&fakeSessionService{}, &fakeSessionRepo{}, &fakePool{})

	req := httptest.NewRequest(http.MethodGet, "/does-not-exist", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestMetricsHandler(t *testing.T) {
	handler := observability.MetricsHandler()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Errorf("body should expose Go runtime metrics, got %q", rec.Body.String())
	}
}

// ─── corsMiddleware ───────────────────────────────────────────────────────────

func TestCorsMiddleware_Origins(t *testing.T) {
	const frontend = "http://192.168.0.2:5173"
	cases := []struct {
		name    string
		allowed []string
		origin  string
		want    string
	}{
		{"origen permitido", []string{frontend}, frontend, frontend},
		{"otro origen", []string{frontend}, "https://evil.example", ""},
		{"sin Origin, como curl o Postman", []string{frontend}, "", ""},
		{"comodín explícito", []string{"*"}, "https://cualquiera.example", "*"},
		{"sin orígenes configurados", nil, frontend, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var nextCalled bool
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nextCalled = true
				w.WriteHeader(http.StatusOK)
			})
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := httptest.NewRecorder()
			corsMiddleware(tc.allowed, next).ServeHTTP(rec, req)

			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != tc.want {
				t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, tc.want)
			}
			if tc.want != "" && rec.Header().Get("Access-Control-Allow-Methods") != "GET, POST, OPTIONS" {
				t.Errorf("Access-Control-Allow-Methods = %q", rec.Header().Get("Access-Control-Allow-Methods"))
			}
			if rec.Header().Get("Vary") != "Origin" {
				t.Errorf("Vary = %q, want Origin: la respuesta depende del origen", rec.Header().Get("Vary"))
			}
			// CORS lo aplica el navegador: el servidor responde igual.
			if !nextCalled || rec.Code != http.StatusOK {
				t.Errorf("next called = %v, status = %d", nextCalled, rec.Code)
			}
		})
	}
}

func TestDefaultAllowedOrigins_MatchesQRContent(t *testing.T) {
	t.Setenv("POSNET_HOST", "192.168.0.2")
	got := DefaultAllowedOrigins()
	want := []string{"http://localhost:5173", "http://192.168.0.2:5173"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("DefaultAllowedOrigins() = %v, want %v", got, want)
	}
}

func TestCorsMiddleware_HandlesPreflightOptions(t *testing.T) {
	var nextCalled bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
	})
	handler := corsMiddleware([]string{"http://localhost:5173"}, next)

	req := httptest.NewRequest(http.MethodOptions, "/sessions/tx-1/cancel", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if nextCalled {
		t.Error("next handler was called for an OPTIONS preflight request, want it short-circuited")
	}
}
