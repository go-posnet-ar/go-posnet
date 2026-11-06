package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/observability"
)

const testAdminKey = "test-admin-key-with-enough-characters"

func newRouterFor(repo repository.SettlementBatchRepository, pool *fakePool) http.Handler {
	return newRouterWithKey(repo, pool, testAdminKey)
}

func newRouterWithKey(repo repository.SettlementBatchRepository, pool *fakePool, key string) http.Handler {
	queryHandler := query.NewBatchQueryHandler(repo)
	adminHandler := command.NewAdminHandler(repo, &fakeProcessor{})
	return NewRouter(queryHandler, adminHandler, pool, key, nil)
}

func TestNewRouter_Routes(t *testing.T) {
	b := newBatch(t)
	repo := &fakeBatchRepo{
		findByIDResult: b,
		listResult:     nil,
	}
	router := newRouterFor(repo, &fakePool{})

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
		{"get batch", http.MethodGet, "/batches/" + b.ID(), "", http.StatusOK},
		{"list batches", http.MethodGet, "/merchants/" + b.MerchantID().String() + "/batches?date=2026-01-15", "", http.StatusOK},
		{"force close", http.MethodPost, "/batches/" + b.ID() + "/force-close", `{"operator_id":"op-1"}`, http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.body != "" {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			} else {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			}
			req.Header.Set("Authorization", "Bearer "+testAdminKey)
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
	router := newRouterFor(&fakeBatchRepo{}, pool)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestNewRouter_RecoversFromPanicInHandler(t *testing.T) {
	// panickyBatchRepo simula un bug en la capa de aplicación en vez de
	// devolver el error normalmente — sirve para probar que recoverMiddleware
	// protege incluso detrás de la composición completa de middlewares.
	router := newRouterFor(&panickyBatchRepo{}, &fakePool{})

	req := httptest.NewRequest(http.MethodGet, "/batches/11111111-1111-4111-8111-111111111111", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestNewRouter_UnknownRouteReturns404(t *testing.T) {
	router := newRouterFor(&fakeBatchRepo{}, &fakePool{})

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

// ─── Autenticación de operaciones de escritura ────────────────────────────────

func TestNewRouter_AdminAuth(t *testing.T) {
	writePaths := []string{"force-close", "resubmit", "reopen", "resolve-dispute", "settle"}

	tests := []struct {
		name       string
		key        string
		authHeader string
		wantStatus int
		wantError  string
	}{
		{"sin token", testAdminKey, "", http.StatusUnauthorized, "UNAUTHORIZED"},
		{"token incorrecto", testAdminKey, "Bearer otro-token", http.StatusUnauthorized, "UNAUTHORIZED"},
		{"token sin Bearer", testAdminKey, testAdminKey, http.StatusUnauthorized, "UNAUTHORIZED"},
		{"esquema distinto", testAdminKey, "Basic " + testAdminKey, http.StatusUnauthorized, "UNAUTHORIZED"},
		{"sin clave configurada", "", "Bearer cualquiera", http.StatusServiceUnavailable, "ADMIN_API_DISABLED"},
		{"sin clave configurada ni token", "", "", http.StatusServiceUnavailable, "ADMIN_API_DISABLED"},
	}

	for _, tc := range tests {
		for _, path := range writePaths {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				// Si la autenticación dejara pasar el pedido, el lote se modificaría.
				b := newBatch(t)
				router := newRouterWithKey(&fakeBatchRepo{findByIDResult: b}, &fakePool{}, tc.key)

				req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/"+path,
					strings.NewReader(`{"operator_id":"op-1","reason":"r"}`))
				if tc.authHeader != "" {
					req.Header.Set("Authorization", tc.authHeader)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)

				if rec.Code != tc.wantStatus {
					t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
				}
				if got := decodeJSON(t, rec.Body.Bytes())["error"]; got != tc.wantError {
					t.Errorf("body[error] = %v, want %q", got, tc.wantError)
				}
				if b.State() != valueobject.BatchStateOpen {
					t.Errorf("State() = %v, want it unchanged", b.State())
				}
			})
		}
	}
}

func TestNewRouter_ValidTokenReachesHandler(t *testing.T) {
	b := newBatch(t)
	router := newRouterFor(&fakeBatchRepo{findByIDResult: b}, &fakePool{})

	req := httptest.NewRequest(http.MethodPost, "/batches/"+b.ID()+"/force-close", strings.NewReader(`{"operator_id":"op-1"}`))
	req.Header.Set("Authorization", "Bearer "+testAdminKey)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if b.State() != valueobject.BatchStateClosed {
		t.Errorf("State() = %v, want %v", b.State(), valueobject.BatchStateClosed)
	}
}

// Las lecturas y los health checks siguen abiertos, incluso sin clave configurada.
func TestNewRouter_ReadsDoNotRequireToken(t *testing.T) {
	b := newBatch(t)
	router := newRouterWithKey(&fakeBatchRepo{findByIDResult: b}, &fakePool{}, "")

	for _, path := range []string{"/healthz", "/readyz", "/metrics", "/batches/" + b.ID()} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}

// Una escritura nueva queda protegida aunque nadie la agregue a una lista.
func TestAdminAuthMiddleware_ProtectsOtherMethods(t *testing.T) {
	called := false
	handler := adminAuthMiddleware(testAdminKey, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))

	for _, method := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, "/x", nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s status = %d, want %d", method, rec.Code, http.StatusUnauthorized)
		}
	}
	if called {
		t.Error("el handler se ejecutó sin token")
	}
}
