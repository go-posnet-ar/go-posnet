package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/observability"
)

func TestNewRouter_Routes(t *testing.T) {
	qs := &fakeQueryService{result: &port.TransactionStatusResult{
		TransactionID: "tx-1",
		State:         "RECEIVED",
	}}
	router := NewRouter(qs, &fakePool{}, Readiness{})

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{"healthz", "/healthz", http.StatusOK},
		{"readyz", "/readyz", http.StatusOK},
		{"metrics", "/metrics", http.StatusOK},
		{"transaction status", "/transactions/" + domain.NewTransactionID().String(), http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestNewRouter_ReadyzReflectsPoolFailure(t *testing.T) {
	pool := &fakePool{pingErr: errors.New("connection refused")}
	router := NewRouter(&fakeQueryService{}, pool, Readiness{})

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestNewRouter_RecoversFromPanicInHandler(t *testing.T) {
	router := NewRouter(&panickyQueryService{}, &fakePool{}, Readiness{})

	req := httptest.NewRequest(http.MethodGet, "/transactions/"+domain.NewTransactionID().String(), nil)
	rec := httptest.NewRecorder()

	// No debe propagar el panic — recoverMiddleware debe atraparlo incluso
	// cuando está compuesto detrás de observability.HTTPMiddleware.
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestNewRouter_UnknownRouteReturns404(t *testing.T) {
	router := NewRouter(&fakeQueryService{}, &fakePool{}, Readiness{})

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
	// El handler sirve el registry default de Prometheus, que siempre
	// incluye las métricas base del runtime Go (go_goroutines, etc.).
	if !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Errorf("body should expose Go runtime metrics, got %q", rec.Body.String())
	}
}
