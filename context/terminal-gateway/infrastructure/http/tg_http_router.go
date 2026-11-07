package http

import (
	"net/http"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// NewRouter construye el mux HTTP del BC Terminal Gateway con middlewares aplicados.
func NewRouter(
	sessionService port.SessionService,
	queryHandler *query.SessionQueryHandler,
	pool pgutil.PgxPool,
	allowedOrigins []string,
	auth *TerminalAuth,
	payTokens *PayTokens,
) http.Handler {
	mux := http.NewServeMux()

	// Rutas existentes
	h := NewHandler(sessionService, queryHandler, pool)
	h.auth = auth
	h.RegisterRoutes(mux)

	// Rutas del flujo QR — simulador POSNET
	qr := NewQRHandler(sessionService, queryHandler)
	qr.auth = auth
	qr.payTokens = payTokens
	qr.RegisterQRRoutes(mux)

	mux.Handle("GET /metrics", observability.MetricsHandler())

	// CORS antes de observability para que el preflight OPTIONS no quede bloqueado
	return corsMiddleware(allowedOrigins, observability.HTTPMiddleware(recoverMiddleware(mux)))
}

// DefaultAllowedOrigins son los orígenes del frontend de pago: el mismo host que arma qr_content,
// y localhost para desarrollo.
func DefaultAllowedOrigins() []string {
	origins := []string{"http://localhost:" + frontendPort}
	if host := getLocalIP(); host != "localhost" {
		origins = append(origins, "http://"+host+":"+frontendPort)
	}
	return origins
}

// corsMiddleware deja que un navegador llame a la API solo desde los orígenes permitidos, por
// ejemplo el frontend de pago. "*" permite cualquiera. No reemplaza a la autenticación: un
// cliente que no es un navegador no envía Origin, y no se lo frena.
func corsMiddleware(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	anyOrigin := allowed["*"]

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		if origin := r.Header.Get("Origin"); origin != "" && (anyOrigin || allowed[origin]) {
			if anyOrigin {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Terminal-ID, X-Session-Token")
		}

		// El browser envía un preflight OPTIONS antes del POST real
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
