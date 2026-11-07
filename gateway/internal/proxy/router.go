package proxy

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/juantevez/go-posnet/gateway/internal/middleware"
)

// Routes son los servicios y límites que publica el gateway.
type Routes struct {
	TerminalGateway http.Handler
	// Settlement es opcional: nil, sus rutas no se publican.
	Settlement http.Handler
	// TerminalGatewayWS es el WebSocket de los terminales. Opcional: nil, /ws no se publica.
	TerminalGatewayWS http.Handler

	// Límites de las rutas públicas, que usa el teléfono del cliente sin credenciales.
	PayLimit    *middleware.RateLimiter
	StatusLimit *middleware.RateLimiter

	// Metrics es opcional: nil, no se registra nada.
	Metrics *middleware.Metrics
}

// NewRouter publica solo las rutas listadas, con su path original: cualquier otra, incluidas
// /metrics y /readyz de los servicios, responde 404. Authorization, Fraud Detection y
// Notification no se publican: sus APIs no tienen autenticación.
func NewRouter(r Routes) http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, h http.Handler) { mux.Handle(pattern, r.Metrics.Route(pattern, h)) }
	mux.HandleFunc("GET /healthz", healthOK)

	tg := r.TerminalGateway
	// Públicas: las usa el teléfono del cliente.
	handle("POST /api/sessions/{id}/pay", validUUID("id", r.PayLimit.Limit(tg)))
	handle("GET /api/sessions/{id}/status", validUUID("id", r.StatusLimit.Limit(tg)))
	// De terminal: Terminal Gateway exige el token.
	handle("POST /api/sessions/create", tg)
	handle("GET /sessions/{id}", validUUID("id", tg))
	handle("POST /sessions/{id}/cancel", validUUID("id", tg))
	handle("POST /sessions/{id}/reversal", validUUID("id", tg))
	handle("POST /batch-close", tg)
	// WebSocket de los terminales: se autentican con su primer mensaje.
	if ws := r.TerminalGatewayWS; ws != nil {
		handle("GET /ws", ws)
	}

	// De operación: Settlement exige su token de administración.
	if st := r.Settlement; st != nil {
		handle("GET /batches/{id}", validUUID("id", st))
		for _, action := range []string{"force-close", "reopen", "resolve-dispute", "resubmit", "settle"} {
			handle("POST /batches/{id}/"+action, validUUID("id", st))
		}
		handle("GET /merchants/{merchant_id}/batches", validUUID("merchant_id", st))
		handle("GET /merchants/{merchant_id}/timezone", validUUID("merchant_id", st))
		handle("PUT /merchants/{merchant_id}/timezone", validUUID("merchant_id", st))
		handle("GET /reversal-reviews", st)
		handle("POST /reversal-reviews/{transaction_id}/resolve", validUUID("transaction_id", st))
	}

	handle("/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		middleware.WriteError(w, http.StatusNotFound, "NOT_FOUND", "route not found")
	}))
	return mux
}

// validUUID corta en el gateway un ID inválido, sin llegar al servicio.
func validUUID(param string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := uuid.Parse(r.PathValue(param)); err != nil {
			middleware.WriteError(w, http.StatusBadRequest, "INVALID_ID", "invalid "+param)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// NewAdminRouter publica /healthz, /readyz y /metrics del gateway, en un puerto que no se expone
// hacia afuera. /readyz no consulta a los servicios: la caída de uno no debe sacar de rotación al
// gateway entero.
func NewAdminRouter(metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthOK)
	mux.HandleFunc("GET /readyz", healthOK)
	mux.Handle("GET /metrics", metrics)
	return mux
}

func healthOK(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
