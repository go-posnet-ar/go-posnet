package http

import (
	"context"
	"net/http"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// NewRouter construye y retorna el mux HTTP del BC Authorization
// con todas las rutas registradas y los middlewares aplicados.
//
// Rutas expuestas:
//
//	GET  /healthz              → liveness probe
//	GET  /readyz               → readiness probe (verifica Postgres y el schema)
//	GET  /metrics              → métricas Prometheus
//	GET  /transactions/{id}    → estado de una transacción (operaciones)
func NewRouter(queryService port.QueryService, pool pgutil.PgxPool, missingSchema func(ctx context.Context) ([]string, error)) http.Handler {
	mux := http.NewServeMux()

	h := NewHandler(queryService, pool)
	h.missingSchema = missingSchema
	h.RegisterRoutes(mux)

	// Registrar endpoint de métricas Prometheus.
	// El handler es provisto por el exporter de OpenTelemetry inicializado
	// en cmd/authorization/main.go mediante observability.InitMeter().
	mux.Handle("GET /metrics", observability.MetricsHandler())

	// Aplicar middlewares globales en orden:
	//   1. Recover  — captura panics y retorna 500 sin caer el proceso
	//   2. Logging  — traza distribuida + log de cada request
	return observability.HTTPMiddleware(
		recoverMiddleware(mux),
	)
}
