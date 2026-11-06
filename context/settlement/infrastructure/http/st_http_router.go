package http

import (
	"net/http"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// NewRouter construye el mux HTTP del BC Settlement con middlewares aplicados.
func NewRouter(
	queryHandler *query.BatchQueryHandler,
	adminHandler *command.AdminHandler,
	pool pgutil.PgxPool,
	adminAPIKey string,
	natsConnected func() bool,
) http.Handler {
	mux := http.NewServeMux()

	h := NewHandler(queryHandler, adminHandler, pool)
	h.natsConnected = natsConnected
	h.RegisterRoutes(mux)

	mux.Handle("GET /metrics", observability.MetricsHandler())

	return observability.HTTPMiddleware(recoverMiddleware(adminAuthMiddleware(adminAPIKey, mux)))
}
