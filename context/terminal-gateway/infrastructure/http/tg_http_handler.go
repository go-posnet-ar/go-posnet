// Package http contiene el adaptador HTTP del BC Terminal Gateway.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// Handler contiene todos los handlers HTTP del BC Terminal Gateway.
type Handler struct {
	sessionService port.SessionService
	queryHandler   *query.SessionQueryHandler
	pool           pgutil.PgxPool
	auth           *TerminalAuth // nil = sin autenticación, solo en desarrollo
}

func NewHandler(
	sessionService port.SessionService,
	queryHandler *query.SessionQueryHandler,
	pool pgutil.PgxPool,
) *Handler {
	return &Handler{
		sessionService: sessionService,
		queryHandler:   queryHandler,
		pool:           pool,
	}
}

// RegisterRoutes registra todas las rutas HTTP del BC.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET  /healthz", h.handleHealth)
	mux.HandleFunc("GET  /readyz", h.handleReady)
	mux.HandleFunc("GET  /sessions/{id}", h.auth.requireTerminal(h.handleGetSession))
	mux.HandleFunc("POST /sessions/{id}/cancel", h.auth.requireTerminal(h.handleCancelSession))
	mux.HandleFunc("POST /sessions/{id}/reversal", h.auth.requireTerminal(h.handleRequestReversal))
	mux.HandleFunc("POST /batch-close", h.auth.requireTerminal(h.handleBatchClose))
}

// GET /healthz — liveness probe
func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /readyz — readiness probe
func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := pgutil.HealthCheck(r.Context(), h.pool); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready", "reason": "database unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// GET /sessions/{id} — estado de una sesión (operaciones/soporte)
func (h *Handler) handleGetSession(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.GetSession")
	defer span.End()

	txID, err := domain.ParseTransactionID(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "invalid session id"))
		return
	}

	result, err := h.queryHandler.GetSessionStatus(ctx, txID)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "session not found"))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	// La sesión de otro terminal se responde como inexistente, para no confirmar que existe.
	if id, ok := authenticatedTerminal(ctx); ok && result.TerminalID != id.String() {
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "session not found"))
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// POST /sessions/{id}/cancel — cancelación manual por el cajero
func (h *Handler) handleCancelSession(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.CancelSession")
	defer span.End()

	terminalID := requestTerminal(r)

	cmd := port.CancelSessionCommand{
		TransactionID: r.PathValue("id"),
		TerminalID:    terminalID,
	}

	if err := h.sessionService.CancelSession(ctx, cmd); err != nil {
		writeSessionCommandError(ctx, w, err, "cancel failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// POST /sessions/{id}/reversal — anulación de transacción aprobada
func (h *Handler) handleRequestReversal(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.RequestReversal")
	defer span.End()

	terminalID := requestTerminal(r)

	cmd := port.RequestReversalCommand{
		OriginalTransactionID: r.PathValue("id"),
		TerminalID:            terminalID,
	}

	if err := h.sessionService.RequestReversal(ctx, cmd); err != nil {
		writeSessionCommandError(ctx, w, err, "reversal request failed")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "reversal_requested"})
}

// POST /batch-close — cierre de lote del terminal
func (h *Handler) handleBatchClose(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.BatchClose")
	defer span.End()

	var req struct {
		TerminalID     string `json:"terminal_id"`
		MerchantID     string `json:"merchant_id"`
		BatchDate      string `json:"batch_date"`
		TerminalCount  int    `json:"terminal_count"`
		TerminalAmount int64  `json:"terminal_amount"`
		Currency       string `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	if id, ok := authenticatedTerminal(ctx); ok {
		if req.TerminalID != "" && req.TerminalID != id.String() {
			writeForbidden(w)
			return
		}
		req.TerminalID = id.String()
	}

	cmd := port.RequestBatchCloseCommand{
		TerminalID:     req.TerminalID,
		MerchantID:     req.MerchantID,
		BatchDate:      req.BatchDate,
		TerminalCount:  req.TerminalCount,
		TerminalAmount: req.TerminalAmount,
		Currency:       req.Currency,
	}

	if err := h.sessionService.RequestBatchClose(ctx, cmd); err != nil {
		var invalid *pkgerrors.ValidationError
		if errors.As(err, &invalid) {
			writeJSON(w, http.StatusBadRequest, errResp("INVALID_REQUEST", invalid.Error()))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "batch close failed"))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "batch_close_requested"})
}

// writeSessionCommandError responde el error de cancel o reversal: 400 si el pedido es
// inválido, 404 si la sesión no existe o es de otro terminal, 409 si su estado no lo admite y
// 500, con un ERROR en el log, en cualquier otro caso.
func writeSessionCommandError(ctx context.Context, w http.ResponseWriter, err error, internalMsg string) {
	var invalid *pkgerrors.ValidationError
	var notFound *pkgerrors.NotFoundError
	switch {
	case errors.As(err, &invalid):
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_REQUEST", invalid.Error()))
	case errors.As(err, &notFound):
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "session not found"))
	case errors.Is(err, aggregate.ErrInvalidTransition):
		writeJSON(w, http.StatusConflict, errResp("INVALID_STATE", "the session does not allow this operation in its current state"))
	default:
		observability.RecordError(ctx, err)
		slog.ErrorContext(ctx, "request failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", internalMsg))
	}
}

// requestTerminal devuelve el terminal que hace el pedido: el del token o, sin tokens
// configurados, el header X-Terminal-ID, que el cliente puede falsear.
func requestTerminal(r *http.Request) string {
	if id, ok := authenticatedTerminal(r.Context()); ok {
		return id.String()
	}
	return r.Header.Get("X-Terminal-ID")
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}
