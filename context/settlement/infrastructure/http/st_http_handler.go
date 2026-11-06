// Package http contiene el adaptador HTTP del BC Settlement.
package http

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// canonicalUUIDLength es la longitud de un UUID con guiones. uuid.Validate también
// acepta otras formas, como "urn:uuid:...", que Postgres no convierte.
const canonicalUUIDLength = 36

// Handler contiene todos los handlers HTTP del BC Settlement.
type Handler struct {
	queryHandler *query.BatchQueryHandler
	adminHandler *command.AdminHandler
	pool         pgutil.PgxPool
	// natsConnected informa el estado de la conexión a NATS; nil = sin verificar.
	natsConnected func() bool
}

func NewHandler(
	queryHandler *query.BatchQueryHandler,
	adminHandler *command.AdminHandler,
	pool pgutil.PgxPool,
) *Handler {
	return &Handler{queryHandler: queryHandler, adminHandler: adminHandler, pool: pool}
}

// RegisterRoutes registra las rutas HTTP del BC.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET  /healthz", h.handleHealth)
	mux.HandleFunc("GET  /readyz", h.handleReady)
	mux.HandleFunc("GET  /batches/{id}", requireBatchID(h.handleGetBatch))
	mux.HandleFunc("GET  /merchants/{merchant_id}/batches", h.handleListBatches)
	mux.HandleFunc("POST /batches/{id}/force-close", requireBatchID(h.handleForceClose))
	mux.HandleFunc("POST /batches/{id}/resubmit", requireBatchID(h.handleResubmitBatch))
	mux.HandleFunc("POST /batches/{id}/reopen", requireBatchID(h.handleReopenBatch))
	mux.HandleFunc("POST /batches/{id}/resolve-dispute", requireBatchID(h.handleResolveDispute))
	mux.HandleFunc("POST /batches/{id}/settle", requireBatchID(h.handleSettleBatch))
}

// requireBatchID rechaza un {id} que no sea un UUID canónico. Sin esto llega a
// Postgres, que falla al convertirlo a UUID, y el cliente recibe un 500 por un dato suyo.
func requireBatchID(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if len(id) != canonicalUUIDLength || uuid.Validate(id) != nil {
			writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "batch id must be a UUID"))
			return
		}
		next(w, r)
	}
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := pgutil.HealthCheck(r.Context(), h.pool); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready", "reason": "database unavailable",
		})
		return
	}
	if h.natsConnected != nil && !h.natsConnected() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready", "reason": "nats unavailable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// GET /batches/{id}
func (h *Handler) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.GetBatch")
	defer span.End()

	result, err := h.queryHandler.GetBatch(ctx, r.PathValue("id"))
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "batch not found"))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// GET /merchants/{merchant_id}/batches?date=2025-06-04
func (h *Handler) handleListBatches(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ListBatches")
	defer span.End()

	cmd := port.ListBatchesCommand{
		MerchantID: r.PathValue("merchant_id"),
		Date:       r.URL.Query().Get("date"),
	}

	results, err := h.queryHandler.ListBatchesByMerchant(ctx, cmd)
	if err != nil {
		var validationErr *pkgerrors.ValidationError
		if errors.As(err, &validationErr) {
			writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", err.Error()))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"batches": results, "count": len(results)})
}

// POST /batches/{id}/force-close
func (h *Handler) handleForceClose(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ForceClose")
	defer span.End()

	var body struct {
		OperatorID string `json:"operator_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	cmd := port.ForceCloseCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: body.OperatorID,
	}

	if err := h.adminHandler.ForceClose(ctx, cmd); err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "batch not found"))
			return
		}
		var validationErr *pkgerrors.ValidationError
		if errors.As(err, &validationErr) {
			writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", err.Error()))
			return
		}
		if errors.Is(err, repository.ErrStaleBatch) {
			writeJSON(w, http.StatusConflict, errResp("CONFLICT", "batch was modified concurrently; retry"))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed"})
}

// POST /batches/{id}/resubmit
func (h *Handler) handleResubmitBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ResubmitBatch")
	defer span.End()

	var body struct {
		OperatorID string `json:"operator_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	cmd := port.ResubmitBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: body.OperatorID,
	}

	if err := h.adminHandler.ResubmitBatch(ctx, cmd); err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "batch not found"))
			return
		}
		var validationErr *pkgerrors.ValidationError
		if errors.As(err, &validationErr) {
			writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", err.Error()))
			return
		}
		if errors.Is(err, repository.ErrStaleBatch) {
			writeJSON(w, http.StatusConflict, errResp("CONFLICT", "batch was modified concurrently; retry"))
			return
		}
		if errors.Is(err, service.ErrProcessorUnavailable) {
			writeJSON(w, http.StatusServiceUnavailable,
				errResp("PROCESSOR_UNAVAILABLE", "no settlement processor configured; batch remains CLOSED"))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "submitted"})
}

// disputeRequest es el body de las operaciones que resuelven una disputa.
type disputeRequest struct {
	OperatorID string `json:"operator_id"`
	Reason     string `json:"reason"`
}

// POST /batches/{id}/reopen
func (h *Handler) handleReopenBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ReopenBatch")
	defer span.End()

	var body disputeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	err := h.adminHandler.ReopenBatch(ctx, port.ReopenBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: body.OperatorID,
		Reason:     body.Reason,
	})
	h.writeAdminResult(w, r, err, "reopened")
}

// POST /batches/{id}/resolve-dispute
func (h *Handler) handleResolveDispute(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ResolveDispute")
	defer span.End()

	var body disputeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	err := h.adminHandler.ResolveDispute(ctx, port.ResolveDisputeCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: body.OperatorID,
		Reason:     body.Reason,
	})
	h.writeAdminResult(w, r, err, "closed")
}

// POST /batches/{id}/settle
func (h *Handler) handleSettleBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.SettleBatch")
	defer span.End()

	var body struct {
		OperatorID string `json:"operator_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	err := h.adminHandler.SettleBatch(ctx, port.SettleBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: body.OperatorID,
	})
	h.writeAdminResult(w, r, err, "settled")
}

func (h *Handler) writeAdminResult(w http.ResponseWriter, r *http.Request, err error, status string) {
	if err == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": status})
		return
	}
	var notFound *pkgerrors.NotFoundError
	if errors.As(err, &notFound) {
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "batch not found"))
		return
	}
	var validationErr *pkgerrors.ValidationError
	if errors.As(err, &validationErr) {
		writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", err.Error()))
		return
	}
	if errors.Is(err, repository.ErrStaleBatch) {
		writeJSON(w, http.StatusConflict, errResp("CONFLICT", "batch was modified concurrently; retry"))
		return
	}
	observability.RecordError(r.Context(), err)
	writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}
