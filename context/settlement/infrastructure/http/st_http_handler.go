// Package http contiene el adaptador HTTP del BC Settlement.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	// missingTables informa las tablas que faltan en la base; nil = sin verificar.
	missingTables func(ctx context.Context) ([]string, error)
	// deadLetters es la DLQ de Settlement; nil = las rutas /dead-letters responden 503.
	deadLetters port.DeadLetterQueue
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
	mux.HandleFunc("GET  /batches/{id}/audit", requireBatchID(h.handleGetBatchAudit))
	mux.HandleFunc("GET  /merchants/{merchant_id}/batches", h.handleListBatches)
	mux.HandleFunc("POST /batches/{id}/force-close", requireBatchID(h.handleForceClose))
	mux.HandleFunc("POST /batches/{id}/resubmit", requireBatchID(h.handleResubmitBatch))
	mux.HandleFunc("POST /batches/{id}/reopen", requireBatchID(h.handleReopenBatch))
	mux.HandleFunc("POST /batches/{id}/resolve-dispute", requireBatchID(h.handleResolveDispute))
	mux.HandleFunc("POST /batches/{id}/settle", requireBatchID(h.handleSettleBatch))
	mux.HandleFunc("GET  /reversal-reviews", h.handleListReversalReviews)
	mux.HandleFunc("POST /reversal-reviews/{transaction_id}/resolve", h.handleResolveReversalReview)
	mux.HandleFunc("GET  /merchants/{merchant_id}/timezone", h.handleGetMerchantTimezone)
	mux.HandleFunc("PUT  /merchants/{merchant_id}/timezone", h.handleSetMerchantTimezone)
	mux.HandleFunc("GET  /dead-letters", h.handleListDeadLetters)
	mux.HandleFunc("POST /dead-letters/{sequence}/replay", h.handleReplayDeadLetter)
	mux.HandleFunc("POST /dead-letters/{sequence}/discard", h.handleDiscardDeadLetter)
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
	if h.missingTables != nil {
		missing, err := h.missingTables(r.Context())
		if err != nil {
			observability.FromContext(r.Context()).Error("readiness: could not check the schema", slog.String("error", err.Error()))
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not ready", "reason": "database unavailable",
			})
			return
		}
		// Los nombres van al log: este endpoint no pide token.
		if len(missing) > 0 {
			observability.FromContext(r.Context()).Error("readiness: required tables are missing — apply the migrations",
				slog.Any("tables", missing),
			)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not ready", "reason": "database schema incomplete",
			})
			return
		}
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

// GET /batches/{id}/audit
func (h *Handler) handleGetBatchAudit(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.GetBatchAudit")
	defer span.End()

	entries, truncated, err := h.queryHandler.GetBatchAudit(ctx, r.PathValue("id"))
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
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "count": len(entries), "truncated": truncated})
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
		OperatorID     string `json:"operator_id"`
		Reason         string `json:"reason"`
		TerminalCount  *int   `json:"terminal_count"`
		TerminalAmount *int64 `json:"terminal_amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	cmd := port.ForceCloseCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: op,
		Reason:     body.Reason,

		TerminalCount:  body.TerminalCount,
		TerminalAmount: body.TerminalAmount,
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
		Reason     string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	cmd := port.ResubmitBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: op,
		Reason:     body.Reason,
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
		if errors.Is(err, repository.ErrSubmittedTotalsChanged) {
			writeJSON(w, http.StatusConflict, errResp("SUBMITTED_TOTALS_CHANGED", err.Error()))
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

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	err := h.adminHandler.ReopenBatch(ctx, port.ReopenBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: op,
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

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	err := h.adminHandler.ResolveDispute(ctx, port.ResolveDisputeCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: op,
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
		Reason     string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	err := h.adminHandler.SettleBatch(ctx, port.SettleBatchCommand{
		BatchID:    r.PathValue("id"),
		OperatorID: op,
		Reason:     body.Reason,
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
	if errors.Is(err, repository.ErrOpenBatchConflict) {
		writeJSON(w, http.StatusConflict, errResp("OPEN_BATCH_CONFLICT",
			"a terminal of this merchant already has an OPEN batch for the new business date: close it first; nothing was changed"))
		return
	}
	observability.RecordError(r.Context(), err)
	writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
}

// GET /reversal-reviews
func (h *Handler) handleListReversalReviews(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ListReversalReviews")
	defer span.End()

	reviews, err := h.queryHandler.ListOpenReversalReviews(ctx)
	if err != nil {
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"reviews": reviews, "count": len(reviews)})
}

// POST /reversal-reviews/{transaction_id}/resolve
func (h *Handler) handleResolveReversalReview(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ResolveReversalReview")
	defer span.End()

	transactionID := r.PathValue("transaction_id")
	if len(transactionID) != canonicalUUIDLength || uuid.Validate(transactionID) != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "transaction id must be a UUID"))
		return
	}
	var body struct {
		OperatorID string `json:"operator_id"`
		Resolution string `json:"resolution"`
		Action     string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	err := h.adminHandler.ResolveReversalReview(ctx, port.ResolveReversalReviewCommand{
		TransactionID: transactionID,
		OperatorID:    op,
		Resolution:    body.Resolution,
		Action:        body.Action,
	})
	var notFound *pkgerrors.NotFoundError
	if errors.As(err, &notFound) {
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "reversal review not found"))
		return
	}
	h.writeAdminResult(w, r, err, "resolved")
}

// GET /merchants/{merchant_id}/timezone
func (h *Handler) handleGetMerchantTimezone(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.GetMerchantTimezone")
	defer span.End()

	result, err := h.queryHandler.MerchantTimezone(ctx, r.PathValue("merchant_id"))
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
	writeJSON(w, http.StatusOK, result)
}

// PUT /merchants/{merchant_id}/timezone
func (h *Handler) handleSetMerchantTimezone(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.SetMerchantTimezone")
	defer span.End()

	var body struct {
		Timezone   string `json:"timezone"`
		OperatorID string `json:"operator_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}
	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return
	}

	err := h.adminHandler.SetMerchantTimezone(ctx, port.SetMerchantTimezoneCommand{
		MerchantID: r.PathValue("merchant_id"),
		Timezone:   body.Timezone,
		OperatorID: op,
	})
	h.writeAdminResult(w, r, err, "updated")
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"error": code, "message": msg}
}
