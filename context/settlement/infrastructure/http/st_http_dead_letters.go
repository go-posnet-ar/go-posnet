package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
)

const (
	deadLettersDefaultLimit = 100
	deadLettersMaxLimit     = 500
)

// GET /dead-letters?limit=N
func (h *Handler) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ListDeadLetters")
	defer span.End()
	if !h.deadLettersEnabled(w) {
		return
	}

	limit := deadLettersDefaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > deadLettersMaxLimit {
			writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", "limit must be between 1 and 500"))
			return
		}
		limit = n
	}
	letters, err := h.deadLetters.List(ctx, limit)
	if err != nil {
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusServiceUnavailable, errResp("DLQ_UNAVAILABLE", "could not read the dead letter queue"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dead_letters": letters, "count": len(letters)})
}

// POST /dead-letters/{sequence}/replay
func (h *Handler) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.ReplayDeadLetter")
	defer span.End()
	seq, op, _, ok := h.deadLetterRequest(w, r, false)
	if !ok {
		return
	}

	err := h.deadLetters.Replay(ctx, seq)
	log := observability.FromContext(ctx).With(slog.Uint64("sequence", seq), slog.String("operator_id", op))
	var notFound *pkgerrors.NotFoundError
	var validationErr *pkgerrors.ValidationError
	var conflictErr *pkgerrors.ConflictError
	switch {
	case err == nil:
		log.Warn("dead letter replayed by operator")
		writeJSON(w, http.StatusOK, map[string]string{"status": "replayed"})
	case errors.As(err, &notFound):
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "dead letter not found"))
	case errors.As(err, &validationErr) || errors.As(err, &conflictErr):
		// Reintentarlo no lo arregla: queda en la DLQ para descartarlo.
		log.Warn("dead letter replay rejected", slog.String("error", err.Error()))
		writeJSON(w, http.StatusUnprocessableEntity, errResp("REPLAY_REJECTED", err.Error()))
	default:
		observability.RecordError(ctx, err)
		log.Error("dead letter replay failed — it stays in the queue", slog.String("error", err.Error()))
		writeJSON(w, http.StatusServiceUnavailable, errResp("REPLAY_FAILED",
			"the event could not be processed and stays in the dead letter queue; retry later"))
	}
}

// POST /dead-letters/{sequence}/discard
func (h *Handler) handleDiscardDeadLetter(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.DiscardDeadLetter")
	defer span.End()
	seq, op, reason, ok := h.deadLetterRequest(w, r, true)
	if !ok {
		return
	}

	err := h.deadLetters.Discard(ctx, seq)
	var notFound *pkgerrors.NotFoundError
	switch {
	case err == nil:
		observability.FromContext(ctx).Warn("dead letter discarded by operator",
			slog.Uint64("sequence", seq), slog.String("operator_id", op), slog.String("reason", reason))
		writeJSON(w, http.StatusOK, map[string]string{"status": "discarded"})
	case errors.As(err, &notFound):
		writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "dead letter not found"))
	default:
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusServiceUnavailable, errResp("DLQ_UNAVAILABLE", "could not delete the dead letter"))
	}
}

// deadLetterRequest lee la secuencia del path y el operador del body. needReason exige un motivo.
func (h *Handler) deadLetterRequest(w http.ResponseWriter, r *http.Request, needReason bool) (uint64, string, string, bool) {
	if !h.deadLettersEnabled(w) {
		return 0, "", "", false
	}
	seq, err := strconv.ParseUint(r.PathValue("sequence"), 10, 64)
	if err != nil || seq == 0 {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "sequence must be a positive integer"))
		return 0, "", "", false
	}
	var body struct {
		OperatorID string `json:"operator_id"`
		Reason     string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return 0, "", "", false
	}
	op, ok := operator(w, r, body.OperatorID)
	if !ok {
		return 0, "", "", false
	}
	if op == "" {
		writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", "operator_id cannot be empty for audit"))
		return 0, "", "", false
	}
	if needReason && body.Reason == "" {
		writeJSON(w, http.StatusBadRequest, errResp("VALIDATION_ERROR", "reason cannot be empty: the event is lost"))
		return 0, "", "", false
	}
	return seq, op, body.Reason, true
}

func (h *Handler) deadLettersEnabled(w http.ResponseWriter) bool {
	if h.deadLetters == nil {
		writeJSON(w, http.StatusServiceUnavailable, errResp("DLQ_DISABLED", "the dead letter queue is not configured"))
		return false
	}
	return true
}
