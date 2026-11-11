// Package query contiene los query handlers del BC Settlement.
package query

import (
	"context"
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// BatchQueryHandler implementa las consultas de solo lectura del BC Settlement.
type BatchQueryHandler struct {
	batchRepo       repository.SettlementBatchRepository
	reviews         repository.ReversalReviewLog
	timezones       repository.MerchantTimezones
	defaultTimezone string
	audit           repository.BatchAuditReader
}

// MaxListedAuditEntries acota la auditoría que devuelve la API para un lote. Un lote tiene pocos
// registros: llegar al límite indica otra cosa, y la respuesta lo informa.
const MaxListedAuditEntries = 500

// ConfigureAudit habilita la consulta de la auditoría de los lotes.
func (h *BatchQueryHandler) ConfigureAudit(audit repository.BatchAuditReader) {
	h.audit = audit
}

// ConfigureReviews habilita la consulta de las revisiones de reversas.
func (h *BatchQueryHandler) ConfigureReviews(reviews repository.ReversalReviewLog) {
	h.reviews = reviews
}

// ConfigureTimezones habilita la consulta de la zona de negocio de un comercio.
func (h *BatchQueryHandler) ConfigureTimezones(timezones repository.MerchantTimezones, defaultTimezone string) {
	h.timezones = timezones
	h.defaultTimezone = defaultTimezone
}

func NewBatchQueryHandler(batchRepo repository.SettlementBatchRepository) *BatchQueryHandler {
	return &BatchQueryHandler{batchRepo: batchRepo}
}

// GetBatch retorna el estado de un batch por su ID.
func (h *BatchQueryHandler) GetBatch(ctx context.Context, batchID string) (*port.BatchResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.GetBatch")
	defer span.End()

	if batchID == "" {
		return nil, pkgerrors.NewValidationError("batch_id cannot be empty")
	}

	batch, err := h.batchRepo.FindByID(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("GetBatch: %w", err)
	}
	if batch == nil {
		return nil, pkgerrors.NewNotFoundError("SettlementBatch", batchID)
	}

	result := &port.BatchResult{
		ID:             batch.ID(),
		TerminalID:     batch.TerminalID().String(),
		MerchantID:     batch.MerchantID().String(),
		BatchDate:      batch.BatchDate().Format("2006-01-02"),
		State:          batch.State().String(),
		Currency:       batch.Currency(),
		Discrepancies:  batch.Discrepancies(),
		ReconciledWith: batch.ReconciledWith(),
	}

	if s := batch.Summary(); s != nil {
		result.TotalCount = s.TotalCount()
		result.TotalAmount = s.TotalAmount().Cents()
		result.AdjustmentCount, result.AdjustmentAmount = s.AdjustmentCount(), s.AdjustmentAmount()
	}

	if t := batch.ClosedAt(); t != nil {
		result.ClosedAt = t.Format(time.RFC3339)
	}
	if t := batch.SubmittedAt(); t != nil {
		result.SubmittedAt = t.Format(time.RFC3339)
	}
	if t := batch.SettledAt(); t != nil {
		result.SettledAt = t.Format(time.RFC3339)
	}

	return result, nil
}

// ListBatchesByMerchant lista los batches de un comercio en una fecha.
func (h *BatchQueryHandler) ListBatchesByMerchant(
	ctx context.Context,
	cmd port.ListBatchesCommand,
) ([]*port.BatchResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.ListBatchesByMerchant")
	defer span.End()

	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid merchant_id")
	}

	date, err := time.Parse("2006-01-02", cmd.Date)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid date format — use YYYY-MM-DD")
	}

	batches, err := h.batchRepo.ListByMerchantDate(ctx, merchantID, date)
	if err != nil {
		return nil, fmt.Errorf("ListBatchesByMerchant: %w", err)
	}

	results := make([]*port.BatchResult, 0, len(batches))
	for _, b := range batches {
		r := &port.BatchResult{
			ID:             b.ID(),
			TerminalID:     b.TerminalID().String(),
			MerchantID:     b.MerchantID().String(),
			BatchDate:      b.BatchDate().Format("2006-01-02"),
			State:          b.State().String(),
			Currency:       b.Currency(),
			Discrepancies:  b.Discrepancies(),
			ReconciledWith: b.ReconciledWith(),
		}
		if s := b.Summary(); s != nil {
			r.TotalCount = s.TotalCount()
			r.TotalAmount = s.TotalAmount().Cents()
			r.AdjustmentCount, r.AdjustmentAmount = s.AdjustmentCount(), s.AdjustmentAmount()
		}
		if t := b.ClosedAt(); t != nil {
			r.ClosedAt = t.Format(time.RFC3339)
		}
		results = append(results, r)
	}

	return results, nil
}

// maxListedReviews limita la respuesta de ListOpenReversalReviews. Muchas revisiones
// abiertas apuntan a un problema general, que no se resuelve de a una.
const maxListedReviews = 200

// ListOpenReversalReviews lista las revisiones abiertas, de la más antigua a la más nueva.
func (h *BatchQueryHandler) ListOpenReversalReviews(ctx context.Context) ([]port.ReversalReviewResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.ListOpenReversalReviews")
	defer span.End()

	if h.reviews == nil {
		return nil, fmt.Errorf("ListOpenReversalReviews: reviews are not configured")
	}
	reviews, err := h.reviews.ListOpenReversalReviews(ctx, maxListedReviews)
	if err != nil {
		return nil, fmt.Errorf("ListOpenReversalReviews: %w", err)
	}
	results := make([]port.ReversalReviewResult, 0, len(reviews))
	for _, v := range reviews {
		r := port.ReversalReviewResult{
			TransactionID:  v.TransactionID,
			BatchID:        v.BatchID,
			BatchState:     v.BatchState,
			Reason:         v.Reason,
			MerchantID:     v.MerchantID,
			ReversalAmount: v.AmountCents,
			Currency:       v.Currency,
			CreatedAt:      v.CreatedAt.UTC().Format(time.RFC3339),
		}
		if p := v.Purchase; p != nil {
			r.PurchaseMerchantID = &p.MerchantID
			r.PurchaseAmount = &p.AmountCents
			r.PurchaseCurrency = &p.Currency
		}
		results = append(results, r)
	}
	return results, nil
}

// MerchantTimezone devuelve la zona de negocio vigente de un comercio.
func (h *BatchQueryHandler) MerchantTimezone(ctx context.Context, merchantID string) (*port.MerchantTimezoneResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.MerchantTimezone")
	defer span.End()

	merchant, err := domain.ParseMerchantID(merchantID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid merchant_id")
	}
	if h.timezones == nil {
		return nil, fmt.Errorf("MerchantTimezone: timezones are not configured")
	}
	name, err := h.timezones.MerchantTimezone(ctx, merchant.String())
	if err != nil {
		return nil, fmt.Errorf("MerchantTimezone: %w", err)
	}
	if name == "" {
		return &port.MerchantTimezoneResult{MerchantID: merchant.String(), Timezone: h.defaultTimezone, Source: "default"}, nil
	}
	return &port.MerchantTimezoneResult{MerchantID: merchant.String(), Timezone: name, Source: "merchant"}, nil
}

// GetBatchAudit devuelve la auditoría de un lote, del registro más viejo al más nuevo. truncated
// indica que hay más registros que MaxListedAuditEntries.
func (h *BatchQueryHandler) GetBatchAudit(ctx context.Context, batchID string) (entries []port.AuditEntryResult, truncated bool, err error) {
	ctx, span := observability.StartSpan(ctx, "query.GetBatchAudit")
	defer span.End()

	if batchID == "" {
		return nil, false, pkgerrors.NewValidationError("batch_id cannot be empty")
	}
	if h.audit == nil {
		return nil, false, fmt.Errorf("GetBatchAudit: audit is not configured")
	}
	stored, err := h.audit.ListBatchAudit(ctx, batchID, MaxListedAuditEntries+1)
	if err != nil {
		return nil, false, fmt.Errorf("GetBatchAudit: %w", err)
	}
	// Sin registros, el lote puede no existir: un 404 lo distingue de un lote sin cambios manuales.
	if len(stored) == 0 {
		batch, err := h.batchRepo.FindByID(ctx, batchID)
		if err != nil {
			return nil, false, fmt.Errorf("GetBatchAudit: find batch: %w", err)
		}
		if batch == nil {
			return nil, false, pkgerrors.NewNotFoundError("SettlementBatch", batchID)
		}
	}
	if len(stored) > MaxListedAuditEntries {
		stored, truncated = stored[:MaxListedAuditEntries], true
	}
	entries = make([]port.AuditEntryResult, 0, len(stored))
	for _, e := range stored {
		entries = append(entries, port.AuditEntryResult{
			ID: e.ID, Action: e.Action, OperatorID: e.OperatorID, Reason: e.Reason,
			FromState: e.FromState, ToState: e.ToState, PreviousDiscrepancies: e.PreviousDiscrepancies,
			TotalAmount: e.TotalAmount, ConfirmationID: e.ConfirmationID,
			CreatedAt: e.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	return entries, truncated, nil
}
