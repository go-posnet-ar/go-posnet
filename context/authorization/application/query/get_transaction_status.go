package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// TransactionQueryHandler implementa port.QueryService.
// Lado Q del patrón CQRS — solo lectura, sin efectos secundarios.
type TransactionQueryHandler struct {
	repo repository.TransactionRepository
}

func NewTransactionQueryHandler(repo repository.TransactionRepository) *TransactionQueryHandler {
	return &TransactionQueryHandler{repo: repo}
}

// GetTransactionStatus retorna el estado actual de una transacción por ID.
func (h *TransactionQueryHandler) GetTransactionStatus(
	ctx context.Context,
	id domain.TransactionID,
) (*port.TransactionStatusResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.GetTransactionStatus")
	defer span.End()

	tx, err := h.repo.FindByID(ctx, id)
	var notFound *pkgerrors.NotFoundError
	if err != nil && !errors.As(err, &notFound) {
		return nil, fmt.Errorf("GetTransactionStatus: %w", err)
	}
	if tx == nil {
		return h.invalidRejectionStatus(ctx, id)
	}

	result := &port.TransactionStatusResult{
		TransactionID: tx.ID().String(),
		State:         tx.State().String(),
		AmountCents:   tx.Amount().Cents(),
		Currency:      tx.Amount().Currency().String(),
	}

	if tx.State() == valueobject.StateApproved && tx.AuthCode() != nil {
		result.AuthCode = tx.AuthCode().String()
		if tx.AuthorizedAt() != nil {
			result.AuthorizedAt = tx.AuthorizedAt().Format("2006-01-02T15:04:05Z")
		}
	}

	if tx.State() == valueobject.StateRejected && tx.RejectionCode() != nil {
		result.RejectionCode = tx.RejectionCode().Code()
		result.RejectionReason = tx.RejectionCode().Description()
		if tx.RejectedAt() != nil {
			result.RejectedAt = tx.RejectedAt().Format("2006-01-02T15:04:05Z")
		}
	}

	if tx.ReversalRequestedAt() != nil && tx.State() != valueobject.StateReversed && tx.State() != valueobject.StateRejected {
		result.ReversalRequestedAt = tx.ReversalRequestedAt().UTC().Format("2006-01-02T15:04:05Z")
	}

	return result, nil
}

// invalidRejectionStatus responde por una transacción que no existe como aggregate: la rechazada
// por datos inválidos. El motivo exacto queda para auditoría: esta API no tiene autenticación.
func (h *TransactionQueryHandler) invalidRejectionStatus(ctx context.Context, id domain.TransactionID) (*port.TransactionStatusResult, error) {
	rejection, err := h.repo.FindInvalidRejection(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("GetTransactionStatus: %w", err)
	}
	if rejection == nil {
		return nil, pkgerrors.NewNotFoundError("Transaction", id.String())
	}
	return &port.TransactionStatusResult{
		TransactionID:   id.String(),
		State:           valueobject.StateRejected.String(),
		AmountCents:     rejection.AmountCents,
		Currency:        rejection.Currency,
		RejectionCode:   rejection.Rejection.Code(),
		RejectionReason: rejection.Rejection.Description(),
		RejectedAt:      rejection.RejectedAt.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}
