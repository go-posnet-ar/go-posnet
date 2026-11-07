// Package query contiene los query handlers del BC Terminal Gateway.
package query

import (
	"context"
	"fmt"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	valueobject "github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// SessionQueryHandler implementa las queries de solo lectura del BC.
type SessionQueryHandler struct {
	sessionRepo repository.PaymentSessionRepository
}

func NewSessionQueryHandler(repo repository.PaymentSessionRepository) *SessionQueryHandler {
	return &SessionQueryHandler{sessionRepo: repo}
}

// SessionStatusResult es el resultado de la query de estado de sesión.
type SessionStatusResult struct {
	TransactionID   string
	TerminalID      string
	MerchantID      string
	State           string
	Channel         string
	AmountCents     int64
	Currency        string
	CreatedAt       string
	ExpiresAt       string
	TTLSeconds      int
	AuthCode        string // Solo si APPROVED
	RejectionCode   string // Solo si REJECTED
	RejectionReason string // Solo si REJECTED
	// CaptureCard es la orden de retener la tarjeta: el emisor la dio con el rechazo (04, 41,
	// 43) o la tarjeta está en la blocklist. Solo si REJECTED.
	CaptureCard bool
}

// GetSessionStatus retorna el estado actual de una sesión por TransactionID.
func (h *SessionQueryHandler) GetSessionStatus(
	ctx context.Context,
	id domain.TransactionID,
) (*SessionStatusResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.GetSessionStatus")
	defer span.End()

	session, err := h.sessionRepo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("GetSessionStatus: %w", err)
	}
	if session == nil {
		return nil, pkgerrors.NewNotFoundError("PaymentSession", id.String())
	}

	result := &SessionStatusResult{
		TransactionID: session.ID().String(),
		TerminalID:    session.TerminalID().String(),
		MerchantID:    session.MerchantID().String(),
		State:         session.State().String(),
		Channel:       session.Channel().String(),
		AmountCents:   session.Amount().Cents(),
		Currency:      session.Amount().Currency().String(),
		CreatedAt:     session.CreatedAt().Format("2006-01-02T15:04:05Z"),
		ExpiresAt:     session.ExpiresAt().Format("2006-01-02T15:04:05Z"),
		TTLSeconds:    int(session.TTLRemaining().Seconds()),
	}

	if session.State() == valueobject.StateApproved {
		result.AuthCode = session.AuthCode()
	}
	if session.State() == valueobject.StateRejected {
		result.RejectionCode = session.RejectionCode()
		result.RejectionReason = session.RejectionReason()
		result.CaptureCard = session.RequiresCardCapture()
	}

	return result, nil
}

// GetActiveSession retorna la sesión activa de un terminal, si existe.
func (h *SessionQueryHandler) GetActiveSession(
	ctx context.Context,
	terminalID domain.TerminalID,
) (*SessionStatusResult, error) {
	ctx, span := observability.StartSpan(ctx, "query.GetActiveSession")
	defer span.End()

	session, err := h.sessionRepo.FindActiveByTerminal(ctx, terminalID)
	if err != nil {
		return nil, fmt.Errorf("GetActiveSession: %w", err)
	}
	if session == nil {
		return nil, nil // Sin sesión activa — no es un error
	}

	return &SessionStatusResult{
		TransactionID: session.ID().String(),
		TerminalID:    session.TerminalID().String(),
		MerchantID:    session.MerchantID().String(),
		State:         session.State().String(),
		Channel:       session.Channel().String(),
		AmountCents:   session.Amount().Cents(),
		Currency:      session.Amount().Currency().String(),
		CreatedAt:     session.CreatedAt().Format("2006-01-02T15:04:05Z"),
		ExpiresAt:     session.ExpiresAt().Format("2006-01-02T15:04:05Z"),
		TTLSeconds:    int(session.TTLRemaining().Seconds()),
	}, nil
}
