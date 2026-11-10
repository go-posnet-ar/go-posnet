package aggregate

import (
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ReconstituteParams contiene todos los campos para reconstruir
// un SettlementBatch desde la capa de persistencia.
type ReconstituteParams struct {
	ID            string
	TerminalID    domain.TerminalID
	MerchantID    domain.MerchantID
	BatchDate     time.Time
	State         valueobject.BatchState
	Currency      string
	Transactions  []*entity.BatchTransaction
	Summary       *valueobject.BatchSummary
	Discrepancies int
	Version       int
	CreatedAt     time.Time
	ClosedAt      *time.Time
	SubmittedAt   *time.Time
	SettledAt     *time.Time

	ReconciledWith string // ReconciledWithTerminal, ReconciledWithBackend o vacío
	TerminalCount  *int
	TerminalAmount *int64
}

// Reconstitute reconstruye un SettlementBatch desde Postgres.
// No emite eventos de dominio ni valida invariantes de creación.
func Reconstitute(p ReconstituteParams) *SettlementBatch {
	return &SettlementBatch{
		id:            p.ID,
		terminalID:    p.TerminalID,
		merchantID:    p.MerchantID,
		batchDate:     p.BatchDate,
		state:         p.State,
		currency:      p.Currency,
		transactions:  p.Transactions,
		summary:       p.Summary,
		discrepancies: p.Discrepancies,
		version:       p.Version,
		createdAt:     p.CreatedAt,
		closedAt:      p.ClosedAt,
		submittedAt:   p.SubmittedAt,
		settledAt:     p.SettledAt,

		reconciledWith: p.ReconciledWith,
		terminalCount:  p.TerminalCount,
		terminalAmount: p.TerminalAmount,
	}
}
