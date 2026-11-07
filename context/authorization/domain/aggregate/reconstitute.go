package aggregate

import (
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ReconstituteParams contiene todos los campos necesarios para reconstruir
// un aggregate Transaction desde la capa de persistencia.
// Separa el constructor de creación (NewTransaction) del de reconstitución.
type ReconstituteParams struct {
	ID              domain.TransactionID
	TerminalID      domain.TerminalID
	MerchantID      domain.MerchantID
	Amount          domain.Money
	STAN            domain.STAN
	PAN             domain.PAN
	EntryMode       valueobject.EntryMode
	CardToken       domain.CardToken
	State           valueobject.TransactionState
	FraudDecision   valueobject.FraudDecision
	AuthCode        *string
	RejectionCode   *string
	RejectionSource *string
	EMVDataBase64   string
	ISO8583Raw      []byte
	ReceivedAt      time.Time
	AuthorizedAt    *time.Time
	RejectedAt      *time.Time

	ReversalRequestedAt *time.Time
}

// Reconstitute reconstruye un aggregate Transaction desde la base de datos.
// A diferencia de NewTransaction, NO emite eventos de dominio ni valida
// invariantes de creación — asume que los datos de Postgres son consistentes.
func Reconstitute(p ReconstituteParams) *Transaction {
	tx := &Transaction{
		id:            p.ID,
		terminalID:    p.TerminalID,
		merchantID:    p.MerchantID,
		amount:        p.Amount,
		stan:          p.STAN,
		pan:           p.PAN,
		entryMode:     p.EntryMode,
		cardToken:     p.CardToken,
		state:         p.State,
		fraudDecision: p.FraudDecision,
		emvDataBase64: p.EMVDataBase64,
		iso8583Raw:    p.ISO8583Raw,
		receivedAt:    p.ReceivedAt,
		authorizedAt:  p.AuthorizedAt,
		rejectedAt:    p.RejectedAt,

		reversalRequestedAt: p.ReversalRequestedAt,
	}

	if p.AuthCode != nil {
		ac, err := domain.NewAuthCode(*p.AuthCode)
		if err == nil {
			tx.authCode = &ac
		}
	}

	if p.RejectionCode != nil {
		var source string
		if p.RejectionSource != nil {
			source = *p.RejectionSource
		}
		rc := valueobject.RestoreRejectionCode(*p.RejectionCode, source)
		tx.rejectionCode = &rc
	}

	return tx
}
