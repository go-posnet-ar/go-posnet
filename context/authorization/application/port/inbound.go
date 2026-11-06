// Package port define los puertos de entrada (inbound) del BC Authorization.
// Son las interfaces que los adaptadores HTTP/NATS llaman para
// iniciar los casos de uso del dominio.
package port

import (
	"context"

	"github.com/juantevez/go-posnet/pkg/domain"
)

// AuthorizationService es el puerto de entrada principal.
// El adaptador NATS lo llama al consumir TransactionReceived.
type AuthorizationService interface {
	// AuthorizeTransaction procesa el evento TransactionReceived completo.
	// Orquesta: idempotencia → fraud check → acquirer → publicar resultado.
	AuthorizeTransaction(ctx context.Context, cmd AuthorizeTransactionCommand) error

	// ProcessReversal procesa el evento ReversalRequested.
	ProcessReversal(ctx context.Context, cmd ProcessReversalCommand) error

	// ApplyFraudScore procesa el evento FraudScoreCalculated.
	// Continúa la Saga de autorización con el score recibido.
	ApplyFraudScore(ctx context.Context, cmd ApplyFraudScoreCommand) error
}

// QueryService es el puerto de entrada para consultas (CQRS — lado Q).
type QueryService interface {
	// GetTransactionStatus retorna el estado actual de una transacción.
	GetTransactionStatus(ctx context.Context, id domain.TransactionID) (*TransactionStatusResult, error)
}

// ─── Commands ────────────────────────────────────────────────────────────────

// AuthorizeTransactionCommand contiene todos los datos necesarios
// para procesar una nueva transacción recibida del terminal.
type AuthorizeTransactionCommand struct {
	// EventID es el ID del evento NATS — usado para idempotencia.
	EventID string

	TransactionID string
	TerminalID    string
	MerchantID    string
	AmountCents   int64
	Currency      string
	STAN          int
	EntryMode     string
	CardLast4     string
	CardNetwork   string
	CardToken     string // HMAC del PAN derivado en el borde; vacío si no se emite
	EMVDataBase64 string
	ISO8583Raw    []byte
	ReceivedAt    string
}

// ProcessReversalCommand contiene los datos del evento ReversalRequested.
type ProcessReversalCommand struct {
	EventID               string
	OriginalTransactionID string
	TerminalID            string
	MerchantID            string
	AmountCents           int64
	Currency              string
	OriginalAuthCode      string
}

// ApplyFraudScoreCommand contiene el resultado del motor antifraude.
type ApplyFraudScoreCommand struct {
	EventID       string
	TransactionID string
	Score         int
	Decision      string
	RulesHit      []string
}

// ─── Query Results ────────────────────────────────────────────────────────────

// TransactionStatusResult es el resultado de la query de estado.
type TransactionStatusResult struct {
	TransactionID   string
	State           string
	AuthCode        string // vacío si no fue aprobada
	RejectionCode   string // vacío si fue aprobada
	RejectionReason string
	AmountCents     int64
	Currency        string
	AuthorizedAt    string
	RejectedAt      string

	// ReversalRequestedAt indica un pedido de reversa sin completar: la transacción sigue
	// vigente hasta que pase a REVERSED. Vacío si no hay ninguno.
	ReversalRequestedAt string
}
