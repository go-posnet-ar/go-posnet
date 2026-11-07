package service

import (
	"context"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// EventPublisher es el puerto de salida hacia NATS JetStream.
// Publicar los eventos de dominio de una Transaction al bus de mensajería.
type EventPublisher interface {
	// PublishApproved publica AuthorizationApproved al stream POSNET_AUTH.
	PublishApproved(ctx context.Context, tx *aggregate.Transaction) error

	// PublishRejected publica AuthorizationRejected al stream POSNET_AUTH.
	PublishRejected(ctx context.Context, tx *aggregate.Transaction) error

	// PublishInvalidRejected publica AuthorizationRejected al stream POSNET_AUTH para una
	// transacción con datos inválidos, que no existe como aggregate.
	PublishInvalidRejected(ctx context.Context, rejection InvalidTransactionRejection) error

	// PublishFraudCheckRequested publica FraudCheckRequested al stream POSNET_FRAUD.
	PublishFraudCheckRequested(ctx context.Context, tx *aggregate.Transaction) error

	// PublishReversalCompleted publica ReversalCompleted al stream POSNET_AUTH.
	PublishReversalCompleted(ctx context.Context, txID domain.TransactionID, tx *aggregate.Transaction) error
}

// InvalidTransactionRejection es el rechazo de una transacción con datos inválidos. Solo lleva
// los valores que pasaron la validación: los demás quedan vacíos o en cero. Lo que no se validó
// no se reenvía, porque card_last4 podría traer un PAN completo.
type InvalidTransactionRejection struct {
	TransactionID domain.TransactionID
	TerminalID    string
	MerchantID    string
	AmountCents   int64
	Currency      string
	CardLast4     string
	CardNetwork   string
	EntryMode     string
	Rejection     valueobject.RejectionCode
}
