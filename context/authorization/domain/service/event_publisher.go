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
	PublishInvalidRejected(ctx context.Context, rejection valueobject.InvalidTransactionRejection) error

	// PublishFraudCheckRequested publica FraudCheckRequested al stream POSNET_FRAUD.
	PublishFraudCheckRequested(ctx context.Context, tx *aggregate.Transaction) error

	// PublishReversalCompleted publica ReversalCompleted al stream POSNET_AUTH.
	PublishReversalCompleted(ctx context.Context, txID domain.TransactionID, tx *aggregate.Transaction) error
}
