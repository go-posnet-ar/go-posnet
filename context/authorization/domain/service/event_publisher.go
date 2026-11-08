package service

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// EventPublisher escribe los eventos de integración en el outbox, dentro de dbTx: la transacción
// de base que guarda el cambio que los produce. Así se publican si y solo si ese cambio se
// confirma, aunque NATS esté caído o el proceso muera. Un relay los publica después.
type EventPublisher interface {
	// PublishApproved encola AuthorizationApproved, del stream POSNET_AUTH.
	PublishApproved(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error

	// PublishRejected encola AuthorizationRejected, del stream POSNET_AUTH.
	PublishRejected(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error

	// PublishInvalidRejected encola AuthorizationRejected para una transacción con datos
	// inválidos, que no existe como aggregate.
	PublishInvalidRejected(ctx context.Context, dbTx pgx.Tx, rejection valueobject.InvalidTransactionRejection) error

	// PublishFraudCheckRequested encola FraudCheckRequested, del stream POSNET_FRAUD.
	PublishFraudCheckRequested(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) error

	// PublishReversalCompleted encola ReversalCompleted, del stream POSNET_AUTH.
	PublishReversalCompleted(ctx context.Context, dbTx pgx.Tx, txID domain.TransactionID, tx *aggregate.Transaction) error
}
