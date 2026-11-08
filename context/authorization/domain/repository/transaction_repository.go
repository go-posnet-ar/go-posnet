// Package repository define los puertos de salida (interfaces) del BC Authorization
// hacia la capa de persistencia. El dominio depende de estas interfaces;
// la infraestructura (Postgres) las implementa.
package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// TransactionRepository es el puerto de salida principal del BC Authorization.
// Define las operaciones de persistencia del aggregate Transaction.
//
// REGLA: esta interface está en el dominio. El adaptador Postgres
// en infrastructure/postgres/repository.go la implementa.
type TransactionRepository interface {
	// InsertTx da de alta una Transaction dentro de dbTx. Si ya existe ese ID, no cambia nada y
	// devuelve false.
	InsertTx(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction) (bool, error)

	// SaveInvalidRejection registra el rechazo de una transacción con datos inválidos. Si ya
	// está registrado, no cambia nada.
	SaveInvalidRejection(ctx context.Context, rejection valueobject.InvalidTransactionRejection) error

	// FindInvalidRejection recupera ese registro. Devuelve nil si no existe.
	FindInvalidRejection(ctx context.Context, id domain.TransactionID) (*valueobject.InvalidTransactionRejection, error)

	// DeleteInvalidRejectionsBefore borra esos registros, si son anteriores a cutoff, y devuelve
	// cuántos borró.
	DeleteInvalidRejectionsBefore(ctx context.Context, cutoff time.Time) (int64, error)

	// FindByID recupera una Transaction por su ID.
	// Retorna ErrNotFound si no existe.
	FindByID(ctx context.Context, id domain.TransactionID) (*aggregate.Transaction, error)

	// ExistsByID verifica si una transacción con ese ID ya fue persistida.
	// Usado para idempotencia: si ya existe, no volver a procesar.
	ExistsByID(ctx context.Context, id domain.TransactionID) (bool, error)

	// FindByIDForUpdateTx recupera la transacción dentro de dbTx y la bloquea hasta su fin.
	FindByIDForUpdateTx(ctx context.Context, dbTx pgx.Tx, id domain.TransactionID) (*aggregate.Transaction, error)

	// SaveIfState guarda el estado y el resultado de la transacción solo si en la base sigue
	// en expected. Devuelve false, sin cambiar nada, si otro proceso ya la movió.
	SaveIfState(ctx context.Context, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error)

	// SaveIfStateTx hace lo mismo dentro de una transacción existente.
	SaveIfStateTx(ctx context.Context, dbTx pgx.Tx, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error)

	// FindUnresolved devuelve las transacciones en INDETERMINATE y las que están en
	// PROCESSING desde antes de processingBefore, de la más antigua a la más nueva.
	FindUnresolved(ctx context.Context, processingBefore time.Time, limit int) ([]*aggregate.Transaction, error)

	// MarkReversalRequestedTx guarda el pedido de reversa dentro de dbTx. Si ya había uno,
	// conserva su fecha.
	MarkReversalRequestedTx(ctx context.Context, dbTx pgx.Tx, id domain.TransactionID) error

	// FindPendingReversals devuelve las transacciones en APPROVED con un pedido de reversa
	// anterior a requestedBefore, de la más antigua a la más nueva.
	FindPendingReversals(ctx context.Context, requestedBefore time.Time, limit int) ([]*aggregate.Transaction, error)

	// CountUnresolved cuenta las transacciones sin resolver, según los mismos cortes.
	CountUnresolved(ctx context.Context, processingBefore, reversalsBefore time.Time) (UnresolvedCounts, error)
}

// UnresolvedCounts son las transacciones que el conciliador tiene pendientes.
type UnresolvedCounts struct {
	Indeterminate   int64 // En INDETERMINATE
	Processing      int64 // En PROCESSING desde antes del corte
	ReversalPending int64 // En APPROVED, con un pedido de reversa anterior al corte
}
