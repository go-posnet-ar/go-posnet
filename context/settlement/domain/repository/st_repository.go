// Package repository define los puertos de salida del BC Settlement.
package repository

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ErrStaleBatch indica que el batch cambió desde que se cargó. Nada se guardó: hay
// que volver a cargarlo y reintentar.
var ErrStaleBatch = errors.New("settlement batch was modified concurrently")

// SettlementBatchRepository persiste y recupera SettlementBatches.
type SettlementBatchRepository interface {
	// Save persiste un batch nuevo o actualiza uno existente (UPSERT).
	// Retorna ErrStaleBatch si otro proceso lo modificó desde que se cargó.
	Save(ctx context.Context, batch *aggregate.SettlementBatch) error

	// SaveTx hace lo mismo dentro de una transacción existente, para guardar el batch
	// junto con su evento en el outbox. Si la transacción no confirma, hay que descartar
	// el batch y volver a cargarlo: su versión en memoria ya avanzó.
	SaveTx(ctx context.Context, tx pgx.Tx, batch *aggregate.SettlementBatch) error

	// MerchantDayBatchesTx devuelve los batches con transacciones de un comercio en una
	// fecha. Espera a que terminen las demás llamadas por ese comercio y fecha, y mantiene
	// la exclusión hasta el fin de tx: quien llama ve lo que confirmaron las anteriores.
	MerchantDayBatchesTx(ctx context.Context, tx pgx.Tx, merchantID domain.MerchantID, date time.Time) ([]MerchantDayBatch, error)

	// FindByID recupera un batch por su ID.
	FindByID(ctx context.Context, id string) (*aggregate.SettlementBatch, error)

	// FindOpenByTerminal recupera el batch OPEN del terminal en una fecha y moneda.
	// Retorna nil si no existe — se creará uno nuevo al recibir la primera transacción.
	FindOpenByTerminal(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency string) (*aggregate.SettlementBatch, error)

	// FindOrCreateOpen recupera el batch OPEN del terminal o lo crea si no existe.
	// Garantiza que hay un solo batch OPEN por terminal, día y moneda.
	FindOrCreateOpen(ctx context.Context, terminalID domain.TerminalID, merchantID domain.MerchantID, date time.Time, currency string) (*aggregate.SettlementBatch, error)

	// ListByMerchantDate lista todos los batches de un comercio en una fecha.
	// Usado para generar el reporte de liquidación diaria.
	ListByMerchantDate(ctx context.Context, merchantID domain.MerchantID, date time.Time) ([]*aggregate.SettlementBatch, error)

	// FindStaleOpenIDs lista los batches OPEN de fechas anteriores a before.
	// Usado por el job de cierre automático.
	FindStaleOpenIDs(ctx context.Context, before time.Time) ([]string, error)
}

// MerchantDayBatch resume un batch para decidir si se completó la liquidación diaria.
type MerchantDayBatch struct {
	Currency    string
	Settled     bool
	TotalAmount int64
}

// BatchAttentionCounts son los lotes que requieren atención de un operador.
type BatchAttentionCounts struct {
	Disputed     int64 // En DISPUTED
	ClosedUnsent int64 // En CLOSED, con transacciones, desde antes del corte
	OpenOverdue  int64 // En OPEN, de un día de negocio anterior
	OutboxStuck  int64 // Eventos en el outbox desde antes del corte
}

// BatchAttentionCounter cuenta los lotes que requieren atención. Es un puerto aparte
// porque solo lo usan las métricas.
type BatchAttentionCounter interface {
	CountBatchesNeedingAttention(ctx context.Context, today, closedBefore, outboxBefore time.Time) (BatchAttentionCounts, error)
}
