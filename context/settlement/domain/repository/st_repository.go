// Package repository define los puertos de salida del BC Settlement.
package repository

import (
	"context"
	"errors"
	"time"

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

// BatchAttentionCounts son los lotes que requieren atención de un operador.
type BatchAttentionCounts struct {
	Disputed     int64 // En DISPUTED
	ClosedUnsent int64 // En CLOSED, con transacciones, desde antes del corte
	OpenOverdue  int64 // En OPEN, de un día de negocio anterior
}

// BatchAttentionCounter cuenta los lotes que requieren atención. Es un puerto aparte
// porque solo lo usan las métricas.
type BatchAttentionCounter interface {
	CountBatchesNeedingAttention(ctx context.Context, today, closedBefore time.Time) (BatchAttentionCounts, error)
}
