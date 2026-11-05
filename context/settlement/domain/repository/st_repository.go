// Package repository define los puertos de salida del BC Settlement.
package repository

import (
	"context"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// SettlementBatchRepository persiste y recupera SettlementBatches.
type SettlementBatchRepository interface {
	// Save persiste un batch nuevo o actualiza uno existente (UPSERT).
	Save(ctx context.Context, batch *aggregate.SettlementBatch) error

	// FindByID recupera un batch por su ID.
	FindByID(ctx context.Context, id string) (*aggregate.SettlementBatch, error)

	// FindOpenByTerminal recupera el batch OPEN del terminal en una fecha dada.
	// Retorna nil si no existe — se creará uno nuevo al recibir la primera transacción.
	FindOpenByTerminal(ctx context.Context, terminalID domain.TerminalID, date time.Time) (*aggregate.SettlementBatch, error)

	// FindOrCreateOpen recupera el batch OPEN del terminal o lo crea si no existe.
	// Garantiza que siempre hay exactamente un batch OPEN por terminal por día.
	FindOrCreateOpen(ctx context.Context, terminalID domain.TerminalID, merchantID domain.MerchantID, date time.Time, currency string) (*aggregate.SettlementBatch, error)

	// ListByMerchantDate lista todos los batches de un comercio en una fecha.
	// Usado para generar el reporte de liquidación diaria.
	ListByMerchantDate(ctx context.Context, merchantID domain.MerchantID, date time.Time) ([]*aggregate.SettlementBatch, error)

	// FindStaleOpenIDs lista los batches OPEN de fechas anteriores a before.
	// Usado por el job de cierre automático.
	FindStaleOpenIDs(ctx context.Context, before time.Time) ([]string, error)
}
