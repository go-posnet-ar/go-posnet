// Package service contiene los puertos de salida del BC Settlement.
package service

import (
	"context"
	"errors"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
)

// ErrProcessorUnavailable indica que no hay un procesador externo configurado.
// El lote no se envió y permanece en CLOSED hasta que un operador lo reenvíe.
var ErrProcessorUnavailable = errors.New("settlement processor unavailable")

// EventPublisher es el puerto de salida hacia NATS JetStream.
type EventPublisher interface {
	// PublishBatchClosed publica el evento al stream POSNET_SETTLEMENT.
	// Consumido por: Notification BC.
	PublishBatchClosed(ctx context.Context, batch *aggregate.SettlementBatch) error

	// PublishSettlementCompleted publica el resumen de liquidación diaria.
	// Consumido por: Notification BC.
	PublishSettlementCompleted(ctx context.Context, merchantID, settlementDate string, totalBatches int, totalAmount, netAmount int64, currency string, mdrPercent float64) error
}

// SettlementProcessor es el puerto de salida hacia el procesador externo
// (Visa/Mastercard) para el envío del archivo de remesa.
type SettlementProcessor interface {
	// Submit envía el archivo de remesa del batch al procesador.
	// Retorna el ID de confirmación del procesador.
	Submit(ctx context.Context, batch *aggregate.SettlementBatch) (string, error)
}
