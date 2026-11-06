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

// EventPublisher arma los eventos del BC. No los publica: el handler los guarda en el
// outbox, en la misma transacción que el batch, y el Relay los envía a JetStream.
type EventPublisher interface {
	// BuildBatchClosed serializa el evento BatchClosed. Consumido por: Notification BC.
	BuildBatchClosed(ctx context.Context, batch *aggregate.SettlementBatch) (subject, eventID string, payload []byte, err error)

	// BuildSettlementCompleted serializa el resumen de liquidación diaria.
	BuildSettlementCompleted(ctx context.Context, merchantID, settlementDate string, totalBatches int, totalAmount, netAmount int64, currency string, mdrPercent float64) (subject, eventID string, payload []byte, err error)
}

// SettlementProcessor es el puerto de salida hacia el procesador externo
// (Visa/Mastercard) para el envío del archivo de remesa.
type SettlementProcessor interface {
	// Submit envía el archivo de remesa del batch al procesador.
	// Retorna el ID de confirmación del procesador.
	Submit(ctx context.Context, batch *aggregate.SettlementBatch) (string, error)
}
