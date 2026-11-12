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

// ErrSubmissionRejected indica que el procesador rechazó el lote por su contenido. No se reintenta:
// el mismo lote daría el mismo rechazo. El adaptador lo envuelve con el motivo.
var ErrSubmissionRejected = errors.New("settlement processor rejected the batch")

// EventPublisher arma los eventos del BC. No los publica: el handler los guarda en el
// outbox, en la misma transacción que el batch, y el Relay los envía a JetStream.
type EventPublisher interface {
	// BuildBatchClosed serializa el evento BatchClosed. Consumido por: Notification BC. trigger es
	// uno de los events.BatchClose*: por qué se emite.
	BuildBatchClosed(ctx context.Context, batch *aggregate.SettlementBatch, trigger string) (subject, eventID string, payload []byte, err error)

	// BuildSettlementCompleted serializa el resumen de liquidación diaria.
	BuildSettlementCompleted(ctx context.Context, c SettlementCompletion) (subject, eventID string, payload []byte, err error)
}

// SettlementCompletion es una revisión del resumen de liquidación de un comercio, un día y una
// moneda. La primera sale cuando se liquidan todos los lotes del día; cada lote que se liquida
// después da otra, con el acumulado y la diferencia con la anterior.
type SettlementCompletion struct {
	MerchantID     string
	SettlementDate string // YYYY-MM-DD
	Currency       string
	MDRPercent     float64

	// Revision es 1 en el primer resumen del día y crece en cada uno siguiente.
	Revision int

	// Acumulado del día, con todas las revisiones. TotalAmount incluye los ajustes, y
	// AdjustmentAmount es su suma, con signo.
	TotalBatches     int
	TotalAmount      int64
	AdjustmentAmount int64
	NetAmount        int64

	// Lo que agrega esta revisión respecto de la anterior. En la primera, igual al acumulado.
	DeltaBatches   int
	DeltaAmount    int64
	DeltaNetAmount int64
}

// SettlementProcessor es el puerto de salida hacia el procesador externo
// (Visa/Mastercard) para el envío del archivo de remesa.
type SettlementProcessor interface {
	// Submit envía el archivo de remesa del batch al procesador.
	// Retorna el ID de confirmación del procesador.
	//
	// Tiene que ser idempotente por batch.ID(): el adaptador lo envía como clave de idempotencia,
	// y el procesador responde la confirmación del envío anterior en vez de aceptarlo otra vez.
	// Settlement no repite un envío con confirmación registrada, pero sí uno cuyo resultado no
	// conoce: un error, un timeout o un corte después de enviarlo.
	Submit(ctx context.Context, batch *aggregate.SettlementBatch) (string, error)
}
