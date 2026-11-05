// Package processor contiene los adaptadores hacia el procesador externo de liquidación.
package processor

import (
	"context"
	"log/slog"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
)

// MockProcessor simula el procesador externo para desarrollo: acepta todos los
// lotes sin enviarlos a ningún lado. Solo se activa con SETTLEMENT_PROCESSOR=mock.
type MockProcessor struct{}

// NewMockProcessor construye el procesador simulado.
func NewMockProcessor() *MockProcessor {
	return &MockProcessor{}
}

// Submit acepta el lote. El prefijo MOCK- deja claro en logs que no hubo un envío real.
func (m *MockProcessor) Submit(ctx context.Context, batch *aggregate.SettlementBatch) (string, error) {
	confirmationID := "MOCK-" + batch.ID()
	slog.WarnContext(ctx, "mock processor accepted batch — nothing was sent",
		slog.String("batch_id", batch.ID()),
		slog.String("confirmation_id", confirmationID),
	)
	return confirmationID, nil
}
