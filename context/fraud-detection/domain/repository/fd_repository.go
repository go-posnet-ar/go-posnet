// Package repository define los puertos de salida del BC Fraud Detection.
package repository

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// FraudCaseRepository persiste y recupera FraudCases.
type FraudCaseRepository interface {
	// Save persiste un FraudCase nuevo dentro de dbTx, la misma transacción que marca el evento
	// como procesado y encola el score. Si la transacción ya tiene un caso, no hace nada y
	// devuelve false.
	Save(ctx context.Context, dbTx pgx.Tx, fc *aggregate.FraudCase) (inserted bool, err error)

	// FindByTransactionIDTx es FindByTransactionID dentro de dbTx.
	FindByTransactionIDTx(ctx context.Context, dbTx pgx.Tx, txID domain.TransactionID) (*aggregate.FraudCase, error)

	// FindByTransactionID recupera el FraudCase de una transacción.
	// Retorna nil si no existe — una transacción puede no haber sido evaluada.
	FindByTransactionID(ctx context.Context, txID domain.TransactionID) (*aggregate.FraudCase, error)
}

// FraudRuleRepository carga las reglas de fraude activas desde Postgres.
// Las reglas se cachean en memoria al arrancar y se recargan periódicamente.
type FraudRuleRepository interface {
	// FindAllActive retorna todas las reglas activas ordenadas por score_weight DESC.
	FindAllActive(ctx context.Context) ([]*entity.FraudRule, error)

	// Save persiste una regla nueva o actualiza una existente.
	Save(ctx context.Context, rule *entity.FraudRule) error
}

// TransactionHistoryRepository consulta el historial de transacciones
// de un terminal para las reglas de velocidad y comportamiento.
// Solo lectura — no persiste nada en este BC.
type TransactionHistoryRepository interface {
	// CountByTerminalLastHour retorna la cantidad de transacciones del terminal
	// en la última hora. Usado por la regla de velocity check.
	CountByTerminalLastHour(ctx context.Context, terminalID domain.TerminalID) (int, error)

	// AverageAmountByMerchant retorna el monto promedio de transacciones
	// del comercio en los últimos 30 días. Usado por la regla de monto inusual.
	AverageAmountByMerchant(ctx context.Context, merchantID domain.MerchantID) (int64, error)

	// CountRecentRejectionsByTerminal retorna rechazos recientes del terminal
	// en los últimos N minutos. Usado por la regla de múltiples rechazos.
	CountRecentRejectionsByTerminal(ctx context.Context, terminalID domain.TerminalID, lastMinutes int) (int, error)

	// CountSameAmountAttempts retorna intentos con el mismo monto exacto
	// en el mismo terminal en los últimos N minutos.
	CountSameAmountAttempts(ctx context.Context, terminalID domain.TerminalID, amountCents int64, lastMinutes int) (int, error)
}
