// Package repository define los puertos de salida del BC Terminal Gateway.
package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/entity"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// PaymentSessionRepository persiste y recupera sesiones de pago activas.
type PaymentSessionRepository interface {
	// Save persiste una sesión nueva o actualiza una existente (UPSERT).
	Save(ctx context.Context, session *aggregate.PaymentSession) error

	// SaveTx persiste la sesión dentro de una transacción Postgres existente.
	// Usar cuando la persistencia debe ser atómica con otra operación (ej: outbox).
	SaveTx(ctx context.Context, tx pgx.Tx, session *aggregate.PaymentSession) error

	// FindByID recupera una sesión por su TransactionID.
	FindByID(ctx context.Context, id domain.TransactionID) (*aggregate.PaymentSession, error)

	// FindActiveByTerminal recupera la sesión activa de un terminal.
	// Retorna nil si el terminal no tiene sesión activa (estado AWAITING o PROCESSING).
	FindActiveByTerminal(ctx context.Context, terminalID domain.TerminalID) (*aggregate.PaymentSession, error)

	// ExpireAwaiting pasa a EXPIRED hasta limit sesiones vencidas en AWAITING_PAYMENT, y devuelve
	// cuáles. Una sesión en PROCESSING no vence: espera el resultado de Authorization.
	ExpireAwaiting(ctx context.Context, limit int) ([]ExpiredSession, error)
}

// ExpiredSession identifica una sesión que acaba de vencer, para avisarle a su terminal.
type ExpiredSession struct {
	TransactionID domain.TransactionID
	TerminalID    domain.TerminalID
}

// TerminalRepository persiste y recupera terminales registrados.
type TerminalRepository interface {
	// FindByID recupera un terminal por su ID.
	FindByID(ctx context.Context, id domain.TerminalID) (*entity.Terminal, error)

	// FindByCertificateCN recupera un terminal por el CN de su certificado mTLS.
	// Usado durante el handshake de autenticación WebSocket.
	FindByCertificateCN(ctx context.Context, cn string) (*entity.Terminal, error)

	// Save persiste un terminal nuevo o actualiza uno existente.
	Save(ctx context.Context, terminal *entity.Terminal) error
}
