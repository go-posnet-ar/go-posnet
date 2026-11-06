package service

import (
	"context"
	"errors"
	"log/slog"

	"github.com/juantevez/go-posnet/pkg/domain"
)

// ErrCardDataNotFound indica que el proveedor no conoce la tarjeta.
var ErrCardDataNotFound = errors.New("card data not found")

// CardData son los datos sensibles de una tarjeta. Solo existen en memoria, durante el armado
// de un mensaje al adquirente: nunca se guardan, ni viajan por eventos, ni van a los logs.
type CardData struct {
	PAN        string
	ExpiryYYMM string // Vencimiento, como en ISO 8583 DE-14
}

const redactedCardData = "CardData{REDACTED}"

// String evita que el PAN llegue a un log por accidente.
func (CardData) String() string { return redactedCardData }

// GoString evita lo mismo con el verbo %#v.
func (CardData) GoString() string { return redactedCardData }

// LogValue evita lo mismo con slog.
func (CardData) LogValue() slog.Value { return slog.StringValue(redactedCardData) }

// CardDataProvider resuelve un token en los datos de la tarjeta. El adaptador del adquirente es
// el único que lo usa, y el resto del sistema solo conoce el token. Hoy lo implementa un stub
// con tarjetas de prueba; después, un cliente de card-management-service.
type CardDataProvider interface {
	// Resolve devuelve los datos de la tarjeta, o ErrCardDataNotFound. El token puede ser cero
	// si el pago llegó sin él: qué hacer en ese caso lo decide el proveedor.
	Resolve(ctx context.Context, token domain.CardToken) (CardData, error)
}
