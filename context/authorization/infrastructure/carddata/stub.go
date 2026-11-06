// Package carddata contiene las implementaciones de service.CardDataProvider.
package carddata

import (
	"context"
	"strconv"

	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// Tarjetas de prueba públicas: no corresponden a ningún titular real.
var testCards = []service.CardData{
	{PAN: "4242424242424242", ExpiryYYMM: "3012"}, // Visa
	{PAN: "5555555555554444", ExpiryYYMM: "3012"}, // Mastercard
	{PAN: "4000056655665556", ExpiryYYMM: "3012"}, // Visa
}

// Stub reemplaza a card-management-service hasta que exista. Devuelve siempre la misma tarjeta
// de prueba para cada token, y una fija si el pago llegó sin token. No se puede usar con
// ENVIRONMENT=production.
type Stub struct{}

// NewStub crea el stub.
func NewStub() *Stub { return &Stub{} }

// Resolve devuelve una tarjeta de prueba.
func (*Stub) Resolve(_ context.Context, token domain.CardToken) (service.CardData, error) {
	if token.IsZero() {
		return testCards[0], nil
	}
	// El token es hexadecimal: se usa su primer carácter para elegir una tarjeta.
	n, err := strconv.ParseUint(token.String()[:1], 16, 8)
	if err != nil {
		return service.CardData{}, service.ErrCardDataNotFound
	}
	return testCards[int(n)%len(testCards)], nil
}
