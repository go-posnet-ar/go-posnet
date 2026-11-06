package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
)

// Montos del modo de escenarios, en centavos. Están fuera de los que usan las pruebas para
// que una venta normal no caiga en uno por casualidad.
const (
	MockAmountDecline      = 770051 // Rechazo del emisor: 51, fondos insuficientes
	MockAmountCaptureCard  = 770041 // Rechazo con retención: 41, tarjeta perdida
	MockAmountError        = 770091 // Error del adquirente, sin respuesta
	MockAmountHang         = 770098 // No responde hasta que venza el timeout de la llamada
	MockAmountReverseFails = 770092 // Aprueba, y sus primeras reversas fallan
)

// mockReverseFailures es cuántas reversas fallan antes de que una funcione.
const mockReverseFailures = 2

// MockAcquirerGateway es un mock del adquirente para desarrollo y MVP. Aprueba siempre con un
// auth code derivado del STAN, salvo que se activen los escenarios: permiten probar en el
// stack completo los caminos que un host real provoca. Reemplazar por la implementación ISO
// 8583 real en producción.
type MockAcquirerGateway struct {
	scenarios bool

	mu             sync.Mutex
	reverseAttempt map[string]int
}

// NewMockAcquirerGateway crea el mock. Con scenarios, ciertos montos no se aprueban.
func NewMockAcquirerGateway(scenarios bool) *MockAcquirerGateway {
	return &MockAcquirerGateway{scenarios: scenarios, reverseAttempt: map[string]int{}}
}

// Authorize aprueba, o responde lo que indique el escenario del monto.
// AuthCode: 6 chars alfanuméricos uppercase — formato ISO 8583 DE-38.
// Usamos los últimos 6 dígitos del STAN con padding para garantizar exactamente 6 chars.
func (m *MockAcquirerGateway) Authorize(
	ctx context.Context,
	tx *aggregate.Transaction,
) (service.AcquirerResponse, error) {
	if m.scenarios {
		switch tx.Amount().Cents() {
		case MockAmountDecline:
			return service.AcquirerResponse{ResponseCode: valueobject.ISO_INSUFFICIENT_FUNDS, RespondedAt: time.Now().UTC()}, nil
		case MockAmountCaptureCard:
			return service.AcquirerResponse{ResponseCode: valueobject.ISO_LOST_CARD, RespondedAt: time.Now().UTC()}, nil
		case MockAmountError:
			return service.AcquirerResponse{}, errors.New("mock acquirer: connection reset")
		case MockAmountHang:
			<-ctx.Done()
			return service.AcquirerResponse{}, fmt.Errorf("mock acquirer: no response: %w", ctx.Err())
		}
	}
	return service.AcquirerResponse{
		ResponseCode: valueobject.ISO_APPROVED, // "00"
		AuthCode:     fmt.Sprintf("%06d", tx.STAN().Value()%1000000),
		ARPCBase64:   "",
		RespondedAt:  time.Now().UTC(),
	}, nil
}

// Reverse tiene éxito, salvo en las primeras reversas del escenario que las hace fallar.
func (m *MockAcquirerGateway) Reverse(
	_ context.Context,
	tx *aggregate.Transaction,
) error {
	if !m.scenarios || tx.Amount().Cents() != MockAmountReverseFails {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := tx.ID().String()
	if m.reverseAttempt[id] < mockReverseFailures {
		m.reverseAttempt[id]++
		return errors.New("mock acquirer: reversal failed")
	}
	delete(m.reverseAttempt, id)
	return nil
}
