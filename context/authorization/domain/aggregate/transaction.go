// Package aggregate contiene los Aggregates del BC Authorization.
// El Aggregate garantiza las invariantes de negocio del dominio.
package aggregate

import (
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/event"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// Transaction es el Aggregate Root del BC Authorization.
// Representa el ciclo de vida completo de una transacción desde
// que es recibida hasta que es autorizada, rechazada o revertida.
//
// REGLA: solo se modifica el estado a través de métodos del aggregate.
// Nunca se asigna directamente a los campos.
type Transaction struct {
	// Identidad
	id         domain.TransactionID
	terminalID domain.TerminalID
	merchantID domain.MerchantID

	// Datos financieros
	amount domain.Money
	stan   domain.STAN
	pan    domain.PAN

	// cardToken identifica la tarjeta entre transacciones sin exponer el PAN.
	// Se persiste porque la Saga es asíncrona: cuando llega la respuesta del
	// adquirente el aggregate ya fue releído desde Postgres, y sin el token
	// no habría con qué bloquear la tarjeta que el emisor mandó retener.
	// Puede ser cero: un terminal que todavía no emite token.
	cardToken domain.CardToken

	// Modo de captura
	entryMode valueobject.EntryMode

	// Estado del proceso
	state         valueobject.TransactionState
	fraudDecision valueobject.FraudDecision

	// Resultado
	authCode      *domain.AuthCode           // Solo si APPROVED
	rejectionCode *valueobject.RejectionCode // Solo si REJECTED

	// Datos EMV — opacos para el dominio, reenviados al adquirente
	emvDataBase64 string
	iso8583Raw    []byte

	// Timestamps
	receivedAt   time.Time
	authorizedAt *time.Time
	rejectedAt   *time.Time

	// reversalRequestedAt indica desde cuándo hay un pedido de reversa sin completar.
	reversalRequestedAt *time.Time

	// Eventos de dominio pendientes de publicar
	// Se limpian después de que el adaptador los publica a NATS.
	domainEvents []event.DomainEvent
}

// NewTransaction crea una nueva Transaction en estado RECEIVED.
// Valida todas las invariantes antes de retornar.
func NewTransaction(
	id domain.TransactionID,
	terminalID domain.TerminalID,
	merchantID domain.MerchantID,
	amount domain.Money,
	stan domain.STAN,
	pan domain.PAN,
	entryMode valueobject.EntryMode,
	cardToken domain.CardToken,
	emvDataBase64 string,
	iso8583Raw []byte,
) (*Transaction, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("transaction: id cannot be zero")
	}
	if terminalID.IsZero() {
		return nil, fmt.Errorf("transaction: terminal_id cannot be zero")
	}
	if merchantID.IsZero() {
		return nil, fmt.Errorf("transaction: merchant_id cannot be zero")
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("transaction: amount must be positive")
	}

	t := &Transaction{
		id:            id,
		terminalID:    terminalID,
		merchantID:    merchantID,
		amount:        amount,
		stan:          stan,
		pan:           pan,
		entryMode:     entryMode,
		cardToken:     cardToken,
		state:         valueobject.StateReceived,
		emvDataBase64: emvDataBase64,
		iso8583Raw:    iso8583Raw,
		receivedAt:    time.Now().UTC(),
	}

	t.record(event.NewTransactionCreated(t.id, t.terminalID, t.merchantID, t.amount, t.stan))
	return t, nil
}

// ─── Transiciones de estado ───────────────────────────────────────────────────

// StartFraudCheck transiciona a FRAUD_CHECKING.
// Llamado cuando se publica el FraudCheckRequested a NATS.
func (t *Transaction) StartFraudCheck() error {
	if err := t.transition(valueobject.StateFraudChecking); err != nil {
		return err
	}
	t.record(event.NewFraudCheckStarted(t.id))
	return nil
}

// ApplyFraudDecision registra el resultado del motor antifraude.
// Si el score es REJECT, transiciona directamente a REJECTED.
func (t *Transaction) ApplyFraudDecision(fd valueobject.FraudDecision) error {
	if t.state != valueobject.StateFraudChecking {
		return fmt.Errorf("transaction: cannot apply fraud decision in state %s", t.state)
	}
	t.fraudDecision = fd

	if fd.ShouldReject() {
		rc := valueobject.NewRejectionFromFraud()
		return t.reject(rc)
	}

	// Score APPROVE o REVIEW → continuar con el adquirente
	return t.transition(valueobject.StateProcessing)
}

// ExpireFraudCheck rechaza la transacción porque Fraud Detection no respondió a tiempo. No se
// sigue sin el score: aprobar sin evaluar el riesgo dejaría pasar justo lo que el motor frena.
func (t *Transaction) ExpireFraudCheck() error {
	if t.state != valueobject.StateFraudChecking {
		return fmt.Errorf("transaction: cannot expire fraud check in state %s", t.state)
	}
	return t.reject(valueobject.NewRejectionFromFraudTimeout())
}

// Approve transiciona a APPROVED con el código de autorización del emisor.
func (t *Transaction) Approve(authCode domain.AuthCode) error {
	if err := t.transition(valueobject.StateApproved); err != nil {
		return err
	}
	t.authCode = &authCode
	now := time.Now().UTC()
	t.authorizedAt = &now

	t.record(event.NewTransactionApproved(t.id, t.terminalID, t.merchantID, t.amount, t.pan, authCode, t.fraudDecision.Score))
	return nil
}

// Reject transiciona a REJECTED con el código de rechazo.
func (t *Transaction) Reject(rc valueobject.RejectionCode) error {
	return t.reject(rc)
}

// MarkIndeterminate transiciona a INDETERMINATE (timeout del adquirente).
// El estado de la transacción en el banco emisor es desconocido.
func (t *Transaction) MarkIndeterminate() error {
	if err := t.transition(valueobject.StateIndeterminate); err != nil {
		return err
	}
	t.record(event.NewTransactionIndeterminate(t.id, t.terminalID))
	return nil
}

// Reverse transiciona a REVERSED (anulación exitosa).
func (t *Transaction) Reverse() error {
	if err := t.transition(valueobject.StateReversed); err != nil {
		return err
	}
	t.record(event.NewTransactionReversed(t.id, t.terminalID, t.merchantID, t.amount))
	return nil
}

// ─── Helpers internos ────────────────────────────────────────────────────────

func (t *Transaction) reject(rc valueobject.RejectionCode) error {
	if err := t.transition(valueobject.StateRejected); err != nil {
		return err
	}
	t.rejectionCode = &rc
	now := time.Now().UTC()
	t.rejectedAt = &now

	t.record(event.NewTransactionRejected(t.id, t.terminalID, t.merchantID, rc))
	return nil
}

func (t *Transaction) transition(next valueobject.TransactionState) error {
	if !t.state.CanTransitionTo(next) {
		return fmt.Errorf("transaction %s: invalid state transition %s → %s",
			t.id, t.state, next)
	}
	t.state = next
	return nil
}

func (t *Transaction) record(e event.DomainEvent) {
	t.domainEvents = append(t.domainEvents, e)
}

// ─── Getters (solo lectura) ───────────────────────────────────────────────────

func (t *Transaction) ID() domain.TransactionID                 { return t.id }
func (t *Transaction) TerminalID() domain.TerminalID            { return t.terminalID }
func (t *Transaction) MerchantID() domain.MerchantID            { return t.merchantID }
func (t *Transaction) Amount() domain.Money                     { return t.amount }
func (t *Transaction) STAN() domain.STAN                        { return t.stan }
func (t *Transaction) PAN() domain.PAN                          { return t.pan }
func (t *Transaction) EntryMode() valueobject.EntryMode         { return t.entryMode }
func (t *Transaction) State() valueobject.TransactionState      { return t.state }
func (t *Transaction) FraudDecision() valueobject.FraudDecision { return t.fraudDecision }
func (t *Transaction) EMVDataBase64() string                    { return t.emvDataBase64 }
func (t *Transaction) ISO8583Raw() []byte                       { return t.iso8583Raw }
func (t *Transaction) ReceivedAt() time.Time                    { return t.receivedAt }
func (t *Transaction) AuthorizedAt() *time.Time                 { return t.authorizedAt }
func (t *Transaction) RejectedAt() *time.Time                   { return t.rejectedAt }

// ReversalRequestedAt indica desde cuándo se pidió la reversa. Nil si no se pidió.
func (t *Transaction) ReversalRequestedAt() *time.Time { return t.reversalRequestedAt }

func (t *Transaction) AuthCode() *domain.AuthCode {
	return t.authCode
}

// CardToken devuelve el token de la tarjeta. Puede ser cero.
func (t *Transaction) CardToken() domain.CardToken { return t.cardToken }

func (t *Transaction) RejectionCode() *valueobject.RejectionCode {
	return t.rejectionCode
}

// DomainEvents retorna los eventos pendientes de publicar.
func (t *Transaction) DomainEvents() []event.DomainEvent {
	return t.domainEvents
}

// ClearDomainEvents limpia los eventos después de que fueron publicados.
func (t *Transaction) ClearDomainEvents() {
	t.domainEvents = nil
}
