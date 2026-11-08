// Package aggregate contiene los Aggregates del BC Terminal Gateway.
package aggregate

import (
	"errors"
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/event"
	valueobject "github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// defaultSessionTTL es el tiempo de vida de una sesión QR.
// Pasado este tiempo sin pago completado, la sesión expira automáticamente.
const defaultSessionTTL = 5 * time.Minute

// PaymentSession es el Aggregate Root del BC Terminal Gateway.
// Representa el ciclo de vida completo de una sesión de pago:
// desde que el cajero inicia el cobro hasta que el resultado
// es mostrado en pantalla y enviado al terminal.
type PaymentSession struct {
	// Identidad
	id         domain.TransactionID // UUID generado al crear la sesión — es el CorrelationID global
	terminalID domain.TerminalID
	merchantID domain.MerchantID

	// Datos de la transacción
	amount  domain.Money
	stan    domain.STAN
	channel valueobject.PaymentChannel

	// Estado
	state     valueobject.SessionState
	expiresAt time.Time

	// Resultado (solo si APPROVED o REJECTED)
	authCode        string
	rejectionCode   string
	rejectionReason string

	// Timestamps
	createdAt time.Time
	closedAt  *time.Time

	// Eventos de dominio pendientes de publicar
	domainEvents []event.DomainEvent
}

// NewPaymentSession crea una nueva sesión de pago en estado AWAITING_PAYMENT.
func NewPaymentSession(
	terminalID domain.TerminalID,
	merchantID domain.MerchantID,
	amount domain.Money,
	stan domain.STAN,
	channel valueobject.PaymentChannel,
) (*PaymentSession, error) {
	if terminalID.IsZero() {
		return nil, fmt.Errorf("payment_session: terminal_id cannot be zero")
	}
	if merchantID.IsZero() {
		return nil, fmt.Errorf("payment_session: merchant_id cannot be zero")
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("payment_session: amount must be positive")
	}

	txID := domain.NewTransactionID()
	now := time.Now().UTC()

	s := &PaymentSession{
		id:         txID,
		terminalID: terminalID,
		merchantID: merchantID,
		amount:     amount,
		stan:       stan,
		channel:    channel,
		state:      valueobject.StateAwaitingPayment,
		expiresAt:  now.Add(defaultSessionTTL),
		createdAt:  now,
	}

	s.record(event.NewSessionCreated(txID, terminalID, merchantID, amount, stan, channel, s.expiresAt))
	return s, nil
}

// ─── Transiciones de estado ───────────────────────────────────────────────────

// StartProcessing transiciona a PROCESSING cuando el cliente inicia el pago.
// Emite el evento que dispara la Saga de autorización en el BC Authorization.
func (s *PaymentSession) StartProcessing(iso8583Raw []byte, emvDataBase64 string) error {
	if s.IsExpired() {
		return fmt.Errorf("payment_session %s: cannot start processing — session expired", s.id)
	}
	if err := s.transition(valueobject.StateProcessing); err != nil {
		return err
	}
	s.record(event.NewTransactionInitiated(
		s.id, s.terminalID, s.merchantID,
		s.amount, s.stan, s.channel,
		iso8583Raw, emvDataBase64,
	))
	return nil
}

// Approve transiciona a APPROVED con el resultado de la autorización.
func (s *PaymentSession) Approve(authCode string) error {
	if err := s.transition(valueobject.StateApproved); err != nil {
		return err
	}
	s.authCode = authCode
	now := time.Now().UTC()
	s.closedAt = &now
	s.record(event.NewSessionApproved(s.id, s.terminalID, authCode))
	return nil
}

// Reject transiciona a REJECTED con el código de rechazo del emisor.
func (s *PaymentSession) Reject(rejectionCode, rejectionReason string) error {
	if err := s.transition(valueobject.StateRejected); err != nil {
		return err
	}
	s.rejectionCode = rejectionCode
	s.rejectionReason = rejectionReason
	now := time.Now().UTC()
	s.closedAt = &now
	s.record(event.NewSessionRejected(s.id, s.terminalID, rejectionCode, rejectionReason))
	return nil
}

// Expire transiciona a EXPIRED cuando el TTL de la sesión vence.
func (s *PaymentSession) Expire() error {
	if err := s.transition(valueobject.StateExpired); err != nil {
		return err
	}
	now := time.Now().UTC()
	s.closedAt = &now
	s.record(event.NewSessionExpired(s.id, s.terminalID))
	return nil
}

// Cancel transiciona a CANCELLED cuando el cajero cancela manualmente.
func (s *PaymentSession) Cancel() error {
	if err := s.transition(valueobject.StateCancelled); err != nil {
		return err
	}
	now := time.Now().UTC()
	s.closedAt = &now
	s.record(event.NewSessionCancelled(s.id, s.terminalID))
	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// IsExpired indica si el TTL de la sesión venció.
func (s *PaymentSession) IsExpired() bool {
	return time.Now().UTC().After(s.expiresAt)
}

// TTLRemaining retorna el tiempo restante de vida de la sesión.
func (s *PaymentSession) TTLRemaining() time.Duration {
	remaining := time.Until(s.expiresAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ErrInvalidTransition indica que la sesión no admite la operación en su estado actual.
var ErrInvalidTransition = errors.New("invalid transition")

func (s *PaymentSession) transition(next valueobject.SessionState) error {
	if !s.state.CanTransitionTo(next) {
		return fmt.Errorf("payment_session %s: %w %s → %s", s.id, ErrInvalidTransition, s.state, next)
	}
	s.state = next
	return nil
}

func (s *PaymentSession) record(e event.DomainEvent) {
	s.domainEvents = append(s.domainEvents, e)
}

// ─── Getters ──────────────────────────────────────────────────────────────────

func (s *PaymentSession) ID() domain.TransactionID            { return s.id }
func (s *PaymentSession) TerminalID() domain.TerminalID       { return s.terminalID }
func (s *PaymentSession) MerchantID() domain.MerchantID       { return s.merchantID }
func (s *PaymentSession) Amount() domain.Money                { return s.amount }
func (s *PaymentSession) STAN() domain.STAN                   { return s.stan }
func (s *PaymentSession) Channel() valueobject.PaymentChannel { return s.channel }
func (s *PaymentSession) State() valueobject.SessionState     { return s.state }
func (s *PaymentSession) ExpiresAt() time.Time                { return s.expiresAt }
func (s *PaymentSession) AuthCode() string                    { return s.authCode }
func (s *PaymentSession) RejectionCode() string               { return s.rejectionCode }
func (s *PaymentSession) RejectionReason() string             { return s.rejectionReason }

// RequiresCardCapture indica si el terminal debe retener la tarjeta
// ("pick-up card") en vez de devolverla al portador.
//
// Se deriva del código de rechazo ya persistido en lugar de guardarse en una
// columna propia: así una sesión releída desde Postgres da la misma respuesta
// que la instrucción que Authorization mandó en el evento, sin que ambas
// puedan divergir. Los rechazos no-ISO (FRAUD_REJECTED, TIMEOUT) nunca
// coinciden con un código de captura.
func (s *PaymentSession) RequiresCardCapture() bool {
	return domain.RequiresCardCapture(s.rejectionCode)
}
func (s *PaymentSession) CreatedAt() time.Time              { return s.createdAt }
func (s *PaymentSession) ClosedAt() *time.Time              { return s.closedAt }
func (s *PaymentSession) DomainEvents() []event.DomainEvent { return s.domainEvents }
func (s *PaymentSession) ClearDomainEvents()                { s.domainEvents = nil }
