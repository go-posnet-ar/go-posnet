// Package websocket contiene el servidor WebSocket de los terminales: les entrega el resultado
// de sus ventas, los vencimientos y los comprobantes, sin que tengan que consultar /status.
//
// Los mensajes son JSON con un campo "type". El terminal envía primero
// {"type":"auth","token":"..."} y, si queda autenticado, recibe {"type":"auth.ok"}. Después solo
// recibe:
//
//   - session.created: se creó una sesión para el terminal.
//   - session.result: la sesión terminó aprobada o rechazada.
//   - session.expired: la sesión venció sin pagarse.
//   - receipt: el comprobante de una venta, enviado por Notification.
package websocket

import (
	"encoding/json"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/service"
)

// Tipos de mensaje.
const (
	TypeAuth           = "auth"
	TypeAuthOK         = "auth.ok"
	TypeError          = "error"
	TypeSessionCreated = "session.created"
	TypeSessionResult  = "session.result"
	TypeSessionExpired = "session.expired"
	TypeReceipt        = "receipt"
)

// authMessage es el primer mensaje del terminal. Con tokens configurados, se identifica con
// token; sin ellos, solo en desarrollo, con terminal_id.
type authMessage struct {
	Type       string `json:"type"`
	Token      string `json:"token,omitempty"`
	TerminalID string `json:"terminal_id,omitempty"`
}

type authOKMessage struct {
	Type       string `json:"type"`
	TerminalID string `json:"terminal_id"`
}

type errorMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type sessionCreatedMessage struct {
	Type          string `json:"type"`
	TransactionID string `json:"transaction_id"`
	AmountCents   int64  `json:"amount_cents"`
	Currency      string `json:"currency"`
	Channel       string `json:"channel"`
	ExpiresAt     string `json:"expires_at"`
}

type sessionResultMessage struct {
	Type            string `json:"type"`
	TransactionID   string `json:"transaction_id"`
	State           string `json:"state"`
	AuthCode        string `json:"auth_code,omitempty"`
	RejectionCode   string `json:"rejection_code,omitempty"`
	RejectionReason string `json:"rejection_reason,omitempty"`
	// CaptureCard es la orden de retener la tarjeta.
	CaptureCard bool `json:"capture_card"`
}

type sessionExpiredMessage struct {
	Type          string `json:"type"`
	TransactionID string `json:"transaction_id"`
}

type receiptMessage struct {
	Type            string `json:"type"`
	TransactionID   string `json:"transaction_id"`
	Result          string `json:"result"`
	MerchantName    string `json:"merchant_name,omitempty"`
	MerchantAddress string `json:"merchant_address,omitempty"`
	TerminalCode    string `json:"terminal_code,omitempty"`
	AuthCode        string `json:"auth_code,omitempty"`
	RejectionCode   string `json:"rejection_code,omitempty"`
	RejectionReason string `json:"rejection_reason,omitempty"`
	AmountCents     int64  `json:"amount_cents"`
	Currency        string `json:"currency"`
	CardLast4       string `json:"card_last4,omitempty"`
	CardNetwork     string `json:"card_network,omitempty"`
	EntryMode       string `json:"entry_mode,omitempty"`
	TransactionAt   string `json:"transaction_at,omitempty"`
}

func newSessionResult(s *aggregate.PaymentSession) sessionResultMessage {
	return sessionResultMessage{
		Type:            TypeSessionResult,
		TransactionID:   s.ID().String(),
		State:           s.State().String(),
		AuthCode:        s.AuthCode(),
		RejectionCode:   s.RejectionCode(),
		RejectionReason: s.RejectionReason(),
		CaptureCard:     s.RequiresCardCapture(),
	}
}

func newReceipt(r service.Receipt) receiptMessage {
	return receiptMessage{
		Type: TypeReceipt, TransactionID: r.TransactionID, Result: r.Result,
		MerchantName: r.MerchantName, MerchantAddress: r.MerchantAddress, TerminalCode: r.TerminalCode,
		AuthCode: r.AuthCode, RejectionCode: r.RejectionCode, RejectionReason: r.RejectionReason,
		AmountCents: r.AmountCents, Currency: r.Currency,
		CardLast4: r.CardLast4, CardNetwork: r.CardNetwork, EntryMode: r.EntryMode,
		TransactionAt: r.TransactionAt,
	}
}

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Los mensajes son structs propios, sin tipos que fallen al serializar.
		panic("websocket: encode message: " + err.Error())
	}
	return b
}
