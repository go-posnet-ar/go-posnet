// Package service contiene los puertos de salida del BC Terminal Gateway.
package service

import (
	"context"
	"errors"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ErrTerminalNotConnected indica que el terminal no tiene una conexión WebSocket abierta.
var ErrTerminalNotConnected = errors.New("terminal not connected")

// TerminalConnections son las conexiones abiertas de los terminales: el servidor gRPC las usa
// para entregar comprobantes y para informar si un terminal está conectado.
type TerminalConnections interface {
	// SendReceipt envía el comprobante al terminal. Devuelve ErrTerminalNotConnected si no tiene
	// una conexión abierta.
	SendReceipt(ctx context.Context, terminalID domain.TerminalID, receipt Receipt) error

	// ConnectedSince devuelve desde cuándo está conectado el terminal, si lo está.
	ConnectedSince(terminalID domain.TerminalID) (time.Time, bool)
}

// Receipt es el comprobante que envía Notification para el terminal.
type Receipt struct {
	TransactionID   string
	Result          string // APPROVED, REJECTED o REVERSED
	MerchantName    string
	MerchantAddress string
	TerminalCode    string
	AuthCode        string
	RejectionCode   string
	RejectionReason string
	AmountCents     int64
	Currency        string
	CardLast4       string
	CardNetwork     string
	EntryMode       string
	TransactionAt   string
}

// TerminalNotifier es el puerto de salida hacia el WebSocket del terminal.
// El adaptador WebSocket lo implementa manteniendo un mapa de conexiones activas.
type TerminalNotifier interface {
	// NotifyResult envía el resultado de la transacción al terminal vía WebSocket.
	// Si el terminal no tiene sesión WebSocket activa, retorna ErrTerminalNotConnected.
	NotifyResult(ctx context.Context, session *aggregate.PaymentSession) error

	// NotifySessionCreated envía el payload de la sesión (QR o instrucción NFC)
	// al terminal para que lo muestre en pantalla.
	NotifySessionCreated(ctx context.Context, session *aggregate.PaymentSession) error

	// NotifySessionExpired avisa al terminal que el TTL de la sesión venció.
	NotifySessionExpired(ctx context.Context, terminalID domain.TerminalID, transactionID domain.TransactionID) error
}

// EventPublisher es el puerto de salida hacia NATS JetStream.
type EventPublisher interface {
	// PublishTransactionReceived publica el evento al stream POSNET_TRANSACTIONS.
	// Consumido por: Authorization BC.
	PublishTransactionReceived(ctx context.Context, session *aggregate.PaymentSession, iso8583Raw []byte, emvDataBase64 string) error

	// BuildTransactionReceived serializa el evento TransactionReceived sin publicarlo.
	// Retorna el subject, el event_id (para Nats-Msg-Id) y el payload JSON del envelope.
	// Usado por ProcessPayment para escribir en el outbox atómicamente con el Save.
	//
	// cardLast4 y cardNetwork se reciben del ProcessPaymentCommand: en el flujo QR
	// vienen del payload del cliente; en ISO 8583 real los parsearía el adaptador.
	// cardToken es el HMAC del PAN derivado en el borde; vacío si el terminal
	// todavía no lo emite — en ese caso la tarjeta no puede ser bloqueada.
	BuildTransactionReceived(ctx context.Context, session *aggregate.PaymentSession, iso8583Raw []byte, emvDataBase64 string, cardLast4, cardNetwork, cardToken string) (subject, eventID string, payload []byte, err error)

	// PublishReversalRequested publica la solicitud de anulación.
	// Consumido por: Authorization BC.
	PublishReversalRequested(ctx context.Context, originalTxID domain.TransactionID, session *aggregate.PaymentSession) error

	// PublishBatchCloseRequested publica el cierre de lote solicitado por el terminal.
	// Consumido por: Settlement BC.
	// batchDate es la fecha que informó el terminal; vacía, la resuelve Settlement.
	PublishBatchCloseRequested(ctx context.Context, terminalID domain.TerminalID, merchantID domain.MerchantID, batchDate string, terminalCount int, terminalAmount int64, currency string) error
}
