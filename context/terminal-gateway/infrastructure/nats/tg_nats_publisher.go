// Package nats contiene los adaptadores NATS del BC Terminal Gateway.
package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

// EventPublisher implementa domain/service.EventPublisher usando NATS JetStream.
type EventPublisher struct {
	pub *natsutil.Publisher
}

func NewEventPublisher(pub *natsutil.Publisher) *EventPublisher {
	return &EventPublisher{pub: pub}
}

// PublishTransactionReceived publica al stream POSNET_TRANSACTIONS.
// Consumido por: Authorization BC — dispara la Saga de autorización.
func (p *EventPublisher) PublishTransactionReceived(
	ctx context.Context,
	session *aggregate.PaymentSession,
	iso8583Raw []byte,
	emvDataBase64 string,
) error {
	_, err := p.pub.Publish(ctx,
		events.SubjectTransactionReceived,
		events.SubjectTransactionReceived,
		session.ID().String(),
		"PaymentSession",
		session.ID().String(),
		"",
		p.buildTransactionReceivedPayload(session, iso8583Raw, emvDataBase64, "", "", ""),
	)
	if err != nil {
		return fmt.Errorf("tg publisher: publish TransactionReceived: %w", err)
	}
	return nil
}

// BuildTransactionReceived serializa el evento sin publicarlo — para el Transactional Outbox.
// cardLast4 y cardNetwork vienen del ProcessPaymentCommand (capturados en el handler HTTP/QR).
// En un flujo ISO 8583 real estos campos se parsearían del mensaje raw; en el simulador QR
// se reciben directamente del payload del cliente.
func (p *EventPublisher) BuildTransactionReceived(
	ctx context.Context,
	session *aggregate.PaymentSession,
	iso8583Raw []byte,
	emvDataBase64 string,
	cardLast4 string,
	cardNetwork string,
	cardToken string,
) (subject, eventID string, payload []byte, err error) {
	eventID, payload, err = p.pub.Build(ctx,
		events.SubjectTransactionReceived,
		events.SubjectTransactionReceived,
		session.ID().String(),
		"PaymentSession",
		session.ID().String(),
		"",
		p.buildTransactionReceivedPayload(session, iso8583Raw, emvDataBase64, cardLast4, cardNetwork, cardToken),
	)
	if err != nil {
		return "", "", nil, fmt.Errorf("tg publisher: build TransactionReceived: %w", err)
	}
	return events.SubjectTransactionReceived, eventID, payload, nil
}

func (p *EventPublisher) buildTransactionReceivedPayload(
	session *aggregate.PaymentSession,
	iso8583Raw []byte,
	emvDataBase64 string,
	cardLast4 string,
	cardNetwork string,
	cardToken string,
) events.TransactionReceivedPayload {
	return events.TransactionReceivedPayload{
		TransactionID: session.ID().String(),
		TerminalID:    session.TerminalID().String(),
		MerchantID:    session.MerchantID().String(),
		AmountCents:   session.Amount().Cents(),
		Currency:      session.Amount().Currency().String(),
		STAN:          session.STAN().Value(),
		EntryMode:     session.Channel().ToEntryMode(),
		CardNetwork:   cardNetwork,
		CardLast4:     cardLast4,
		CardToken:     cardToken,
		EMVDataBase64: emvDataBase64,
		ISO8583Raw:    iso8583Raw,
		ReceivedAt:    time.Now().UTC().Format(time.RFC3339),
	}
}

// PublishReversalRequested publica al stream POSNET_TRANSACTIONS.
// Consumido por: Authorization BC.
func (p *EventPublisher) PublishReversalRequested(
	ctx context.Context,
	originalTxID domain.TransactionID,
	session *aggregate.PaymentSession,
) error {
	payload := events.ReversalRequestedPayload{
		OriginalTransactionID: originalTxID.String(),
		TerminalID:            session.TerminalID().String(),
		MerchantID:            session.MerchantID().String(),
		AmountCents:           session.Amount().Cents(),
		Currency:              session.Amount().Currency().String(),
		OriginalAuthCode:      session.AuthCode(),
		RequestedAt:           time.Now().UTC().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectReversalRequested,
		events.SubjectReversalRequested,
		originalTxID.String(),
		"PaymentSession",
		originalTxID.String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("tg publisher: publish ReversalRequested: %w", err)
	}
	return nil
}

// PublishBatchCloseRequested publica al stream POSNET_TRANSACTIONS.
// Consumido por: Settlement BC.
func (p *EventPublisher) PublishBatchCloseRequested(
	ctx context.Context,
	terminalID domain.TerminalID,
	merchantID domain.MerchantID,
	batchDate string,
	terminalCount int,
	terminalAmount int64,
	currency string,
) error {
	payload := events.BatchCloseRequestedPayload{
		TerminalID:     terminalID.String(),
		MerchantID:     merchantID.String(),
		BatchDate:      batchDate,
		TerminalCount:  terminalCount,
		TerminalAmount: terminalAmount,
		Currency:       currency,
		RequestedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	_, err := p.pub.Publish(ctx,
		events.SubjectBatchCloseRequested,
		events.SubjectBatchCloseRequested,
		terminalID.String(),
		"Terminal",
		terminalID.String(),
		"",
		payload,
	)
	if err != nil {
		return fmt.Errorf("tg publisher: publish BatchCloseRequested: %w", err)
	}
	return nil
}
