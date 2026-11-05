package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func newTestPublisher(js *fakeJetStream) *EventPublisher {
	return NewEventPublisher(natsutil.NewPublisher(js))
}

func mustMoney(t *testing.T) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(1000, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return m
}

func mustSTAN(t *testing.T) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(123456)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
	}
	return s
}

func newSession(t *testing.T, channel valueobject.PaymentChannel) *aggregate.PaymentSession {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    channel,
		State:      valueobject.StateAwaitingPayment,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}

// ─── PublishTransactionReceived ────────────────────────────────────────────────

func TestPublishTransactionReceived_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 1}
	p := newTestPublisher(js)
	session := newSession(t, valueobject.ChannelNFC)

	if err := p.PublishTransactionReceived(context.Background(), session, []byte("iso-raw"), "emv-b64"); err != nil {
		t.Fatalf("PublishTransactionReceived() error = %v", err)
	}

	if len(js.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(js.published))
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectTransactionReceived {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectTransactionReceived)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.AggregateType != "PaymentSession" {
		t.Errorf("AggregateType = %q, want %q", envelope.AggregateType, "PaymentSession")
	}
	if envelope.AggregateID != session.ID().String() {
		t.Errorf("AggregateID = %q, want %q", envelope.AggregateID, session.ID().String())
	}

	payload, err := events.Unwrap[events.TransactionReceivedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != session.ID().String() {
		t.Errorf("TransactionID = %q, want %q", payload.TransactionID, session.ID().String())
	}
	if payload.AmountCents != session.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", payload.AmountCents, session.Amount().Cents())
	}
	if payload.STAN != session.STAN().Value() {
		t.Errorf("STAN = %d, want %d", payload.STAN, session.STAN().Value())
	}
	if payload.EntryMode != valueobject.ChannelNFC.ToEntryMode() {
		t.Errorf("EntryMode = %q, want %q", payload.EntryMode, valueobject.ChannelNFC.ToEntryMode())
	}
	if string(payload.ISO8583Raw) != "iso-raw" {
		t.Errorf("ISO8583Raw = %q, want %q", payload.ISO8583Raw, "iso-raw")
	}
	if payload.EMVDataBase64 != "emv-b64" {
		t.Errorf("EMVDataBase64 = %q, want %q", payload.EMVDataBase64, "emv-b64")
	}
	// PublishTransactionReceived (a diferencia de BuildTransactionReceived) no
	// recibe cardLast4/cardNetwork como parámetro — siempre publica estos
	// campos vacíos. Este método no tiene callers en producción actualmente
	// (el flujo real usa BuildTransactionReceived + outbox).
	if payload.CardLast4 != "" || payload.CardNetwork != "" {
		t.Errorf("CardLast4/CardNetwork = %q/%q, want empty (comportamiento actual)", payload.CardLast4, payload.CardNetwork)
	}
}

func TestPublishTransactionReceived_Error(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)

	err := p.PublishTransactionReceived(context.Background(), newSession(t, valueobject.ChannelQR), nil, "")
	if err == nil || !strings.Contains(err.Error(), "tg publisher: publish TransactionReceived") {
		t.Fatalf("error = %v, want it to contain %q", err, "tg publisher: publish TransactionReceived")
	}
}

// testCardToken es un HMAC-SHA256 de ejemplo: 64 hex en minúscula.
const testCardToken = "3b1f8a2c9d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8"

// ─── BuildTransactionReceived ───────────────────────────────────────────────────

func TestBuildTransactionReceived_Success(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)
	session := newSession(t, valueobject.ChannelMagstripe)

	subject, eventID, payloadBytes, err := p.BuildTransactionReceived(
		context.Background(), session, []byte("iso-raw"), "emv-b64", "1234", "VISA", testCardToken,
	)
	if err != nil {
		t.Fatalf("BuildTransactionReceived() error = %v", err)
	}
	if subject != events.SubjectTransactionReceived {
		t.Errorf("subject = %q, want %q", subject, events.SubjectTransactionReceived)
	}
	if eventID == "" {
		t.Error("eventID is empty, want a generated UUID")
	}
	if len(js.published) != 0 {
		t.Errorf("published messages = %d, want 0 (Build no publica)", len(js.published))
	}

	envelope, err := events.UnmarshalEnvelope(payloadBytes)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.EventID != eventID {
		t.Errorf("envelope.EventID = %q, want %q", envelope.EventID, eventID)
	}

	payload, err := events.Unwrap[events.TransactionReceivedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.CardLast4 != "1234" {
		t.Errorf("CardLast4 = %q, want %q", payload.CardLast4, "1234")
	}
	if payload.CardNetwork != "VISA" {
		t.Errorf("CardNetwork = %q, want %q", payload.CardNetwork, "VISA")
	}
	if payload.CardToken != testCardToken {
		t.Errorf("CardToken = %q, want %q", payload.CardToken, testCardToken)
	}
	if payload.EntryMode != valueobject.ChannelMagstripe.ToEntryMode() {
		t.Errorf("EntryMode = %q, want %q", payload.EntryMode, valueobject.ChannelMagstripe.ToEntryMode())
	}
}

// ─── PublishReversalRequested ───────────────────────────────────────────────────

func TestPublishReversalRequested_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 2}
	p := newTestPublisher(js)
	session := newSession(t, valueobject.ChannelQR)
	origTxID := domain.NewTransactionID()

	if err := p.PublishReversalRequested(context.Background(), origTxID, session); err != nil {
		t.Fatalf("PublishReversalRequested() error = %v", err)
	}

	if len(js.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(js.published))
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectReversalRequested {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectReversalRequested)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.AggregateID != origTxID.String() {
		t.Errorf("AggregateID = %q, want %q", envelope.AggregateID, origTxID.String())
	}

	payload, err := events.Unwrap[events.ReversalRequestedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.OriginalTransactionID != origTxID.String() {
		t.Errorf("OriginalTransactionID = %q, want %q", payload.OriginalTransactionID, origTxID.String())
	}
	if payload.TerminalID != session.TerminalID().String() {
		t.Errorf("TerminalID = %q, want %q", payload.TerminalID, session.TerminalID().String())
	}
	if payload.AmountCents != session.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", payload.AmountCents, session.Amount().Cents())
	}
}

func TestPublishReversalRequested_Error(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)

	err := p.PublishReversalRequested(context.Background(), domain.NewTransactionID(), newSession(t, valueobject.ChannelQR))
	if err == nil || !strings.Contains(err.Error(), "tg publisher: publish ReversalRequested") {
		t.Fatalf("error = %v, want it to contain %q", err, "tg publisher: publish ReversalRequested")
	}
}

// ─── PublishBatchCloseRequested ─────────────────────────────────────────────────

func TestPublishBatchCloseRequested_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 3}
	p := newTestPublisher(js)
	terminalID := domain.NewTerminalID()
	merchantID := domain.NewMerchantID()

	if err := p.PublishBatchCloseRequested(context.Background(), terminalID, merchantID, "2026-01-15", 5, 5000, "ARS"); err != nil {
		t.Fatalf("PublishBatchCloseRequested() error = %v", err)
	}

	if len(js.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(js.published))
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectBatchCloseRequested {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectBatchCloseRequested)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.AggregateType != "Terminal" {
		t.Errorf("AggregateType = %q, want %q", envelope.AggregateType, "Terminal")
	}

	payload, err := events.Unwrap[events.BatchCloseRequestedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.BatchDate != "2026-01-15" {
		t.Errorf("BatchDate = %q, want the date reported by the terminal", payload.BatchDate)
	}
	if payload.TerminalID != terminalID.String() {
		t.Errorf("TerminalID = %q, want %q", payload.TerminalID, terminalID.String())
	}
	if payload.MerchantID != merchantID.String() {
		t.Errorf("MerchantID = %q, want %q", payload.MerchantID, merchantID.String())
	}
	if payload.TerminalCount != 5 {
		t.Errorf("TerminalCount = %d, want 5", payload.TerminalCount)
	}
	if payload.TerminalAmount != 5000 {
		t.Errorf("TerminalAmount = %d, want 5000", payload.TerminalAmount)
	}
	if payload.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", payload.Currency, "ARS")
	}
}

func TestPublishBatchCloseRequested_Error(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)

	err := p.PublishBatchCloseRequested(context.Background(), domain.NewTerminalID(), domain.NewMerchantID(), "", 1, 1000, "ARS")
	if err == nil || !strings.Contains(err.Error(), "tg publisher: publish BatchCloseRequested") {
		t.Fatalf("error = %v, want it to contain %q", err, "tg publisher: publish BatchCloseRequested")
	}
}
