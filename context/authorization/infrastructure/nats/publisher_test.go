package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func newTestPublisher(js *fakeJetStream) *EventPublisher {
	return NewEventPublisher(natsutil.NewPublisher(js))
}

// ─── PublishApproved ───────────────────────────────────────────────────────────

func TestPublishApproved_Success(t *testing.T) {
	js := &fakeJetStream{ackSeq: 42}
	p := newTestPublisher(js)
	tx := newApprovedTransaction(t)

	if err := p.PublishApproved(context.Background(), tx); err != nil {
		t.Fatalf("PublishApproved() error = %v", err)
	}
	if len(js.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(js.published))
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectAuthApproved {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectAuthApproved)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.EventType != events.SubjectAuthApproved {
		t.Errorf("EventType = %q, want %q", envelope.EventType, events.SubjectAuthApproved)
	}
	if envelope.AggregateID != tx.ID().String() {
		t.Errorf("AggregateID = %q, want %q", envelope.AggregateID, tx.ID().String())
	}

	payload, err := events.Unwrap[events.AuthorizationApprovedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != tx.ID().String() {
		t.Errorf("TransactionID = %q, want %q", payload.TransactionID, tx.ID().String())
	}
	if payload.AuthCode != tx.AuthCode().String() {
		t.Errorf("AuthCode = %q, want %q", payload.AuthCode, tx.AuthCode().String())
	}
	if payload.AmountCents != tx.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", payload.AmountCents, tx.Amount().Cents())
	}
	if payload.Currency != tx.Amount().Currency().String() {
		t.Errorf("Currency = %q, want %q", payload.Currency, tx.Amount().Currency().String())
	}
	if payload.CardLast4 != tx.PAN().Last4() {
		t.Errorf("CardLast4 = %q, want %q", payload.CardLast4, tx.PAN().Last4())
	}
	if payload.AuthorizedAt != tx.AuthorizedAt().Format(time.RFC3339) {
		t.Errorf("AuthorizedAt = %q, want %q", payload.AuthorizedAt, tx.AuthorizedAt().Format(time.RFC3339))
	}
}

func TestPublishApproved_NilAuthCode(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)

	authorizedAt := time.Now().UTC()
	tx := aggregate.Reconstitute(baseReconstituteParams(t, valueobject.StateApproved, &authorizedAt, nil))

	err := p.PublishApproved(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "auth code is nil") {
		t.Fatalf("error = %v, want it to contain %q", err, "auth code is nil")
	}
	if len(js.published) != 0 {
		t.Errorf("published messages = %d, want 0", len(js.published))
	}
}

func TestPublishApproved_NilAuthorizedAt(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)

	authCode := "AB1234"
	params := baseReconstituteParams(t, valueobject.StateApproved, nil, nil)
	params.AuthCode = &authCode
	tx := aggregate.Reconstitute(params)

	err := p.PublishApproved(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "authorized_at is nil") {
		t.Fatalf("error = %v, want it to contain %q", err, "authorized_at is nil")
	}
	if len(js.published) != 0 {
		t.Errorf("published messages = %d, want 0", len(js.published))
	}
}

func TestPublishApproved_PublishError(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)
	tx := newApprovedTransaction(t)

	err := p.PublishApproved(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "publish approved") {
		t.Fatalf("error = %v, want it to contain %q", err, "publish approved")
	}
}

// ─── PublishRejected ───────────────────────────────────────────────────────────

func TestPublishRejected_Success(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)
	tx := newRejectedTransaction(t)

	if err := p.PublishRejected(context.Background(), tx); err != nil {
		t.Fatalf("PublishRejected() error = %v", err)
	}
	if len(js.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(js.published))
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectAuthRejected {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectAuthRejected)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	payload, err := events.Unwrap[events.AuthorizationRejectedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	rc := tx.RejectionCode()
	if payload.RejectionCode != rc.Code() {
		t.Errorf("RejectionCode = %q, want %q", payload.RejectionCode, rc.Code())
	}
	if payload.RejectionReason != rc.Description() {
		t.Errorf("RejectionReason = %q, want %q", payload.RejectionReason, rc.Description())
	}
	if payload.IsRetryable != rc.IsRetryable() {
		t.Errorf("IsRetryable = %v, want %v", payload.IsRetryable, rc.IsRetryable())
	}
	if payload.Source != string(rc.Source()) {
		t.Errorf("Source = %q, want %q", payload.Source, string(rc.Source()))
	}
}

func TestPublishRejected_NilRejectionCode(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)

	tx := aggregate.Reconstitute(baseReconstituteParams(t, valueobject.StateRejected, nil, nil))

	err := p.PublishRejected(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "rejection code is nil") {
		t.Fatalf("error = %v, want it to contain %q", err, "rejection code is nil")
	}
	if len(js.published) != 0 {
		t.Errorf("published messages = %d, want 0", len(js.published))
	}
}

func TestPublishRejected_PublishError(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)
	tx := newRejectedTransaction(t)

	err := p.PublishRejected(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "publish rejected") {
		t.Fatalf("error = %v, want it to contain %q", err, "publish rejected")
	}
}

func TestPublishInvalidRejected(t *testing.T) {
	js := &fakeJetStream{}
	id := domain.NewTransactionID()

	err := newTestPublisher(js).PublishInvalidRejected(context.Background(), service.InvalidTransactionRejection{
		TransactionID: id,
		AmountCents:   5000,
		Currency:      "ARS",
		Rejection:     valueobject.NewRejectionFromValidation(valueobject.CodeInvalidData),
	})
	if err != nil {
		t.Fatalf("PublishInvalidRejected() error = %v", err)
	}
	if len(js.published) != 1 || js.published[0].Subject != events.SubjectAuthRejected {
		t.Fatalf("published = %+v, want one message on %q", js.published, events.SubjectAuthRejected)
	}
	envelope, err := events.UnmarshalEnvelope(js.published[0].Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.CorrelationID != id.String() {
		t.Errorf("CorrelationID = %q, want %q", envelope.CorrelationID, id)
	}
	payload, err := events.Unwrap[events.AuthorizationRejectedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != id.String() || payload.RejectionCode != valueobject.CodeInvalidData ||
		payload.Source != string(valueobject.SourceValidation) || payload.RejectionReason == "" {
		t.Errorf("payload = %+v", payload)
	}
	if payload.IsRetryable || payload.CaptureCard {
		t.Errorf("payload = %+v: no debe ser reintentable ni ordenar la retención", payload)
	}
	if payload.CardLast4 != "" || payload.AmountCents != 5000 || payload.RejectedAt == "" {
		t.Errorf("payload = %+v", payload)
	}
}

// ─── PublishFraudCheckRequested ────────────────────────────────────────────────

func TestPublishFraudCheckRequested_Success(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)
	tx := newValidTransaction(t)

	if err := p.PublishFraudCheckRequested(context.Background(), tx); err != nil {
		t.Fatalf("PublishFraudCheckRequested() error = %v", err)
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectFraudCheckRequested {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectFraudCheckRequested)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	payload, err := events.Unwrap[events.FraudCheckRequestedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != tx.ID().String() {
		t.Errorf("TransactionID = %q, want %q", payload.TransactionID, tx.ID().String())
	}
	if payload.AmountCents != tx.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", payload.AmountCents, tx.Amount().Cents())
	}
	if payload.OccurredAt != tx.ReceivedAt().Format(time.RFC3339) {
		t.Errorf("OccurredAt = %q, want %q", payload.OccurredAt, tx.ReceivedAt().Format(time.RFC3339))
	}
}

func TestPublishFraudCheckRequested_PublishError(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)
	tx := newValidTransaction(t)

	err := p.PublishFraudCheckRequested(context.Background(), tx)
	if err == nil || !strings.Contains(err.Error(), "publish fraud check requested") {
		t.Fatalf("error = %v, want it to contain %q", err, "publish fraud check requested")
	}
}

// ─── PublishReversalCompleted ──────────────────────────────────────────────────

func TestPublishReversalCompleted_Success(t *testing.T) {
	js := &fakeJetStream{}
	p := newTestPublisher(js)
	tx := newApprovedTransaction(t)
	originalTxID := domain.NewTransactionID() // deliberadamente distinto de tx.ID()

	if err := p.PublishReversalCompleted(context.Background(), originalTxID, tx); err != nil {
		t.Fatalf("PublishReversalCompleted() error = %v", err)
	}
	msg := js.published[0]
	if msg.Subject != events.SubjectReversalCompleted {
		t.Errorf("Subject = %q, want %q", msg.Subject, events.SubjectReversalCompleted)
	}

	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	if envelope.AggregateID != originalTxID.String() {
		t.Errorf("AggregateID = %q, want %q (el ID recibido por parámetro, no tx.ID())", envelope.AggregateID, originalTxID.String())
	}

	payload, err := events.Unwrap[events.ReversalCompletedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.OriginalTransactionID != originalTxID.String() {
		t.Errorf("OriginalTransactionID = %q, want %q", payload.OriginalTransactionID, originalTxID.String())
	}
	if payload.AmountCents != tx.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", payload.AmountCents, tx.Amount().Cents())
	}
}

func TestPublishReversalCompleted_PublishError(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("nats unavailable")}
	p := newTestPublisher(js)
	tx := newApprovedTransaction(t)

	err := p.PublishReversalCompleted(context.Background(), domain.NewTransactionID(), tx)
	if err == nil || !strings.Contains(err.Error(), "publish reversal completed") {
		t.Fatalf("error = %v, want it to contain %q", err, "publish reversal completed")
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

// baseReconstituteParams arma un ReconstituteParams válido con el estado y los
// campos opcionales (AuthorizedAt/RejectedAt) dados, para forzar estados
// inconsistentes (ej: APPROVED sin AuthCode) que la máquina de estados normal
// no produciría pero que el publisher igual debe manejar sin panic.
func baseReconstituteParams(t *testing.T, state valueobject.TransactionState, authorizedAt, rejectedAt *time.Time) aggregate.ReconstituteParams {
	t.Helper()
	return aggregate.ReconstituteParams{
		ID:            domain.NewTransactionID(),
		TerminalID:    domain.NewTerminalID(),
		MerchantID:    domain.NewMerchantID(),
		Amount:        mustMoney(t, 12345),
		STAN:          mustSTAN(t, 7),
		PAN:           mustPAN(t),
		EntryMode:     valueobject.EntryModeChip,
		State:         state,
		EMVDataBase64: "emv==",
		ISO8583Raw:    []byte{0x01},
		ReceivedAt:    time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		AuthorizedAt:  authorizedAt,
		RejectedAt:    rejectedAt,
	}
}
