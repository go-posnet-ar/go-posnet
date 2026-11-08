package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

func newTestPublisher() *EventPublisher {
	return NewEventPublisher(natsutil.NewPublisher(nil), outbox.NewStore(idempotencySchema))
}

// bytesArg captura el payload que se escribe en el outbox.
type bytesArg struct{ got *[]byte }

func (a bytesArg) Match(v any) bool {
	b, ok := v.([]byte)
	if ok {
		*a.got = b
	}
	return ok
}

// enqueue publica fc con un pool mock y devuelve el envelope escrito en el outbox. Nada sale
// a NATS: lo publica el relay.
func enqueue(t *testing.T, fc *aggregate.FraudCase) events.DomainEvent {
	t.Helper()
	pool := newMockPool(t)
	var data []byte
	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO "+idempotencySchema+".outbox").
		WithArgs(events.SubjectFraudScoreCalculated, pgxmock.AnyArg(), bytesArg{got: &data}).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()

	tx := beginTx(t, pool)
	if err := newTestPublisher().PublishFraudScoreCalculated(context.Background(), tx, fc); err != nil {
		t.Fatalf("PublishFraudScoreCalculated() error = %v", err)
	}
	envelope, err := events.UnmarshalEnvelope(data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	return envelope
}

func beginTx(t *testing.T, pool pgxmock.PgxPoolIface) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

func TestPublishFraudScoreCalculated_EnqueuesInTheOutbox(t *testing.T) {
	txID := domain.NewTransactionID()
	fc := newEvaluatedFraudCase(t, txID)
	evaluatedAt := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	fc = withEvaluatedAt(t, fc, evaluatedAt)

	envelope := enqueue(t, fc)
	if envelope.EventType != events.SubjectFraudScoreCalculated {
		t.Errorf("EventType = %q, want %q", envelope.EventType, events.SubjectFraudScoreCalculated)
	}
	if envelope.AggregateType != "FraudCase" || envelope.AggregateID != txID.String() {
		t.Errorf("aggregate = %s %s, want FraudCase %s", envelope.AggregateType, envelope.AggregateID, txID)
	}

	payload, err := events.Unwrap[events.FraudScoreCalculatedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != txID.String() {
		t.Errorf("TransactionID = %q, want %q", payload.TransactionID, txID.String())
	}
	if payload.Score != fc.Score().Score() || payload.Decision != fc.Score().Decision().String() {
		t.Errorf("score = %d %s, want %d %s", payload.Score, payload.Decision, fc.Score().Score(), fc.Score().Decision())
	}
	if len(payload.RulesHit) != 1 || payload.RulesHit[0] != "RULE-001" {
		t.Errorf("RulesHit = %v, want [RULE-001]", payload.RulesHit)
	}
	// El momento de la evaluación, no el de la publicación.
	if payload.EvaluatedAt != evaluatedAt.Format(time.RFC3339) {
		t.Errorf("EvaluatedAt = %q, want %q", payload.EvaluatedAt, evaluatedAt.Format(time.RFC3339))
	}
}

func TestPublishFraudScoreCalculated_EmptyRulesHit(t *testing.T) {
	envelope := enqueue(t, newApprovedFraudCase(t, domain.NewTransactionID())) // score 0, sin reglas

	payload, err := events.Unwrap[events.FraudScoreCalculatedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.Score != 0 || len(payload.RulesHit) != 0 {
		t.Errorf("payload = %+v, want score 0 and no rules hit", payload)
	}
}

func TestPublishFraudScoreCalculated_EnqueueError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	pool.ExpectExec("INSERT INTO "+idempotencySchema+".outbox").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(errors.New("connection reset"))
	pool.ExpectRollback()

	err := newTestPublisher().PublishFraudScoreCalculated(context.Background(), beginTx(t, pool), newEvaluatedFraudCase(t, domain.NewTransactionID()))
	if err == nil || !strings.Contains(err.Error(), "enqueue FraudScoreCalculated") {
		t.Fatalf("error = %v, want it to contain %q", err, "enqueue FraudScoreCalculated")
	}
}

// withEvaluatedAt reconstruye fc con la fecha de evaluación at.
func withEvaluatedAt(t *testing.T, fc *aggregate.FraudCase, at time.Time) *aggregate.FraudCase {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:            fc.ID(),
		TransactionID: fc.TransactionID(),
		TerminalID:    fc.TerminalID(),
		MerchantID:    fc.MerchantID(),
		AmountCents:   fc.AmountCents(),
		Currency:      fc.Currency(),
		CardNetwork:   fc.CardNetwork(),
		EntryMode:     fc.EntryMode(),
		OccurredAt:    fc.OccurredAt(),
		Score:         fc.Score(),
		EvaluatedAt:   &at,
	})
}
