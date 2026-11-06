package nats

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── fakeJetStream ───────────────────────────────────────────────────────────

// fakeJetStream implementa natsclient.JetStreamContext embebiendo la interfaz
// (nil) y sobreescribiendo solo PublishMsg y QueueSubscribe.
type fakeJetStream struct {
	natsclient.JetStreamContext

	publishErr error
	published  []*natsclient.Msg
	ackSeq     uint64

	subscribeErr   error
	subscribeCalls []queueSubscribeCall
}

type queueSubscribeCall struct {
	subject string
	durable string
}

func (f *fakeJetStream) PublishMsg(m *natsclient.Msg, _ ...natsclient.PubOpt) (*natsclient.PubAck, error) {
	f.published = append(f.published, m)
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return &natsclient.PubAck{Sequence: f.ackSeq}, nil
}

func (f *fakeJetStream) QueueSubscribe(subj, queue string, _ natsclient.MsgHandler, _ ...natsclient.SubOpt) (*natsclient.Subscription, error) {
	f.subscribeCalls = append(f.subscribeCalls, queueSubscribeCall{subject: subj, durable: queue})
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	return nil, nil
}

// ─── fakeBatchRepo (para construir un *command.BatchHandler real) ────────────

type fakeBatchRepo struct {
	saveErr       error
	saveCallCount int
	savedBatches  []*aggregate.SettlementBatch

	findOrCreateResult *aggregate.SettlementBatch
	findOrCreateErr    error

	findOpenResult *aggregate.SettlementBatch
	findOpenErr    error
}

var _ repository.SettlementBatchRepository = (*fakeBatchRepo)(nil)

func (f *fakeBatchRepo) Save(_ context.Context, b *aggregate.SettlementBatch) error {
	f.saveCallCount++
	f.savedBatches = append(f.savedBatches, b)
	return f.saveErr
}

func (f *fakeBatchRepo) SaveTx(_ context.Context, _ pgx.Tx, b *aggregate.SettlementBatch) error {
	return f.Save(context.Background(), b)
}

func (f *fakeBatchRepo) MerchantDayBatchesTx(context.Context, pgx.Tx, domain.MerchantID, time.Time) ([]repository.MerchantDayBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) lastSaved() *aggregate.SettlementBatch {
	if len(f.savedBatches) == 0 {
		return nil
	}
	return f.savedBatches[len(f.savedBatches)-1]
}

func (f *fakeBatchRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) FindOpenByTerminal(context.Context, domain.TerminalID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return f.findOpenResult, f.findOpenErr
}

func (f *fakeBatchRepo) FindOrCreateOpen(context.Context, domain.TerminalID, domain.MerchantID, time.Time, string) (*aggregate.SettlementBatch, error) {
	return f.findOrCreateResult, f.findOrCreateErr
}

func (f *fakeBatchRepo) ListByMerchantDate(context.Context, domain.MerchantID, time.Time) ([]*aggregate.SettlementBatch, error) {
	return nil, nil
}

func (f *fakeBatchRepo) FindStaleOpenIDs(context.Context, time.Time, string) ([]string, error) {
	return nil, nil
}

func (f *fakeBatchRepo) ClosedTotalsForTerminalDay(context.Context, domain.TerminalID, time.Time, string, string) (int, int64, error) {
	return 0, 0, nil
}

// ─── fakeDomainPublisher / fakeProcessor ─────────────────────────────────────

// fakeDomainPublisher implementa service.EventPublisher — distinto del
// *EventPublisher productivo de este mismo paquete (st_nats_publisher.go),
// que es lo que se está testeando en st_nats_publisher_test.go.
type fakeDomainPublisher struct{}

func (f *fakeDomainPublisher) BuildBatchClosed(context.Context, *aggregate.SettlementBatch) (string, string, []byte, error) {
	return "subject", "event-id", []byte("{}"), nil
}

func (f *fakeDomainPublisher) BuildSettlementCompleted(context.Context, string, string, int, int64, int64, string, float64) (string, string, []byte, error) {
	return "subject", "event-id", []byte("{}"), nil
}

type fakeProcessor struct {
	confirmationID string
	err            error
	calls          int
}

func (f *fakeProcessor) Submit(context.Context, *aggregate.SettlementBatch) (string, error) {
	f.calls++
	return f.confirmationID, f.err
}

// ─── builders ────────────────────────────────────────────────────────────────

func newOpenBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}

func newClosableBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t)
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	return b
}
