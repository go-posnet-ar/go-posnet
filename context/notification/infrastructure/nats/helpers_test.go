package nats

import (
	"context"
	"sync"
	"testing"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/notification/domain/aggregate"
	"github.com/juantevez/go-posnet/context/notification/domain/repository"
	"github.com/juantevez/go-posnet/context/notification/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests que construyen un *command.NotifyHandler real.
const idempotencySchema = "notification"

// ─── fakeJetStream ───────────────────────────────────────────────────────────

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

// ─── fakes de dominio (para construir un *command.NotifyHandler real) ────────

// fakeNotificationRepo es thread-safe porque NotifyHandler dispara el dispatch
// real en goroutines separadas (go h.dispatch(...)) — sin esto, los tests con
// -race detectarían un acceso concurrente a savedNotifs.
type fakeNotificationRepo struct {
	mu          sync.Mutex
	saveErr     error
	savedNotifs []*aggregate.Notification
	saveSignal  chan struct{} // si no-nil, se notifica en cada Save

	findResult *aggregate.Notification
	findErr    error
}

var _ repository.NotificationRepository = (*fakeNotificationRepo)(nil)

func (f *fakeNotificationRepo) Save(_ context.Context, n *aggregate.Notification) error {
	f.mu.Lock()
	f.savedNotifs = append(f.savedNotifs, n)
	f.mu.Unlock()
	if f.saveSignal != nil {
		f.saveSignal <- struct{}{}
	}
	return f.saveErr
}

func (f *fakeNotificationRepo) savedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.savedNotifs)
}

func (f *fakeNotificationRepo) lastSaved() *aggregate.Notification {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.savedNotifs) == 0 {
		return nil
	}
	return f.savedNotifs[len(f.savedNotifs)-1]
}

func (f *fakeNotificationRepo) FindByID(context.Context, string) (*aggregate.Notification, error) {
	return f.findResult, f.findErr
}

func (f *fakeNotificationRepo) FindByTransactionID(context.Context, domain.TransactionID) ([]*aggregate.Notification, error) {
	return nil, nil
}

func (f *fakeNotificationRepo) FindPendingRetries(context.Context, int) ([]*aggregate.Notification, error) {
	return nil, nil
}

func (f *fakeNotificationRepo) FindDead(context.Context, int) ([]*aggregate.Notification, error) {
	return nil, nil
}

type fakeTerminalNotifier struct {
	delivered bool
	reason    string
	err       error
}

func (f *fakeTerminalNotifier) SendReceipt(context.Context, *aggregate.Notification) (bool, string, error) {
	return f.delivered, f.reason, f.err
}

type fakeWebhookDispatcher struct {
	httpStatus int
	err        error
}

func (f *fakeWebhookDispatcher) Dispatch(context.Context, *aggregate.Notification) (int, error) {
	return f.httpStatus, f.err
}

type fakeDomainPublisher struct{}

func (f *fakeDomainPublisher) PublishDispatched(context.Context, *aggregate.Notification) error {
	return nil
}

// ─── builders ────────────────────────────────────────────────────────────────

func mustReceipt(t *testing.T) valueobject.ReceiptPayload {
	t.Helper()
	r, err := valueobject.NewReceiptPayload(
		domain.NewTransactionID().String(), "Merchant Inc", "TERM-001",
		"APPROVED", 5000, "ARS", "1234", "VISA", "CHIP",
		"2026-01-01T10:00:00Z",
	)
	if err != nil {
		t.Fatalf("NewReceiptPayload() error = %v", err)
	}
	return r
}

func newTestNotification(t *testing.T) *aggregate.Notification {
	t.Helper()
	n, err := aggregate.NewNotification(domain.NewTransactionID(), domain.NewMerchantID(), valueobject.ChannelWebhook, mustReceipt(t))
	if err != nil {
		t.Fatalf("NewNotification() error = %v", err)
	}
	return n
}

func newDeliveredNotification(t *testing.T) *aggregate.Notification {
	t.Helper()
	n := newTestNotification(t)
	if err := n.MarkSent(200); err != nil {
		t.Fatalf("MarkSent() error = %v", err)
	}
	return n
}
