package command_test

import (
	"context"
	"sync"
	"testing"

	"github.com/juantevez/go-posnet/context/notification/domain/aggregate"
	"github.com/juantevez/go-posnet/context/notification/domain/repository"
	"github.com/juantevez/go-posnet/context/notification/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests que construyen un *command.NotifyHandler real.
const idempotencySchema = "notification"

// ─── fakeNotificationRepo ────────────────────────────────────────────────────

// fakeNotificationRepo es thread-safe porque NotifyHandler dispara el dispatch
// real en goroutines separadas (go h.dispatch(...)).
type fakeNotificationRepo struct {
	mu            sync.Mutex
	saveErr       error
	saveErrOnCall int // si > 0, saveErr solo se devuelve en la llamada N (1-based)
	saveCallCount int
	savedNotifs   []*aggregate.Notification
	saveSignal    chan struct{} // si no-nil, se notifica en cada Save

	findResult *aggregate.Notification
	findErr    error

	byTransaction    []*aggregate.Notification // Lo que devuelve FindByTransactionID
	byTransactionErr error
}

var _ repository.NotificationRepository = (*fakeNotificationRepo)(nil)

func (f *fakeNotificationRepo) Save(_ context.Context, n *aggregate.Notification) error {
	f.mu.Lock()
	f.saveCallCount++
	call := f.saveCallCount
	f.savedNotifs = append(f.savedNotifs, n)
	f.mu.Unlock()
	if f.saveSignal != nil {
		f.saveSignal <- struct{}{}
	}
	if f.saveErrOnCall > 0 {
		if call == f.saveErrOnCall {
			return f.saveErr
		}
		return nil
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
	return f.byTransaction, f.byTransactionErr
}

func (f *fakeNotificationRepo) FindPendingRetries(context.Context, int) ([]*aggregate.Notification, error) {
	return nil, nil
}

func (f *fakeNotificationRepo) FindDead(context.Context, int) ([]*aggregate.Notification, error) {
	return nil, nil
}

// ─── fakeTerminalNotifier / fakeWebhookDispatcher / fakeEventPublisher ────────

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

// fakeEventPublisher es thread-safe por la misma razón que fakeNotificationRepo:
// NotifyApproval dispara dos dispatch(...) concurrentes, cada uno pudiendo
// llamar a PublishDispatched al mismo tiempo.
type fakeEventPublisher struct {
	mu           sync.Mutex
	publishErr   error
	publishCalls int
}

func (f *fakeEventPublisher) PublishDispatched(context.Context, *aggregate.Notification) error {
	f.mu.Lock()
	f.publishCalls++
	f.mu.Unlock()
	return f.publishErr
}

func (f *fakeEventPublisher) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.publishCalls
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

func newPendingNotification(t *testing.T, channel valueobject.NotificationChannel) *aggregate.Notification {
	t.Helper()
	n, err := aggregate.NewNotification(domain.NewTransactionID(), domain.NewMerchantID(), channel, mustReceipt(t))
	if err != nil {
		t.Fatalf("NewNotification() error = %v", err)
	}
	return n
}
