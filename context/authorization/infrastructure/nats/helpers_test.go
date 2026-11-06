package nats

import (
	"context"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests que construyen un *command.AuthorizationHandler real.
const idempotencySchema = "authorization"

// ─── fakeJetStream ───────────────────────────────────────────────────────────

// fakeJetStream implementa natsclient.JetStreamContext embebiendo la interfaz
// (nil) y sobreescribiendo solo los métodos que el paquete ejercita:
// PublishMsg (usado por natsutil.Publisher) y QueueSubscribe (usado por
// Subscriber.Subscribe). Cualquier otro método de la interfaz haría panic si
// se invocara, pero el código bajo test nunca los llama.
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

// ─── fakes de dominio (para construir un *command.AuthorizationHandler real) ──

type fakeRepo struct {
	saveErr    error
	savedTxs   []*aggregate.Transaction
	findResult *aggregate.Transaction
	findErr    error
}

var _ repository.TransactionRepository = (*fakeRepo)(nil)

func (f *fakeRepo) Save(_ context.Context, tx *aggregate.Transaction) error {
	f.savedTxs = append(f.savedTxs, tx)
	return f.saveErr
}

func (f *fakeRepo) FindByID(_ context.Context, _ domain.TransactionID) (*aggregate.Transaction, error) {
	return f.findResult, f.findErr
}

func (f *fakeRepo) FindBySTAN(_ context.Context, _ domain.TerminalID, _ domain.STAN, _ time.Time) (*aggregate.Transaction, error) {
	return nil, nil
}

func (f *fakeRepo) UpdateState(_ context.Context, _ domain.TransactionID, _ valueobject.TransactionState) error {
	return nil
}

func (f *fakeRepo) ExistsByID(_ context.Context, _ domain.TransactionID) (bool, error) {
	return false, nil
}

func (f *fakeRepo) SaveIfState(ctx context.Context, tx *aggregate.Transaction, _ valueobject.TransactionState) (bool, error) {
	return true, f.Save(ctx, tx)
}

func (f *fakeRepo) SaveIfStateTx(ctx context.Context, _ pgx.Tx, tx *aggregate.Transaction, _ valueobject.TransactionState) (bool, error) {
	return true, f.Save(ctx, tx)
}

func (f *fakeRepo) FindByIDForUpdateTx(ctx context.Context, _ pgx.Tx, id domain.TransactionID) (*aggregate.Transaction, error) {
	return f.FindByID(ctx, id)
}

func (f *fakeRepo) FindUnresolved(context.Context, time.Time, int) ([]*aggregate.Transaction, error) {
	return nil, nil
}

func (f *fakeRepo) MarkReversalRequestedTx(context.Context, pgx.Tx, domain.TransactionID) error {
	return nil
}

func (f *fakeRepo) FindPendingReversals(context.Context, time.Time, int) ([]*aggregate.Transaction, error) {
	return nil, nil
}

func (f *fakeRepo) CountUnresolved(context.Context, time.Time, time.Time) (repository.UnresolvedCounts, error) {
	return repository.UnresolvedCounts{}, nil
}

type fakeAcquirer struct {
	authorizeResp service.AcquirerResponse
	authorizeErr  error
	reverseErr    error
}

var _ service.AcquirerGateway = (*fakeAcquirer)(nil)

func (f *fakeAcquirer) Authorize(_ context.Context, _ *aggregate.Transaction) (service.AcquirerResponse, error) {
	return f.authorizeResp, f.authorizeErr
}

func (f *fakeAcquirer) Reverse(_ context.Context, _ *aggregate.Transaction) error {
	return f.reverseErr
}

// fakeDomainPublisher implementa service.EventPublisher — distinto del
// *EventPublisher productivo de este mismo paquete (publisher.go), que es lo
// que se está testeando en publisher_test.go.
type fakeDomainPublisher struct {
	approvedCalls   int
	rejectedCalls   int
	fraudCheckCalls int
	reversalCalls   int
}

var _ service.EventPublisher = (*fakeDomainPublisher)(nil)

func (f *fakeDomainPublisher) PublishApproved(_ context.Context, _ *aggregate.Transaction) error {
	f.approvedCalls++
	return nil
}

func (f *fakeDomainPublisher) PublishRejected(_ context.Context, _ *aggregate.Transaction) error {
	f.rejectedCalls++
	return nil
}

func (f *fakeDomainPublisher) PublishFraudCheckRequested(_ context.Context, _ *aggregate.Transaction) error {
	f.fraudCheckCalls++
	return nil
}

func (f *fakeDomainPublisher) PublishReversalCompleted(_ context.Context, _ domain.TransactionID, _ *aggregate.Transaction) error {
	f.reversalCalls++
	return nil
}

// ─── builders de value objects / aggregates ──────────────────────────────────

func mustMoney(t *testing.T, cents int64) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(cents, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney(%d) error = %v", cents, err)
	}
	return m
}

func mustSTAN(t *testing.T, v int) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(v)
	if err != nil {
		t.Fatalf("NewSTAN(%d) error = %v", v, err)
	}
	return s
}

func mustPAN(t *testing.T) domain.PAN {
	t.Helper()
	p, err := domain.NewPAN("1234", domain.NetworkVisa)
	if err != nil {
		t.Fatalf("NewPAN() error = %v", err)
	}
	return p
}

func mustAuthCode(t *testing.T, s string) domain.AuthCode {
	t.Helper()
	a, err := domain.NewAuthCode(s)
	if err != nil {
		t.Fatalf("NewAuthCode(%q) error = %v", s, err)
	}
	return a
}

func newValidTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx, err := aggregate.NewTransaction(
		domain.NewTransactionID(),
		domain.NewTerminalID(),
		domain.NewMerchantID(),
		mustMoney(t, 5000),
		mustSTAN(t, 1),
		mustPAN(t),
		valueobject.EntryModeChip,
		domain.CardToken{},
		"emv-data-base64",
		[]byte{0xAA, 0xBB},
	)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	return tx
}

func newFraudCheckingTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newValidTransaction(t)
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	return tx
}

func newApprovedTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newFraudCheckingTransaction(t)
	fd, err := valueobject.NewFraudDecision(10, valueobject.FraudDecisionApprove, nil)
	if err != nil {
		t.Fatalf("NewFraudDecision() error = %v", err)
	}
	if err := tx.ApplyFraudDecision(fd); err != nil {
		t.Fatalf("ApplyFraudDecision() error = %v", err)
	}
	if err := tx.Approve(mustAuthCode(t, "AB1234")); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	return tx
}

func newRejectedTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newValidTransaction(t)
	rc, err := valueobject.NewRejectionFromISO(valueobject.ISO_DO_NOT_HONOR)
	if err != nil {
		t.Fatalf("NewRejectionFromISO() error = %v", err)
	}
	if err := tx.Reject(rc); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	return tx
}
