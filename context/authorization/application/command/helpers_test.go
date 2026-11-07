package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// idempotencySchema es el schema usado por natsutil.NewIdempotencyStore en los
// tests. La query que arma IdempotencyStore.TryMarkAsProcessed incluye este
// nombre, así que los expectations de pgxmock deben usarlo también.
const idempotencySchema = "authorization"

// ─── fakeRepo ──────────────────────────────────────────────────────────────

type fakeRepo struct {
	saveErr    error
	savedTxs   []*aggregate.Transaction
	findResult *aggregate.Transaction
	findErr    error

	// alreadyExists simula un transaction_id que ya existe: InsertTx devuelve false y
	// ExistsByID, true.
	alreadyExists bool
	existsErr     error

	// claimErr, claimLost y claimed corresponden al guardado junto con el evento (SaveIfStateTx).
	claimErr  error
	claimLost bool
	claimed   []claimedSave

	// resultLost simula que otro proceso movió la transacción (SaveIfState devuelve false).
	resultLost bool
	expected   []valueobject.TransactionState
	// failSaveAt hace fallar solo el guardado número failSaveAt de SaveIfState, contado desde 1.
	failSaveAt int
	saveCalls  int

	unresolved    []*aggregate.Transaction
	unresolvedErr error

	reversalMarks    int
	markErr          error
	pendingReversals []*aggregate.Transaction
	pendingErr       error
}

type claimedSave struct {
	state    valueobject.TransactionState
	expected valueobject.TransactionState
}

func (f *fakeRepo) SaveIfState(_ context.Context, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error) {
	f.saveCalls++
	f.expected = append(f.expected, expected)
	if f.saveErr != nil {
		return false, f.saveErr
	}
	if f.failSaveAt == f.saveCalls {
		return false, errors.New("db unreachable")
	}
	if f.resultLost {
		return false, nil
	}
	f.savedTxs = append(f.savedTxs, tx)
	return true, nil
}

func (f *fakeRepo) SaveIfStateTx(_ context.Context, _ pgx.Tx, tx *aggregate.Transaction, expected valueobject.TransactionState) (bool, error) {
	if f.claimErr != nil {
		return false, f.claimErr
	}
	if f.claimLost {
		return false, nil
	}
	f.claimed = append(f.claimed, claimedSave{state: tx.State(), expected: expected})
	return true, nil
}

func (f *fakeRepo) FindByIDForUpdateTx(ctx context.Context, _ pgx.Tx, id domain.TransactionID) (*aggregate.Transaction, error) {
	return f.FindByID(ctx, id)
}

func (f *fakeRepo) FindUnresolved(context.Context, time.Time, int) ([]*aggregate.Transaction, error) {
	return f.unresolved, f.unresolvedErr
}

func (f *fakeRepo) MarkReversalRequestedTx(context.Context, pgx.Tx, domain.TransactionID) error {
	f.reversalMarks++
	return f.markErr
}

func (f *fakeRepo) FindPendingReversals(context.Context, time.Time, int) ([]*aggregate.Transaction, error) {
	return f.pendingReversals, f.pendingErr
}

func (f *fakeRepo) CountUnresolved(context.Context, time.Time, time.Time) (repository.UnresolvedCounts, error) {
	return repository.UnresolvedCounts{}, nil
}

var _ repository.TransactionRepository = (*fakeRepo)(nil)

func (f *fakeRepo) InsertTx(_ context.Context, _ pgx.Tx, tx *aggregate.Transaction) (bool, error) {
	if f.saveErr != nil {
		return false, f.saveErr
	}
	if f.alreadyExists {
		return false, nil
	}
	f.savedTxs = append(f.savedTxs, tx)
	return true, nil
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
	return f.alreadyExists, f.existsErr
}

// ─── fakeBlockedCards ────────────────────────────────────────────────────────

type fakeBlockedCards struct {
	blocked    bool
	isBlockErr error

	blockErr   error
	blockCalls []blockCall
}

type blockCall struct {
	token  domain.CardToken
	reason string
	txID   domain.TransactionID
}

var _ repository.BlockedCardRepository = (*fakeBlockedCards)(nil)

func (f *fakeBlockedCards) IsBlocked(_ context.Context, _ domain.CardToken) (bool, error) {
	if f.isBlockErr != nil {
		return false, f.isBlockErr
	}
	return f.blocked, nil
}

func (f *fakeBlockedCards) Block(_ context.Context, token domain.CardToken, reason string, txID domain.TransactionID) error {
	f.blockCalls = append(f.blockCalls, blockCall{token: token, reason: reason, txID: txID})
	return f.blockErr
}

// testCardToken es un HMAC-SHA256 de ejemplo: 64 hex en minúscula.
const testCardToken = "3b1f8a2c9d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8"

// ─── fakeAcquirer ────────────────────────────────────────────────────────────

type fakeAcquirer struct {
	authorizeResp  service.AcquirerResponse
	authorizeErr   error
	authorizeCalls int

	reverseErr   error
	reverseCalls int
}

var _ service.AcquirerGateway = (*fakeAcquirer)(nil)

func (f *fakeAcquirer) Authorize(_ context.Context, _ *aggregate.Transaction) (service.AcquirerResponse, error) {
	f.authorizeCalls++
	return f.authorizeResp, f.authorizeErr
}

func (f *fakeAcquirer) Reverse(_ context.Context, _ *aggregate.Transaction) error {
	f.reverseCalls++
	return f.reverseErr
}

// ─── fakePublisher ───────────────────────────────────────────────────────────

type fakePublisher struct {
	approvedCalls int
	approvedErr   error

	rejectedCalls int
	rejectedErr   error

	invalidRejections []service.InvalidTransactionRejection
	invalidErr        error

	fraudCheckCalls int
	fraudCheckErr   error

	reversalCalls int
	reversalErr   error
}

var _ service.EventPublisher = (*fakePublisher)(nil)

func (f *fakePublisher) PublishApproved(_ context.Context, _ *aggregate.Transaction) error {
	f.approvedCalls++
	return f.approvedErr
}

func (f *fakePublisher) PublishRejected(_ context.Context, _ *aggregate.Transaction) error {
	f.rejectedCalls++
	return f.rejectedErr
}

func (f *fakePublisher) PublishInvalidRejected(_ context.Context, r service.InvalidTransactionRejection) error {
	if f.invalidErr != nil {
		return f.invalidErr
	}
	f.invalidRejections = append(f.invalidRejections, r)
	return nil
}

func (f *fakePublisher) PublishFraudCheckRequested(_ context.Context, _ *aggregate.Transaction) error {
	f.fraudCheckCalls++
	return f.fraudCheckErr
}

func (f *fakePublisher) PublishReversalCompleted(_ context.Context, _ domain.TransactionID, _ *aggregate.Transaction) error {
	f.reversalCalls++
	return f.reversalErr
}

// ─── Value object / command builders ─────────────────────────────────────────

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

// newValidTransaction crea una Transaction en estado RECEIVED.
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

// newFraudCheckingTransaction avanza una Transaction nueva hasta FRAUD_CHECKING.
func newFraudCheckingTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newValidTransaction(t)
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	return tx
}

// newApprovedTransaction avanza una Transaction nueva hasta APPROVED.
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
	ac, err := domain.NewAuthCode("AB1234")
	if err != nil {
		t.Fatalf("NewAuthCode() error = %v", err)
	}
	if err := tx.Approve(ac); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	return tx
}

// validAuthorizeCmd devuelve un AuthorizeTransactionCommand válido, listo
// para ser ajustado en cada caso de test.
func validAuthorizeCmd(t *testing.T) port.AuthorizeTransactionCommand {
	t.Helper()
	return port.AuthorizeTransactionCommand{
		EventID:       domain.NewTransactionID().String(),
		TransactionID: domain.NewTransactionID().String(),
		TerminalID:    domain.NewTerminalID().String(),
		MerchantID:    domain.NewMerchantID().String(),
		AmountCents:   5000,
		Currency:      string(domain.ARS),
		STAN:          1,
		EntryMode:     string(valueobject.EntryModeChip),
		CardLast4:     "1234",
		CardNetwork:   string(domain.NetworkVisa),
		EMVDataBase64: "emv==",
		ISO8583Raw:    []byte{0x01, 0x02},
	}
}

// validFraudScoreCmd devuelve un ApplyFraudScoreCommand válido para txID.
func validFraudScoreCmd(t *testing.T, txID domain.TransactionID) port.ApplyFraudScoreCommand {
	t.Helper()
	return port.ApplyFraudScoreCommand{
		EventID:       domain.NewTransactionID().String(),
		TransactionID: txID.String(),
		Score:         10,
		Decision:      valueobject.FraudDecisionApprove,
	}
}

// validReversalCmd devuelve un ProcessReversalCommand válido para txID.
func validReversalCmd(t *testing.T, txID domain.TransactionID) port.ProcessReversalCommand {
	t.Helper()
	return port.ProcessReversalCommand{
		EventID:               domain.NewTransactionID().String(),
		OriginalTransactionID: txID.String(),
		TerminalID:            domain.NewTerminalID().String(),
		MerchantID:            domain.NewMerchantID().String(),
		AmountCents:           5000,
		Currency:              string(domain.ARS),
		OriginalAuthCode:      "AB1234",
	}
}
