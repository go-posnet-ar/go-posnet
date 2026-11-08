package query_test

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/application/query"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── fakeRepo ──────────────────────────────────────────────────────────────

type fakeRepo struct {
	invalidResult *valueobject.InvalidTransactionRejection
	invalidErr    error

	findResult *aggregate.Transaction
	findErr    error
}

var _ repository.TransactionRepository = (*fakeRepo)(nil)

func (f *fakeRepo) Save(_ context.Context, _ *aggregate.Transaction) error { return nil }

func (f *fakeRepo) FindByID(_ context.Context, _ domain.TransactionID) (*aggregate.Transaction, error) {
	return f.findResult, f.findErr
}

func (f *fakeRepo) ExistsByID(_ context.Context, _ domain.TransactionID) (bool, error) {
	return false, nil
}

func (f *fakeRepo) SaveInvalidRejection(context.Context, valueobject.InvalidTransactionRejection) error {
	return nil
}

func (f *fakeRepo) DeleteInvalidRejectionsBefore(context.Context, time.Time) (int64, error) {
	return 0, nil
}

func (f *fakeRepo) FindInvalidRejection(context.Context, domain.TransactionID) (*valueobject.InvalidTransactionRejection, error) {
	return f.invalidResult, f.invalidErr
}

func (f *fakeRepo) InsertTx(ctx context.Context, _ pgx.Tx, tx *aggregate.Transaction) (bool, error) {
	return true, f.Save(ctx, tx)
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

// ─── builders ────────────────────────────────────────────────────────────────

func mustMoney(t *testing.T, cents int64) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(cents, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney(%d) error = %v", cents, err)
	}
	return m
}

func mustSTAN(t *testing.T) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(1)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
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

func baseParams(t *testing.T, id domain.TransactionID) aggregate.ReconstituteParams {
	t.Helper()
	return aggregate.ReconstituteParams{
		ID:            id,
		TerminalID:    domain.NewTerminalID(),
		MerchantID:    domain.NewMerchantID(),
		Amount:        mustMoney(t, 12345),
		STAN:          mustSTAN(t),
		PAN:           mustPAN(t),
		EntryMode:     valueobject.EntryModeChip,
		EMVDataBase64: "emv==",
		ISO8583Raw:    []byte{0x01},
		ReceivedAt:    time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}
}

// ─── tests ───────────────────────────────────────────────────────────────────

func TestGetTransactionStatus_FindByIDError(t *testing.T) {
	repo := &fakeRepo{findErr: errors.New("connection lost")}
	h := query.NewTransactionQueryHandler(repo)

	_, err := h.GetTransactionStatus(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "GetTransactionStatus") {
		t.Fatalf("error = %v, want it to contain %q", err, "GetTransactionStatus")
	}
}

func TestGetTransactionStatus_NotFound(t *testing.T) {
	repo := &fakeRepo{findResult: nil, findErr: nil}
	h := query.NewTransactionQueryHandler(repo)

	id := domain.NewTransactionID()
	_, err := h.GetTransactionStatus(context.Background(), id)
	if err == nil {
		t.Fatal("GetTransactionStatus() error = nil, want NotFoundError")
	}
	var nf *pkgerrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error type = %T, want *pkgerrors.NotFoundError", err)
	}
	if nf.Entity != "Transaction" || nf.ID != id.String() {
		t.Errorf("NotFoundError = %+v, want Entity=Transaction ID=%s", nf, id.String())
	}
}

func TestGetTransactionStatus_ReceivedState(t *testing.T) {
	id := domain.NewTransactionID()
	params := baseParams(t, id)
	params.State = valueobject.StateReceived

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.TransactionID != id.String() {
		t.Errorf("TransactionID = %q, want %q", result.TransactionID, id.String())
	}
	if result.State != "RECEIVED" {
		t.Errorf("State = %q, want %q", result.State, "RECEIVED")
	}
	if result.AmountCents != 12345 {
		t.Errorf("AmountCents = %d, want 12345", result.AmountCents)
	}
	if result.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", result.Currency, "ARS")
	}
	if result.AuthCode != "" || result.AuthorizedAt != "" {
		t.Errorf("AuthCode/AuthorizedAt = %q/%q, want both empty", result.AuthCode, result.AuthorizedAt)
	}
	if result.RejectionCode != "" || result.RejectionReason != "" || result.RejectedAt != "" {
		t.Errorf("rejection fields not empty: %+v", result)
	}
}

func TestGetTransactionStatus_Approved(t *testing.T) {
	id := domain.NewTransactionID()
	authCode := "AB1234"
	authorizedAt := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)

	params := baseParams(t, id)
	params.State = valueobject.StateApproved
	params.AuthCode = &authCode
	params.AuthorizedAt = &authorizedAt

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.State != "APPROVED" {
		t.Errorf("State = %q, want %q", result.State, "APPROVED")
	}
	if result.AuthCode != authCode {
		t.Errorf("AuthCode = %q, want %q", result.AuthCode, authCode)
	}
	wantAuthorizedAt := authorizedAt.Format("2006-01-02T15:04:05Z")
	if result.AuthorizedAt != wantAuthorizedAt {
		t.Errorf("AuthorizedAt = %q, want %q", result.AuthorizedAt, wantAuthorizedAt)
	}
	if result.RejectionCode != "" || result.RejectionReason != "" || result.RejectedAt != "" {
		t.Errorf("rejection fields not empty on an approved transaction: %+v", result)
	}
}

func TestGetTransactionStatus_ApprovedWithoutAuthCode(t *testing.T) {
	// Estado inconsistente (no debería ocurrir en la práctica, ya que Approve()
	// siempre setea AuthCode) — verifica que el guard `tx.AuthCode() != nil` se
	// respete y no haga panic ni setee un AuthCode vacío por accidente.
	id := domain.NewTransactionID()
	params := baseParams(t, id)
	params.State = valueobject.StateApproved

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.AuthCode != "" {
		t.Errorf("AuthCode = %q, want empty when the aggregate has no AuthCode", result.AuthCode)
	}
	if result.AuthorizedAt != "" {
		t.Errorf("AuthorizedAt = %q, want empty", result.AuthorizedAt)
	}
}

func TestGetTransactionStatus_ApprovedWithoutAuthorizedAt(t *testing.T) {
	id := domain.NewTransactionID()
	authCode := "AB1234"
	params := baseParams(t, id)
	params.State = valueobject.StateApproved
	params.AuthCode = &authCode
	// AuthorizedAt deliberadamente sin setear.

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.AuthCode != authCode {
		t.Errorf("AuthCode = %q, want %q", result.AuthCode, authCode)
	}
	if result.AuthorizedAt != "" {
		t.Errorf("AuthorizedAt = %q, want empty when the aggregate has no AuthorizedAt", result.AuthorizedAt)
	}
}

func TestGetTransactionStatus_Rejected(t *testing.T) {
	id := domain.NewTransactionID()
	rejectionCode := valueobject.ISO_DO_NOT_HONOR
	rejectedAt := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)

	params := baseParams(t, id)
	params.State = valueobject.StateRejected
	params.RejectionCode = &rejectionCode
	params.RejectedAt = &rejectedAt

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.State != "REJECTED" {
		t.Errorf("State = %q, want %q", result.State, "REJECTED")
	}
	if result.RejectionCode != rejectionCode {
		t.Errorf("RejectionCode = %q, want %q", result.RejectionCode, rejectionCode)
	}
	if result.RejectionReason != "Do Not Honor" {
		t.Errorf("RejectionReason = %q, want %q", result.RejectionReason, "Do Not Honor")
	}
	wantRejectedAt := rejectedAt.Format("2006-01-02T15:04:05Z")
	if result.RejectedAt != wantRejectedAt {
		t.Errorf("RejectedAt = %q, want %q", result.RejectedAt, wantRejectedAt)
	}
	if result.AuthCode != "" || result.AuthorizedAt != "" {
		t.Errorf("auth fields not empty on a rejected transaction: %+v", result)
	}
}

func TestGetTransactionStatus_RejectedWithoutRejectionCode(t *testing.T) {
	// Estado inconsistente (no debería ocurrir en la práctica, ya que Reject()
	// siempre setea RejectionCode) — verifica el guard `tx.RejectionCode() != nil`.
	id := domain.NewTransactionID()
	params := baseParams(t, id)
	params.State = valueobject.StateRejected

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.RejectionCode != "" || result.RejectionReason != "" || result.RejectedAt != "" {
		t.Errorf("rejection fields = %+v, want all empty when the aggregate has no RejectionCode", result)
	}
}

func TestGetTransactionStatus_RejectedWithoutRejectedAt(t *testing.T) {
	id := domain.NewTransactionID()
	rejectionCode := valueobject.ISO_DO_NOT_HONOR
	params := baseParams(t, id)
	params.State = valueobject.StateRejected
	params.RejectionCode = &rejectionCode
	// RejectedAt deliberadamente sin setear.

	repo := &fakeRepo{findResult: aggregate.Reconstitute(params)}
	h := query.NewTransactionQueryHandler(repo)

	result, err := h.GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if result.RejectionCode != rejectionCode {
		t.Errorf("RejectionCode = %q, want %q", result.RejectionCode, rejectionCode)
	}
	if result.RejectedAt != "" {
		t.Errorf("RejectedAt = %q, want empty when the aggregate has no RejectedAt", result.RejectedAt)
	}
}

func TestGetTransactionStatus_InvalidTransactionReadsAsRejected(t *testing.T) {
	id := domain.NewTransactionID()
	rejectedAt := time.Date(2026, 11, 6, 15, 0, 4, 0, time.UTC)
	repo := &fakeRepo{
		findErr: pkgerrors.NewNotFoundError("Transaction", id.String()),
		invalidResult: &valueobject.InvalidTransactionRejection{
			TransactionID:   id,
			AmountCents:     5000,
			Currency:        "ARS",
			Rejection:       valueobject.NewRejectionFromValidation(valueobject.CodeInvalidData),
			ValidationError: "invalid card_network: unknown value",
			RejectedAt:      rejectedAt,
		},
	}

	got, err := query.NewTransactionQueryHandler(repo).GetTransactionStatus(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if got.State != "REJECTED" || got.RejectionCode != valueobject.CodeInvalidData ||
		got.AmountCents != 5000 || got.RejectedAt != "2026-11-06T15:00:04Z" {
		t.Errorf("result = %+v", got)
	}
	if strings.Contains(got.RejectionReason, "card_network") {
		t.Errorf("RejectionReason = %q: el motivo exacto no se expone", got.RejectionReason)
	}
}

func TestGetTransactionStatus_UnknownIDIsNotFound(t *testing.T) {
	id := domain.NewTransactionID()
	repo := &fakeRepo{findErr: pkgerrors.NewNotFoundError("Transaction", id.String())}

	_, err := query.NewTransactionQueryHandler(repo).GetTransactionStatus(context.Background(), id)

	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Errorf("error = %v, want NotFoundError", err)
	}
}

func TestGetTransactionStatus_InvalidLookupErrorIsNotNotFound(t *testing.T) {
	id := domain.NewTransactionID()
	repo := &fakeRepo{
		findErr:    pkgerrors.NewNotFoundError("Transaction", id.String()),
		invalidErr: errors.New("db down"),
	}

	_, err := query.NewTransactionQueryHandler(repo).GetTransactionStatus(context.Background(), id)

	var notFound *pkgerrors.NotFoundError
	if err == nil || errors.As(err, &notFound) {
		t.Errorf("error = %v, want a non-NotFound error", err)
	}
}
