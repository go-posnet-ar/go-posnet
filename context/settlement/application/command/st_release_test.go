package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// heldBatch es un lote con una compra de 1000, retenido por una revisión que ya se cerró.
func heldBatch(t *testing.T, state valueobject.BatchState, reconciledWith string, terminalCount int, terminalAmount int64) *aggregate.SettlementBatch {
	t.Helper()
	purchase, err := entity.NewBatchTransaction("b-1", domain.NewTransactionID(), 1000, "ARS", valueobject.BatchTxPurchase)
	if err != nil {
		t.Fatalf("NewBatchTransaction() error = %v", err)
	}
	p := aggregate.ReconstituteParams{
		ID: "b-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC), State: state, Currency: "ARS",
		Transactions: []*entity.BatchTransaction{purchase}, CreatedAt: time.Now(), ReconciledWith: reconciledWith,
	}
	if reconciledWith == aggregate.ReconciledWithTerminal {
		p.TerminalCount, p.TerminalAmount = &terminalCount, &terminalAmount
	}
	return aggregate.Reconstitute(p)
}

func expectReleaseTx(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectCommit()
	pool.ExpectRollback()
}

func TestReleaseHeldBatch(t *testing.T) {
	tests := []struct {
		name         string
		batch        *aggregate.SettlementBatch
		openReviews  int
		wantTx       bool
		wantState    valueobject.BatchState
		wantAudit    bool
		wantSubmit   bool
		wantDiscrepa int
	}{
		{"reconciles against the backend: released and sent", heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, 0, 0), 0, true, valueobject.BatchStateSubmitted, true, true, 0},
		{"terminal totals match: released and sent", heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, 1, 1000), 0, true, valueobject.BatchStateSubmitted, true, true, 0},
		{"terminal totals no longer match: stays disputed", heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, 2, 0), 0, true, valueobject.BatchStateDisputed, false, false, 1},
		{"another review is still open", heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, 0, 0), 1, false, valueobject.BatchStateDisputed, false, false, 0},
		{"closed before the reference was recorded", heldBatch(t, valueobject.BatchStateDisputed, "", 0, 0), 0, false, valueobject.BatchStateDisputed, false, false, 0},
		{"not disputed", heldBatch(t, valueobject.BatchStateOpen, aggregate.ReconciledWithBackend, 0, 0), 0, false, valueobject.BatchStateOpen, false, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool := newMockPool(t)
			if tc.wantTx {
				expectReleaseTx(pool)
			}
			repo := &fakeBatchRepo{findByIDResult: tc.batch}
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := newHandler(pool, repo, &fakePublisher{}, processor)
			h.SetReversalReviews(&fakeReviews{openCount: tc.openReviews})
			h.SetAuditLog(repo)

			if err := h.ReleaseHeldBatch(context.Background(), "b-1"); err != nil {
				t.Fatalf("ReleaseHeldBatch() error = %v", err)
			}
			if tc.batch.State() != tc.wantState || tc.batch.Discrepancies() != tc.wantDiscrepa {
				t.Errorf("state = %s, discrepancies = %d, want %s, %d", tc.batch.State(), tc.batch.Discrepancies(), tc.wantState, tc.wantDiscrepa)
			}
			if got := len(repo.auditEntries) == 1 && repo.auditEntries[0].Action == repository.AuditActionAutoRelease &&
				repo.auditEntries[0].OperatorID == "system" && repo.auditEntries[0].ToState == "CLOSED"; got != tc.wantAudit {
				t.Errorf("audit = %+v, want an AUTO_RELEASE entry: %v", repo.auditEntries, tc.wantAudit)
			}
			if (processor.calls == 1) != tc.wantSubmit {
				t.Errorf("processor calls = %d, want a submit: %v", processor.calls, tc.wantSubmit)
			}
		})
	}
}

// Cerrar la última revisión de una reversa que esperaba su compra libera el lote que retenía.
func TestRegisterApproval_ReleasesTheBatchItWasHolding(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, 0, 0)
	purchaseBatch := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: purchaseBatch, findByIDResult: held}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	expectReleaseTx(pool)
	reviews := &fakeReviews{reconcileResult: repository.ReviewReconciliation{Resolved: true, BatchID: "b-1"}}
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "conf-1"})
	h.SetReversalReviews(reviews)

	if err := h.RegisterApproval(context.Background(), validApprovalCmd()); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}
	if reviews.reconcileBatch != purchaseBatch.ID() {
		t.Errorf("purchase batch passed to the reconcile = %q, want %q", reviews.reconcileBatch, purchaseBatch.ID())
	}
	if held.State() != valueobject.BatchStateSubmitted {
		t.Errorf("held batch = %s, want SUBMITTED", held.State())
	}
}

type fakeReleaser struct{ released []string }

func (f *fakeReleaser) ReleaseHeldBatch(_ context.Context, batchID string) error {
	f.released = append(f.released, batchID)
	return nil
}

// Una revisión reabierta porque la compra de una reversa excluida no coincide: la reversa no está
// en ningún lote.
func excludedReview() *repository.StoredReversalReview {
	r := storedReview(repository.ReversalReviewAmountMismatch, "OPEN", &repository.ReversalPurchase{AmountCents: 900, Currency: "ARS"})
	r.Action = repository.ReviewActionExclude
	return r
}

func TestResolveReversalReview_ExcludedReversal(t *testing.T) {
	tests := []struct {
		name         string
		action       string
		wantErr      bool
		wantInserted []string
		wantStored   string
	}{
		{"USE_PURCHASE_AMOUNT puts it back with the purchase amount", repository.ReviewActionUsePurchaseAmount, false, []string{"b-1|tx-1|900|ARS"}, repository.ReviewActionUsePurchaseAmount},
		{"ACCEPT keeps it excluded", repository.ReviewActionAccept, false, nil, repository.ReviewActionExclude},
		{"EXCLUDE does not apply", repository.ReviewActionExclude, true, nil, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reviews := &fakeReviews{stored: excludedReview()}
			h, _ := reviewAdmin(t, reviews, !tc.wantErr)
			releaser := &fakeReleaser{}
			h.SetBatchReleaser(releaser)

			err := h.ResolveReversalReview(context.Background(), resolveCmd(tc.action))
			if tc.wantErr {
				wantValidation(t, err)
				if len(releaser.released) != 0 {
					t.Errorf("released = %v, want nothing after a failed resolution", releaser.released)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveReversalReview() error = %v", err)
			}
			if len(reviews.insertedReversals) != len(tc.wantInserted) || (len(tc.wantInserted) == 1 && reviews.insertedReversals[0] != tc.wantInserted[0]) {
				t.Errorf("inserted = %v, want %v", reviews.insertedReversals, tc.wantInserted)
			}
			if reviews.deleteCalls != 0 || reviews.updateCalls != 0 {
				t.Errorf("deletes, updates = %d, %d, want none: the reversal is not in a batch", reviews.deleteCalls, reviews.updateCalls)
			}
			if reviews.closedAction != tc.wantStored {
				t.Errorf("stored action = %q, want %q", reviews.closedAction, tc.wantStored)
			}
			if len(releaser.released) != 1 || releaser.released[0] != "b-1" {
				t.Errorf("released = %v, want [b-1]", releaser.released)
			}
		})
	}
}
