package command_test

import (
	"context"
	"errors"
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

// sentReview es una revisión de una reversa de 500 en un lote ya enviado.
func sentReview(state string, purchase *repository.ReversalPurchase) *repository.StoredReversalReview {
	r := storedReview(repository.ReversalReviewAmountMismatch, state, purchase)
	r.TransactionID = domain.NewTransactionID().String()
	return r
}

// Corregir una reversa de un lote ya enviado agrega un ajuste, con signo, al lote abierto del
// mismo terminal. El lote enviado no cambia.
func TestResolveReversalReview_SentBatchBecomesAnAdjustment(t *testing.T) {
	purchase := &repository.ReversalPurchase{AmountCents: 900, Currency: "ARS"}
	tests := []struct {
		name     string
		action   string
		state    string
		excluded bool
		want     int64
	}{
		// Se descontaron 500 y la compra era de 900: falta descontar 400.
		{"USE_PURCHASE_AMOUNT on a submitted batch", repository.ReviewActionUsePurchaseAmount, "SUBMITTED", false, -400},
		// La reversa no debía descontarse: vuelven los 500.
		{"EXCLUDE on a settled batch", repository.ReviewActionExclude, "SETTLED", false, 500},
		// La reversa estaba excluida y la compra de 900 ya se liquidó entera.
		{"USE_PURCHASE_AMOUNT on an excluded reversal", repository.ReviewActionUsePurchaseAmount, "SETTLED", true, -900},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			review := sentReview(tc.state, purchase)
			if tc.excluded {
				review.Action = repository.ReviewActionExclude
			}
			reviews := &fakeReviews{stored: review}
			h, repo := reviewAdmin(t, reviews, true)
			sent := heldBatch(t, valueobject.BatchStateSettled, aggregate.ReconciledWithTerminal, 1, 1000)
			open := newOpenBatch(t, sent.TerminalID(), sent.MerchantID())
			repo.findByIDResult, repo.findOrCreateResult = sent, open
			cmd := resolveCmd(tc.action)
			cmd.TransactionID = review.TransactionID

			if err := h.ResolveReversalReview(context.Background(), cmd); err != nil {
				t.Fatalf("ResolveReversalReview() error = %v", err)
			}
			var adjustments []int64
			for _, tx := range open.Transactions() {
				if tx.Type() == valueobject.BatchTxAdjustment {
					adjustments = append(adjustments, tx.AmountCents())
				}
			}
			if len(adjustments) != 1 || adjustments[0] != tc.want {
				t.Errorf("adjustments in the open batch = %v, want [%d]", adjustments, tc.want)
			}
			if repo.findOrCreateCurrency != "ARS" {
				t.Errorf("open batch currency = %q, want the sent batch's", repo.findOrCreateCurrency)
			}
			if reviews.updateCalls+reviews.deleteCalls != 0 || len(reviews.insertedReversals) != 0 {
				t.Errorf("the sent batch must not change: updates %d, deletes %d, inserts %v", reviews.updateCalls, reviews.deleteCalls, reviews.insertedReversals)
			}
			if len(repo.auditEntries) != 2 || repo.auditEntries[0].Action != repository.AuditActionAdjustment || repo.auditEntries[0].BatchID != open.ID() {
				t.Errorf("audit = %+v, want an ADJUSTMENT on the open batch and the review", repo.auditEntries)
			}
		})
	}
}

// Un lote CLOSED todavía no se envió: no lleva ajuste, se corrige pasándolo a DISPUTED.
func TestResolveReversalReview_ClosedBatchIsRejected(t *testing.T) {
	reviews := &fakeReviews{stored: sentReview("CLOSED", &repository.ReversalPurchase{AmountCents: 900, Currency: "ARS"})}
	h, repo := reviewAdmin(t, reviews, false)

	err := h.ResolveReversalReview(context.Background(), resolveCmd(repository.ReviewActionExclude))
	wantValidation(t, err)
	if len(repo.savedBatches) != 0 {
		t.Errorf("saved = %d batches, want none", len(repo.savedBatches))
	}
}

// Una revisión ya cerrada solo se vuelve a resolver para corregir un lote enviado, si se había
// aceptado la reversa, y una sola vez.
func TestResolveReversalReview_CorrectsAnAcceptedReviewOnceItsBatchWasSent(t *testing.T) {
	purchase := &repository.ReversalPurchase{AmountCents: 900, Currency: "ARS"}
	tests := []struct {
		name       string
		state      string
		stored     string // acción guardada al cerrarla
		action     string
		wantAdjust bool
	}{
		{"accepted, batch submitted", "SUBMITTED", repository.ReviewActionAccept, repository.ReviewActionUsePurchaseAmount, true},
		{"resolved by the system, batch settled", "SETTLED", "", repository.ReviewActionExclude, true},
		{"accepted, batch not sent yet", "OPEN", repository.ReviewActionAccept, repository.ReviewActionUsePurchaseAmount, false},
		{"already corrected", "SUBMITTED", repository.ReviewActionUsePurchaseAmount, repository.ReviewActionUsePurchaseAmount, false},
		{"accepting again", "SUBMITTED", repository.ReviewActionAccept, repository.ReviewActionAccept, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			review := sentReview(tc.state, purchase)
			review.Resolved, review.Action = true, tc.stored
			reviews := &fakeReviews{stored: review}
			h, repo := reviewAdmin(t, reviews, tc.wantAdjust)
			sent := heldBatch(t, valueobject.BatchStateSubmitted, aggregate.ReconciledWithTerminal, 1, 1000)
			open := newOpenBatch(t, sent.TerminalID(), sent.MerchantID())
			repo.findByIDResult, repo.findOrCreateResult = sent, open
			cmd := resolveCmd(tc.action)
			cmd.TransactionID = review.TransactionID

			err := h.ResolveReversalReview(context.Background(), cmd)
			if !tc.wantAdjust {
				wantValidation(t, err)
				if len(open.Transactions()) != 0 {
					t.Errorf("open batch got %d transactions, want none", len(open.Transactions()))
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveReversalReview() error = %v", err)
			}
			if len(open.Transactions()) != 1 || open.Transactions()[0].Type() != valueobject.BatchTxAdjustment {
				t.Errorf("open batch transactions = %v, want one adjustment", open.Transactions())
			}
			if reviews.closedAction != tc.action {
				t.Errorf("stored action = %q, want %q: a second correction must be rejected", reviews.closedAction, tc.action)
			}
		})
	}
}

// El job reintenta cada lote retenido que quedó sin revisiones abiertas: el que concilia se libera
// y se envía.
func TestReleaseHeldBatches_RetriesEachBatch(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, 1, 1000)
	pool := newMockPool(t)
	expectReleaseTx(pool)
	repo := &fakeBatchRepo{findByIDResult: held}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	h := newHandler(pool, repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{releasable: []string{"b-1"}})
	h.SetAuditLog(repo)

	retried, failed, err := h.ReleaseHeldBatches(context.Background())
	if err != nil || retried != 1 || failed != 0 {
		t.Fatalf("ReleaseHeldBatches() = %d, %d, %v, want 1, 0, nil", retried, failed, err)
	}
	if held.State() != valueobject.BatchStateSubmitted || processor.calls != 1 {
		t.Errorf("state = %s, processor calls = %d, want SUBMITTED and 1", held.State(), processor.calls)
	}
}

// Si la liberación vuelve a fallar, se cuenta y el lote sigue DISPUTED para el barrido siguiente.
func TestReleaseHeldBatches_CountsFailures(t *testing.T) {
	repo := &fakeBatchRepo{findByIDErr: errors.New("db down")}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})
	h.SetReversalReviews(&fakeReviews{releasable: []string{"b-1", "b-2"}})

	retried, failed, err := h.ReleaseHeldBatches(context.Background())
	if err != nil || retried != 2 || failed != 2 {
		t.Fatalf("ReleaseHeldBatches() = %d, %d, %v, want 2, 2, nil", retried, failed, err)
	}
}

func TestReleaseHeldBatches_ListError(t *testing.T) {
	h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	h.SetReversalReviews(&fakeReviews{releasableErr: errors.New("db down")})

	if _, _, err := h.ReleaseHeldBatches(context.Background()); err == nil {
		t.Fatal("ReleaseHeldBatches() error = nil, want error")
	}
}

// El caso de la brecha: el procesador aceptó el lote, una revisión lo retuvo mientras se enviaba y
// sus totales cambiaron. Al liberarse, queda CLOSED: no se reenvía ni se marca SUBMITTED con la
// confirmación anterior.
func TestReleaseHeldBatch_SentWithOtherTotalsStaysClosed(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, 0, 0)
	pool := newMockPool(t)
	expectReleaseTx(pool)
	repo := &fakeBatchRepo{findByIDResult: held}
	processor := &fakeProcessor{confirmationID: "conf-2"}
	h := newHandler(pool, repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{})
	h.SetAuditLog(repo)
	h.SetBatchSubmissions(&fakeSubmissions{previous: "conf-1", sent: &repository.SubmissionTotals{TotalCount: 2, TotalAmount: 500}})

	if err := h.ReleaseHeldBatch(context.Background(), "b-1"); err != nil {
		t.Fatalf("ReleaseHeldBatch() error = %v", err)
	}
	if held.State() != valueobject.BatchStateClosed || processor.calls != 0 {
		t.Errorf("state = %s, processor calls = %d, want CLOSED and 0", held.State(), processor.calls)
	}
}

// Una revisión que excluye la única reversa deja el lote vacío: al liberarse queda VOID, sin
// enviarse, y la auditoría lo dice.
func TestReleaseHeldBatch_EmptyBatchIsVoided(t *testing.T) {
	empty := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "b-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC), State: valueobject.BatchStateDisputed, Currency: "ARS",
		CreatedAt: time.Now(), ReconciledWith: aggregate.ReconciledWithBackend,
	})
	pool := newMockPool(t)
	expectReleaseTx(pool)
	repo := &fakeBatchRepo{findByIDResult: empty}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	h := newHandler(pool, repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{})
	h.SetAuditLog(repo)

	if err := h.ReleaseHeldBatch(context.Background(), "b-1"); err != nil {
		t.Fatalf("ReleaseHeldBatch() error = %v", err)
	}
	if empty.State() != valueobject.BatchStateVoid || processor.calls != 0 {
		t.Errorf("state = %s, processor calls = %d, want VOID and 0", empty.State(), processor.calls)
	}
	if len(repo.auditEntries) != 1 || repo.auditEntries[0].ToState != "VOID" {
		t.Errorf("audit = %+v, want an AUTO_RELEASE entry to VOID", repo.auditEntries)
	}
}
