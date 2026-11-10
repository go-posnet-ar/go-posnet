package command_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

type fakeTerminalDays struct {
	lateID     string
	lateErr    error
	lateAt     time.Time
	dayBatches int
	countErr   error
	counted    int
}

func (f *fakeTerminalDays) FindLateBatchTx(_ context.Context, _ pgx.Tx, _ domain.TerminalID, _ time.Time, _ string, authorizedAt time.Time) (string, error) {
	f.lateAt = authorizedAt
	return f.lateID, f.lateErr
}

func (f *fakeTerminalDays) CountTerminalDayBatches(context.Context, domain.TerminalID, time.Time, string) (int, error) {
	f.counted++
	return f.dayBatches, f.countErr
}

// lateClosed es un lote DISPUTED con una compra de 1000, cerrado contra un terminal que informó
// 2 ventas por 2000: le falta una de 1000.
func lateClosed(t *testing.T, closedAt time.Time) *aggregate.SettlementBatch {
	t.Helper()
	purchase, err := entity.NewBatchTransaction("b-late", domain.NewTransactionID(), 1000, "ARS", valueobject.BatchTxPurchase)
	if err != nil {
		t.Fatalf("NewBatchTransaction() error = %v", err)
	}
	count, amount := 2, int64(2000)
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "b-late", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: closedAt.Truncate(24 * time.Hour), State: valueobject.BatchStateDisputed, Currency: "ARS",
		Transactions: []*entity.BatchTransaction{purchase}, CreatedAt: closedAt.Add(-time.Hour), ClosedAt: &closedAt,
		Discrepancies: 1, ReconciledWith: aggregate.ReconciledWithTerminal, TerminalCount: &count, TerminalAmount: &amount,
	})
}

func TestRegisterApproval_LateSaleCompletesItsDisputedBatch(t *testing.T) {
	closedAt := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name        string
		amount      int64
		openReviews int
		wantState   valueobject.BatchState
		wantDiscrep int
		wantAudit   []string
		wantSubmit  bool
	}{
		{"reconciles: released and sent", 1000, 0, valueobject.BatchStateSubmitted, 0,
			[]string{repository.AuditActionLateSale, repository.AuditActionAutoRelease}, true},
		{"reconciles, but a review holds it", 1000, 1, valueobject.BatchStateDisputed, 0,
			[]string{repository.AuditActionLateSale}, false},
		{"another amount: still disputed", 700, 0, valueobject.BatchStateDisputed, 1,
			[]string{repository.AuditActionLateSale}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			late := lateClosed(t, closedAt)
			open := newOpenBatch(t, late.TerminalID(), late.MerchantID())
			repo := &fakeBatchRepo{findByIDResult: late, findOrCreateResult: open}
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := newHandler(pool, repo, &fakePublisher{}, processor)
			h.SetAuditLog(repo)
			h.SetReversalReviews(&fakeReviews{openCount: tc.openReviews})
			days := &fakeTerminalDays{lateID: "b-late"}
			h.SetTerminalDayBatches(days)

			cmd := validApprovalCmd()
			cmd.TerminalID, cmd.MerchantID, cmd.AmountCents = late.TerminalID().String(), late.MerchantID().String(), tc.amount
			cmd.AuthorizedAt = closedAt.Add(-time.Minute).Format(time.RFC3339)
			if err := h.RegisterApproval(context.Background(), cmd); err != nil {
				t.Fatalf("RegisterApproval() error = %v", err)
			}

			if !days.lateAt.Equal(closedAt.Add(-time.Minute)) {
				t.Errorf("searched with authorized_at %v, want the sale's", days.lateAt)
			}
			if len(late.Transactions()) != 2 || len(open.Transactions()) != 0 {
				t.Errorf("transactions: disputed %d, open %d, want 2 and 0", len(late.Transactions()), len(open.Transactions()))
			}
			if late.State() != tc.wantState || late.Discrepancies() != tc.wantDiscrep {
				t.Errorf("state = %s, discrepancies = %d, want %s, %d", late.State(), late.Discrepancies(), tc.wantState, tc.wantDiscrep)
			}
			var actions []string
			for _, e := range repo.auditEntries {
				if e.OperatorID != "system" || e.BatchID != "b-late" {
					t.Errorf("audit entry = %+v, want system on b-late", e)
				}
				actions = append(actions, e.Action)
			}
			if len(actions) != len(tc.wantAudit) || (len(actions) > 0 && actions[0] != tc.wantAudit[0]) || (len(actions) > 1 && actions[1] != tc.wantAudit[1]) {
				t.Errorf("audit = %v, want %v", actions, tc.wantAudit)
			}
			if (processor.calls == 1) != tc.wantSubmit {
				t.Errorf("processor calls = %d, want a submit: %v", processor.calls, tc.wantSubmit)
			}
		})
	}
}

// Sin un lote en disputa que la espere, o si la venta no le corresponde, abre o usa el lote OPEN.
func TestRegisterApproval_SaleWithoutDisputedBatchGoesToTheOpenOne(t *testing.T) {
	closedAt := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name   string
		lateID string
		at     time.Time
	}{
		{"no disputed batch", "", closedAt.Add(-time.Minute)},
		{"authorized after the close", "b-late", closedAt.Add(time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			late := lateClosed(t, closedAt)
			open := newOpenBatch(t, late.TerminalID(), late.MerchantID())
			repo := &fakeBatchRepo{findByIDResult: late, findOrCreateResult: open}
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
			h.SetAuditLog(repo)
			h.SetTerminalDayBatches(&fakeTerminalDays{lateID: tc.lateID})

			cmd := validApprovalCmd()
			cmd.AuthorizedAt = tc.at.Format(time.RFC3339)
			if err := h.RegisterApproval(context.Background(), cmd); err != nil {
				t.Fatalf("RegisterApproval() error = %v", err)
			}
			if len(open.Transactions()) != 1 || len(late.Transactions()) != 1 || len(repo.auditEntries) != 0 {
				t.Errorf("open %d, disputed %d, audit %d, want 1, 1, 0", len(open.Transactions()), len(late.Transactions()), len(repo.auditEntries))
			}
		})
	}
}

// Sin authorized_at legible, la hora es la de llegada: no se puede saber si era del lote cerrado.
func TestRegisterApproval_UnknownAuthorizationTimeIsNotLate(t *testing.T) {
	open := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{findOrCreateResult: open}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	days := &fakeTerminalDays{lateID: "b-late"}
	h.SetTerminalDayBatches(days)

	cmd := validApprovalCmd()
	cmd.AuthorizedAt = "ayer"
	if err := h.RegisterApproval(context.Background(), cmd); err != nil {
		t.Fatalf("RegisterApproval() error = %v", err)
	}
	if !days.lateAt.IsZero() || len(open.Transactions()) != 1 {
		t.Errorf("searched a disputed batch = %v, open transactions = %d", !days.lateAt.IsZero(), len(open.Transactions()))
	}
}

// ─── Cierre sin lote ──────────────────────────────────────────────────────────

type messageRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *messageRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *messageRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, rec.Level.String()+" "+rec.Message)
	return nil
}
func (r *messageRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *messageRecorder) WithGroup(string) slog.Handler      { return r }

func (r *messageRecorder) has(msg string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.msgs {
		if m == msg {
			return true
		}
	}
	return false
}

func recordLogs(t *testing.T) *messageRecorder {
	t.Helper()
	rec := &messageRecorder{}
	prev := slog.Default()
	slog.SetDefault(slog.New(rec))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return rec
}

func TestProcessBatchClose_WithoutBatchSaysWhy(t *testing.T) {
	tests := []struct {
		name       string
		dayBatches int
		count      int
		amount     int64
		want       string
	}{
		{"the day's batches are closed", 1, 2, 3000, "WARN no open batch for the terminal — the day's batches are already closed"},
		{"sales without any batch", 0, 2, 3000, "ERROR terminal reported sales for a day without any batch — they may still be retrying, or be lost"},
		{"a day without sales", 0, 0, 0, "INFO no open batch for the terminal and no sales reported — nothing to close"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := recordLogs(t)
			h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
			days := &fakeTerminalDays{dayBatches: tc.dayBatches}
			h.SetTerminalDayBatches(days)

			cmd := validProcessCloseCmd()
			cmd.TerminalCount, cmd.TerminalAmount = tc.count, tc.amount
			if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
				t.Fatalf("ProcessBatchClose() error = %v, want nil: the event is acknowledged", err)
			}
			if days.counted != 1 || !logs.has(tc.want) {
				t.Errorf("counted = %d, logs = %v, want %q", days.counted, logs.msgs, tc.want)
			}
		})
	}
}

// lateReversalClosed es un lote DISPUTED con compras de 1000 y 500, cerrado contra un terminal
// que informó 3 transacciones por 1000 neto: le falta la reversa de la de 500.
func lateReversalClosed(t *testing.T, closedAt time.Time) (*aggregate.SettlementBatch, domain.TransactionID) {
	t.Helper()
	reversed := domain.NewTransactionID()
	var txs []*entity.BatchTransaction
	for _, p := range []struct {
		id     domain.TransactionID
		amount int64
	}{{domain.NewTransactionID(), 1000}, {reversed, 500}} {
		tx, err := entity.NewBatchTransaction("b-late", p.id, p.amount, "ARS", valueobject.BatchTxPurchase)
		if err != nil {
			t.Fatalf("NewBatchTransaction() error = %v", err)
		}
		txs = append(txs, tx)
	}
	count, amount := 3, int64(1000)
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "b-late", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: closedAt.Truncate(24 * time.Hour), State: valueobject.BatchStateDisputed, Currency: "ARS",
		Transactions: txs, CreatedAt: closedAt.Add(-time.Hour), ClosedAt: &closedAt,
		Discrepancies: 1, ReconciledWith: aggregate.ReconciledWithTerminal, TerminalCount: &count, TerminalAmount: &amount,
	}), reversed
}

func TestRegisterReversal_LateReversalCompletesItsDisputedBatch(t *testing.T) {
	closedAt := time.Now().UTC().Truncate(time.Second)
	tests := []struct {
		name        string
		purchase    bool // La compra se encuentra y coincide
		amount      int64
		wantState   valueobject.BatchState
		wantDiscrep int
		wantAudit   []string
		wantReview  bool
	}{
		{"reconciles: released and sent", true, 500, valueobject.BatchStateSubmitted, 0,
			[]string{repository.AuditActionLateReversal, repository.AuditActionAutoRelease}, false},
		{"reconciles, but does not match its purchase: held", false, 500, valueobject.BatchStateDisputed, 0,
			[]string{repository.AuditActionLateReversal}, true},
		{"another amount: still disputed", true, 300, valueobject.BatchStateDisputed, 1,
			[]string{repository.AuditActionLateReversal}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			late, reversed := lateReversalClosed(t, closedAt)
			open := newOpenBatch(t, late.TerminalID(), late.MerchantID())
			repo := &fakeBatchRepo{findByIDResult: late, findOrCreateResult: open}
			pool := newMockPool(t)
			expectClaimed(pool, 1)
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := newHandler(pool, repo, &fakePublisher{}, processor)
			h.SetAuditLog(repo)
			reviews := &fakeReviews{}
			if tc.purchase {
				reviews.purchase = &repository.ReversalPurchase{MerchantID: late.MerchantID().String(), AmountCents: 500, Currency: "ARS"}
			}
			h.SetReversalReviews(reviews)
			days := &fakeTerminalDays{lateID: "b-late"}
			h.SetTerminalDayBatches(days)

			cmd := validReversalCmd()
			cmd.TerminalID, cmd.MerchantID, cmd.OriginalTransactionID = late.TerminalID().String(), late.MerchantID().String(), reversed.String()
			cmd.AmountCents = tc.amount
			cmd.CompletedAt = closedAt.Add(-time.Minute).Format(time.RFC3339)
			if err := h.RegisterReversal(context.Background(), cmd); err != nil {
				t.Fatalf("RegisterReversal() error = %v", err)
			}

			if len(late.Transactions()) != 3 || len(open.Transactions()) != 0 {
				t.Errorf("transactions: disputed %d, open %d, want 3 and 0", len(late.Transactions()), len(open.Transactions()))
			}
			if late.State() != tc.wantState || late.Discrepancies() != tc.wantDiscrep {
				t.Errorf("state = %s, discrepancies = %d, want %s, %d", late.State(), late.Discrepancies(), tc.wantState, tc.wantDiscrep)
			}
			var actions []string
			for _, e := range repo.auditEntries {
				actions = append(actions, e.Action)
			}
			if len(actions) != len(tc.wantAudit) || actions[0] != tc.wantAudit[0] || (len(actions) > 1 && actions[1] != tc.wantAudit[1]) {
				t.Errorf("audit = %v, want %v", actions, tc.wantAudit)
			}
			if (len(reviews.inserted) == 1) != tc.wantReview {
				t.Errorf("reviews = %+v, want a review: %v", reviews.inserted, tc.wantReview)
			}
			if tc.wantReview && reviews.inserted[0].BatchID != "b-late" {
				t.Errorf("review batch = %q, want the disputed batch", reviews.inserted[0].BatchID)
			}
			if (processor.calls == 1) != (tc.wantState == valueobject.BatchStateSubmitted) {
				t.Errorf("processor calls = %d", processor.calls)
			}
		})
	}
}

// Una reversa completada después del cierre, o sin hora conocida, va al lote OPEN como siempre.
func TestRegisterReversal_NotLateGoesToTheOpenBatch(t *testing.T) {
	closedAt := time.Now().UTC().Truncate(time.Second)
	for _, completedAt := range []string{closedAt.Add(time.Minute).Format(time.RFC3339), "not-a-time"} {
		late, reversed := lateReversalClosed(t, closedAt)
		open := newOpenBatch(t, late.TerminalID(), late.MerchantID())
		repo := &fakeBatchRepo{findByIDResult: late, findOrCreateResult: open}
		pool := newMockPool(t)
		expectClaimed(pool, 1)
		h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
		h.SetAuditLog(repo)
		h.SetTerminalDayBatches(&fakeTerminalDays{lateID: "b-late"})

		cmd := validReversalCmd()
		cmd.TerminalID, cmd.MerchantID, cmd.OriginalTransactionID = late.TerminalID().String(), late.MerchantID().String(), reversed.String()
		cmd.AmountCents, cmd.CompletedAt = 500, completedAt
		if err := h.RegisterReversal(context.Background(), cmd); err != nil {
			t.Fatalf("RegisterReversal(%s) error = %v", completedAt, err)
		}
		if len(late.Transactions()) != 2 || len(open.Transactions()) != 1 {
			t.Errorf("%s: disputed %d, open %d, want 2 and 1", completedAt, len(late.Transactions()), len(open.Transactions()))
		}
	}
}
