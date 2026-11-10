package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

func openBatchWithPurchase(t *testing.T, amount int64) (*aggregate.SettlementBatch, domain.TerminalID) {
	t.Helper()
	terminal := domain.NewTerminalID()
	b := newOpenBatch(t, terminal, domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), amount, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	return b, terminal
}

func closeCmd(terminal domain.TerminalID, count int, amount int64) port.ProcessBatchCloseCommand {
	return port.ProcessBatchCloseCommand{
		EventID: "evt-close", TerminalID: terminal.String(), MerchantID: domain.NewMerchantID().String(),
		TerminalCount: count, TerminalAmount: amount, Currency: "ARS",
	}
}

func wantValidation(t *testing.T, err error) {
	t.Helper()
	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v (%T), want *pkgerrors.ValidationError", err, err)
	}
}

// ─── Cierre con contadores acumulados ─────────────────────────────────────────

func TestProcessBatchClose_CumulativeTotals(t *testing.T) {
	cases := []struct {
		name       string
		count      int
		amount     int64
		priorCount int
		priorTotal int64
		wantState  valueobject.BatchState
	}{
		{"informa solo lo pendiente", 1, 1000, 2, 3000, valueobject.BatchStateSubmitted},
		{"informa el acumulado exacto", 3, 4000, 2, 3000, valueobject.BatchStateSubmitted},
		{"acumulado con otro monto", 3, 3999, 2, 3000, valueobject.BatchStateDisputed},
		{"acumulado con otra cantidad", 4, 4000, 2, 3000, valueobject.BatchStateDisputed},
		{"sin cierres anteriores", 3, 4000, 0, 0, valueobject.BatchStateDisputed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			batch, terminal := openBatchWithPurchase(t, 1000)
			repo := &fakeBatchRepo{findOpenResult: batch, priorCount: tc.priorCount, priorAmount: tc.priorTotal}
			pool := newMockPool(t)
			expectClaimedWithEvent(pool)

			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "c-1"})
			if err := h.ProcessBatchClose(context.Background(), closeCmd(terminal, tc.count, tc.amount)); err != nil {
				t.Fatalf("ProcessBatchClose() error = %v", err)
			}
			if batch.State() != tc.wantState {
				t.Errorf("state = %s, want %s", batch.State(), tc.wantState)
			}
		})
	}
}

func TestProcessBatchClose_PriorTotalsErrorIsRetried(t *testing.T) {
	batch, terminal := openBatchWithPurchase(t, 1000)
	repo := &fakeBatchRepo{findOpenResult: batch, priorErr: errors.New("db down")}

	err := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{}).
		ProcessBatchClose(context.Background(), closeCmd(terminal, 9, 9))
	if err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want an error")
	}
	var ve *pkgerrors.ValidationError
	if errors.As(err, &ve) {
		t.Fatalf("error = %v, want a transient error: a validation error would lose the close", err)
	}
	if batch.State() != valueobject.BatchStateOpen {
		t.Errorf("state = %s, want OPEN", batch.State())
	}
}

// ─── Una revisión abierta frena el envío ──────────────────────────────────────

func TestProcessBatchClose_OpenReviewHoldsBatch(t *testing.T) {
	batch, terminal := openBatchWithPurchase(t, 1000)
	repo := &fakeBatchRepo{findOpenResult: batch}
	processor := &fakeProcessor{confirmationID: "c-1"}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)

	h := newHandler(pool, repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{openCount: 1})
	if err := h.ProcessBatchClose(context.Background(), closeCmd(terminal, 1, 1000)); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}
	if batch.State() != valueobject.BatchStateDisputed {
		t.Errorf("state = %s, want DISPUTED", batch.State())
	}
	if batch.Discrepancies() != 0 {
		t.Errorf("discrepancies = %d, want 0: the terminal totals matched", batch.Discrepancies())
	}
	if processor.calls != 0 {
		t.Errorf("processor calls = %d, want 0", processor.calls)
	}
}

func TestProcessBatchClose_ReviewCountErrorIsRetried(t *testing.T) {
	batch, terminal := openBatchWithPurchase(t, 1000)
	processor := &fakeProcessor{}

	h := newHandler(newMockPool(t), &fakeBatchRepo{findOpenResult: batch}, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{openErr: errors.New("db down")})
	if err := h.ProcessBatchClose(context.Background(), closeCmd(terminal, 1, 1000)); err == nil {
		t.Fatal("ProcessBatchClose() error = nil: without the count, the batch could be sent")
	}
	if processor.calls != 0 || batch.State() != valueobject.BatchStateOpen {
		t.Errorf("calls = %d, state = %s, want 0 and OPEN", processor.calls, batch.State())
	}
}

func TestCloseStaleBatches_OpenReviewHoldsBatch(t *testing.T) {
	batch, _ := openBatchWithPurchase(t, 1000)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	processor := &fakeProcessor{confirmationID: "c-1"}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)

	h := newHandler(pool, repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{openCount: 2})
	closed, failed, err := h.CloseStaleBatches(context.Background(), time.Now())
	if err != nil || closed != 1 || failed != 0 {
		t.Fatalf("CloseStaleBatches() = %d, %d, %v", closed, failed, err)
	}
	if batch.State() != valueobject.BatchStateDisputed || processor.calls != 0 {
		t.Errorf("state = %s, calls = %d, want DISPUTED and 0", batch.State(), processor.calls)
	}
}

func closedBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, _ := openBatchWithPurchase(t, 1000)
	if err := b.CloseWithBackendTotals(); err != nil {
		t.Fatalf("CloseWithBackendTotals() error = %v", err)
	}
	return b
}

func TestResubmit_OpenReviewMovesToDisputed(t *testing.T) {
	batch := closedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: batch}
	processor := &fakeProcessor{confirmationID: "c-1"}
	h := command.NewAdminHandler(repo, processor)
	h.SetReversalReviews(&fakeReviews{openCount: 1})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: batch.ID(), OperatorID: "op-1"})
	wantValidation(t, err)
	if batch.State() != valueobject.BatchStateDisputed || processor.calls != 0 {
		t.Errorf("state = %s, calls = %d, want DISPUTED and 0", batch.State(), processor.calls)
	}
}

func TestResolveDispute_OpenReviewIsRejected(t *testing.T) {
	batch := closedBatch(t)
	if err := batch.MarkDisputed("x"); err != nil {
		t.Fatalf("MarkDisputed() error = %v", err)
	}
	h := command.NewAdminHandler(&fakeBatchRepo{findByIDResult: batch}, &fakeProcessor{})
	h.SetReversalReviews(&fakeReviews{openCount: 1})

	err := h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: batch.ID(), OperatorID: "op-1", Reason: "r"})
	wantValidation(t, err)
	if batch.State() != valueobject.BatchStateDisputed {
		t.Errorf("state = %s, want DISPUTED", batch.State())
	}
}

// ─── Cerrar una revisión ──────────────────────────────────────────────────────

func storedReview(reason, batchState string, purchase *repository.ReversalPurchase) *repository.StoredReversalReview {
	return &repository.StoredReversalReview{
		ReversalReview: repository.ReversalReview{TransactionID: "tx-1", BatchID: "b-1", Reason: reason, AmountCents: 500, Purchase: purchase},
		BatchState:     batchState,
	}
}

func reviewAdmin(t *testing.T, reviews *fakeReviews, commits bool) (*command.AdminHandler, *fakeBatchRepo) {
	t.Helper()
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if commits {
		pool.ExpectCommit()
	}
	pool.ExpectRollback()
	repo := &fakeBatchRepo{}
	h := command.NewAdminHandler(repo, &fakeProcessor{})
	h.ConfigureSettlement(pool, repo, &fakePublisher{}, outbox.NewStore(idempotencySchema), 2.5)
	h.SetReversalReviews(reviews)
	return h, repo
}

func resolveCmd(action string) port.ResolveReversalReviewCommand {
	return port.ResolveReversalReviewCommand{TransactionID: "tx-1", OperatorID: "op-1", Resolution: "ticket-1", Action: action}
}

func TestResolveReversalReview_Actions(t *testing.T) {
	purchase := &repository.ReversalPurchase{AmountCents: 900}
	cases := []struct {
		name       string
		action     string
		review     *repository.StoredReversalReview
		wantUpdate int
		wantDelete int
		wantAudit  string
	}{
		{"acepta en cualquier estado", repository.ReviewActionAccept, storedReview(repository.ReversalReviewAmountMismatch, "SETTLED", purchase), 0, 0, repository.AuditActionReviewAccept},
		{"usa el monto de la compra", repository.ReviewActionUsePurchaseAmount, storedReview(repository.ReversalReviewAmountMismatch, "DISPUTED", purchase), 1, 0, repository.AuditActionReviewUsePurchaseAmount},
		{"excluye de un lote abierto", repository.ReviewActionExclude, storedReview(repository.ReversalReviewMerchantMismatch, "OPEN", purchase), 0, 1, repository.AuditActionReviewExclude},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reviews := &fakeReviews{stored: tc.review}
			h, repo := reviewAdmin(t, reviews, true)

			if err := h.ResolveReversalReview(context.Background(), resolveCmd(tc.action)); err != nil {
				t.Fatalf("ResolveReversalReview() error = %v", err)
			}
			if reviews.updateCalls != tc.wantUpdate || reviews.deleteCalls != tc.wantDelete {
				t.Errorf("updates, deletes = %d, %d, want %d, %d", reviews.updateCalls, reviews.deleteCalls, tc.wantUpdate, tc.wantDelete)
			}
			if tc.wantUpdate == 1 && reviews.updateAmount != 900 {
				t.Errorf("amount = %d, want the purchase amount", reviews.updateAmount)
			}
			if reviews.closedBy != "op-1" || reviews.closedNote != "ticket-1" {
				t.Errorf("closed by %q, %q", reviews.closedBy, reviews.closedNote)
			}
			if len(repo.auditEntries) != 1 || repo.auditEntries[0].Action != tc.wantAudit || repo.auditEntries[0].Reason != "ticket-1" {
				t.Errorf("audit = %+v, want one %s", repo.auditEntries, tc.wantAudit)
			}
		})
	}
}

func TestResolveReversalReview_Rejected(t *testing.T) {
	purchase := &repository.ReversalPurchase{AmountCents: 900}
	resolved := storedReview(repository.ReversalReviewAmountMismatch, "OPEN", purchase)
	resolved.Resolved = true
	cases := []struct {
		name   string
		action string
		review *repository.StoredReversalReview
	}{
		{"ya resuelta", repository.ReviewActionAccept, resolved},
		{"lote cerrado", repository.ReviewActionUsePurchaseAmount, storedReview(repository.ReversalReviewAmountMismatch, "CLOSED", purchase)},
		{"monto de compra sin diferencia de monto", repository.ReviewActionUsePurchaseAmount, storedReview(repository.ReversalReviewMerchantMismatch, "OPEN", purchase)},
		{"monto de compra sin compra", repository.ReviewActionUsePurchaseAmount, storedReview(repository.ReversalReviewAmountMismatch, "OPEN", nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reviews := &fakeReviews{stored: tc.review}
			h, repo := reviewAdmin(t, reviews, false)

			wantValidation(t, h.ResolveReversalReview(context.Background(), resolveCmd(tc.action)))
			if reviews.updateCalls+reviews.deleteCalls != 0 || reviews.closedBy != "" || len(repo.auditEntries) != 0 {
				t.Errorf("nothing should change: %+v, audit %+v", reviews, repo.auditEntries)
			}
		})
	}
}

func TestResolveReversalReview_NotFound(t *testing.T) {
	h, _ := reviewAdmin(t, &fakeReviews{}, false)

	var nf *pkgerrors.NotFoundError
	if err := h.ResolveReversalReview(context.Background(), resolveCmd(repository.ReviewActionAccept)); !errors.As(err, &nf) {
		t.Fatalf("error = %v, want *pkgerrors.NotFoundError", err)
	}
}

func TestResolveReversalReview_InvalidRequest(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})
	h.SetReversalReviews(&fakeReviews{})
	base := resolveCmd(repository.ReviewActionAccept)
	cases := map[string]func(*port.ResolveReversalReviewCommand){
		"sin transacción": func(c *port.ResolveReversalReviewCommand) { c.TransactionID = "" },
		"sin operador":    func(c *port.ResolveReversalReviewCommand) { c.OperatorID = "" },
		"sin resolución":  func(c *port.ResolveReversalReviewCommand) { c.Resolution = "" },
		"acción inválida": func(c *port.ResolveReversalReviewCommand) { c.Action = "DELETE" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			cmd := base
			change(&cmd)
			wantValidation(t, h.ResolveReversalReview(context.Background(), cmd))
		})
	}
}

// ─── Zona horaria por comercio ────────────────────────────────────────────────

type fakeTimezones struct {
	byMerchant map[string]string
	err        error
	setErr     error
	set        []string
	locked     []string
	moved      []repository.RedatedBatch
}

func (f *fakeTimezones) MerchantTimezone(_ context.Context, merchantID string) (string, error) {
	return f.byMerchant[merchantID], f.err
}

func (f *fakeTimezones) MerchantTimezoneTx(_ context.Context, _ pgx.Tx, merchantID string) (string, error) {
	f.locked = append(f.locked, merchantID)
	return f.byMerchant[merchantID], f.err
}

func (f *fakeTimezones) SetMerchantTimezone(_ context.Context, merchantID, timezone, defaultTimezone, operatorID string) ([]repository.RedatedBatch, error) {
	f.set = append(f.set, merchantID+"|"+timezone+"|"+defaultTimezone+"|"+operatorID)
	return f.moved, f.setErr
}

// La zona anterior de un comercio sin zona propia es la general: el repositorio la necesita
// para saber qué lote es el de hoy.
func TestSetMerchantTimezone_PassesTheDefaultZone(t *testing.T) {
	loc, _ := time.LoadLocation("America/Argentina/Buenos_Aires")
	merchant := domain.NewMerchantID().String()
	timezones := &fakeTimezones{moved: []repository.RedatedBatch{{BatchID: "b1", FromDate: "2026-11-08", ToDate: "2026-11-09"}}}
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})
	h.SetMerchantTimezones(timezones)
	h.SetBusinessLocation(loc)

	cmd := port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Asia/Tokyo", OperatorID: "op"}
	if err := h.SetMerchantTimezone(context.Background(), cmd); err != nil {
		t.Fatalf("error = %v", err)
	}
	if want := merchant + "|Asia/Tokyo|America/Argentina/Buenos_Aires|op"; len(timezones.set) != 1 || timezones.set[0] != want {
		t.Errorf("set = %v, want [%s]", timezones.set, want)
	}
}

// Un choque con otro lote OPEN se propaga sin envolverlo como validación: el HTTP responde 409.
func TestSetMerchantTimezone_OpenBatchConflict(t *testing.T) {
	h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})
	h.SetMerchantTimezones(&fakeTimezones{setErr: repository.ErrOpenBatchConflict})

	err := h.SetMerchantTimezone(context.Background(), port.SetMerchantTimezoneCommand{
		MerchantID: domain.NewMerchantID().String(), Timezone: "Asia/Tokyo", OperatorID: "op",
	})
	if !errors.Is(err, repository.ErrOpenBatchConflict) {
		t.Fatalf("error = %v, want ErrOpenBatchConflict", err)
	}
}

func TestRegisterApproval_UsesMerchantTimezone(t *testing.T) {
	// 01:30 UTC del 6: en Tokio ya es el 6, en Buenos Aires todavía el 5.
	authorizedAt := "2026-11-06T01:30:00Z"
	cases := []struct {
		name     string
		timezone string
		want     string
	}{
		{"zona propia", "Asia/Tokyo", "2026-11-06"},
		{"zona general", "", "2026-11-05"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			merchant := domain.NewMerchantID()
			terminal := domain.NewTerminalID()
			repo := &fakeBatchRepo{findOrCreateResult: newOpenBatch(t, terminal, merchant)}
			pool := newMockPool(t)
			expectClaimed(pool, 1)

			loc, err := time.LoadLocation("America/Argentina/Buenos_Aires")
			if err != nil {
				t.Fatalf("LoadLocation() error = %v", err)
			}
			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
			h.SetBusinessLocation(loc)
			timezones := &fakeTimezones{byMerchant: map[string]string{merchant.String(): tc.timezone}}
			h.SetMerchantTimezones(timezones)

			cmd := validApprovalCmd()
			cmd.TerminalID, cmd.MerchantID, cmd.AuthorizedAt = terminal.String(), merchant.String(), authorizedAt
			if err := h.RegisterApproval(context.Background(), cmd); err != nil {
				t.Fatalf("RegisterApproval() error = %v", err)
			}
			// La zona se lee con el lock del comercio, dentro de la transacción de la venta.
			if len(timezones.locked) != 1 || timezones.locked[0] != merchant.String() {
				t.Errorf("locked = %v, want [%s]", timezones.locked, merchant)
			}
			if got := repo.findOrCreateDate.Format("2006-01-02"); got != tc.want {
				t.Errorf("batch date = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRegisterApproval_TimezoneLookupErrorIsRetried(t *testing.T) {
	h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	h.SetMerchantTimezones(&fakeTimezones{err: errors.New("db down")})

	err := h.RegisterApproval(context.Background(), validApprovalCmd())
	var ve *pkgerrors.ValidationError
	if err == nil || errors.As(err, &ve) {
		t.Fatalf("error = %v, want a transient error", err)
	}
}

func TestSetMerchantTimezone(t *testing.T) {
	merchant := domain.NewMerchantID().String()
	cases := []struct {
		name    string
		cmd     port.SetMerchantTimezoneCommand
		setErr  error
		wantSet bool
		wantErr bool
	}{
		{"válida", port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Asia/Tokyo", OperatorID: "op"}, nil, true, false},
		{"comercio inválido", port.SetMerchantTimezoneCommand{MerchantID: "x", Timezone: "Asia/Tokyo", OperatorID: "op"}, nil, false, true},
		{"sin operador", port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Asia/Tokyo"}, nil, false, true},
		{"zona inexistente", port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Mars/Base", OperatorID: "op"}, nil, false, true},
		{"zona vacía", port.SetMerchantTimezoneCommand{MerchantID: merchant, OperatorID: "op"}, nil, false, true},
		{"zona del servidor", port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Local", OperatorID: "op"}, nil, false, true},
		{"Postgres no la conoce", port.SetMerchantTimezoneCommand{MerchantID: merchant, Timezone: "Asia/Tokyo", OperatorID: "op"}, repository.ErrUnknownTimezone, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			timezones := &fakeTimezones{setErr: tc.setErr}
			h := command.NewAdminHandler(&fakeBatchRepo{}, &fakeProcessor{})
			h.SetMerchantTimezones(timezones)

			err := h.SetMerchantTimezone(context.Background(), tc.cmd)
			if tc.wantErr {
				wantValidation(t, err)
			} else if err != nil {
				t.Fatalf("error = %v", err)
			}
			if (len(timezones.set) == 1) != tc.wantSet {
				t.Errorf("set = %v, want called: %v", timezones.set, tc.wantSet)
			}
		})
	}
}

// ─── Coherencia de batch_date ─────────────────────────────────────────────────

func TestProcessBatchClose_BatchDateCoherence(t *testing.T) {
	requested := time.Date(2026, 11, 6, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		date    string
		wantErr bool
	}{
		{"2026-11-06", false},
		{"2026-11-07", true},
		{"2026-10-30", false}, // Justo 7 días
		{"2026-10-29", true},
		{"", false},
		{"no-es-fecha", false}, // Usa el día de negocio, como antes
	}
	for _, tc := range cases {
		t.Run(tc.date, func(t *testing.T) {
			repo := &fakeBatchRepo{}
			h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})
			h.SetMaxCloseAge(7)

			cmd := closeCmd(domain.NewTerminalID(), 0, 0)
			cmd.BatchDate, cmd.RequestedAt = tc.date, requested.Format(time.RFC3339)
			err := h.ProcessBatchClose(context.Background(), cmd)
			if tc.wantErr {
				wantValidation(t, err)
				if !repo.findOpenDate.IsZero() {
					t.Error("it should not look for a batch")
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

// El límite se mide en el día de negocio del comercio: el 6 en Tokio es todavía el 5 en UTC.
func TestProcessBatchClose_BatchDateUsesMerchantTimezone(t *testing.T) {
	merchant := domain.NewMerchantID()
	repo := &fakeBatchRepo{}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, &fakeProcessor{})
	h.SetMerchantTimezones(&fakeTimezones{byMerchant: map[string]string{merchant.String(): "Asia/Tokyo"}})

	cmd := closeCmd(domain.NewTerminalID(), 0, 0)
	cmd.MerchantID, cmd.BatchDate = merchant.String(), "2026-11-06"
	cmd.RequestedAt = time.Date(2026, 11, 5, 20, 0, 0, 0, time.UTC).Format(time.RFC3339)
	if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
		t.Fatalf("error = %v, want the date accepted", err)
	}
	if got := repo.findOpenDate.Format("2006-01-02"); got != "2026-11-06" {
		t.Errorf("close date = %s", got)
	}
}

// ─── Días anteriores: solo ayer, o un lote reabierto ─────────────────────────

type fakeReopens struct {
	reopened bool
	err      error
	asked    []string
}

func (f *fakeReopens) WasReopened(_ context.Context, batchID string) (bool, error) {
	f.asked = append(f.asked, batchID)
	return f.reopened, f.err
}

func TestProcessBatchClose_PastDaysNeedAReopen(t *testing.T) {
	requested := time.Date(2026, 11, 9, 15, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		date      string
		reopened  bool
		wantClose bool
		wantAsked bool
	}{
		{"today", "2026-11-09", false, true, false},
		{"yesterday, closed after midnight", "2026-11-08", false, true, false},
		{"three days ago, not reopened", "2026-11-06", false, false, true},
		{"three days ago, reopened by an operator", "2026-11-06", true, true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch := closableFoundBatch(t)
			repo := &fakeBatchRepo{findOpenResult: batch}
			pool := newMockPool(t)
			if tc.wantClose {
				expectClaimedWithEvent(pool)
			}
			reopens := &fakeReopens{reopened: tc.reopened}
			h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
			h.SetMaxCloseAge(7)
			h.SetBatchReopens(reopens)

			cmd := closeCmd(batch.TerminalID(), 2, 3000)
			cmd.BatchDate, cmd.RequestedAt = tc.date, requested.Format(time.RFC3339)
			err := h.ProcessBatchClose(context.Background(), cmd)

			if (len(reopens.asked) > 0) != tc.wantAsked {
				t.Errorf("asked about a reopen = %v, want %v", reopens.asked, tc.wantAsked)
			}
			if !tc.wantClose {
				wantValidation(t, err)
				if !strings.Contains(err.Error(), "was not reopened") {
					t.Errorf("error = %v, want it to explain the batch was not reopened", err)
				}
				if batch.State() != valueobject.BatchStateOpen || repo.saveCallCount != 0 {
					t.Errorf("state = %s, saves = %d, want OPEN and nothing saved", batch.State(), repo.saveCallCount)
				}
				return
			}
			if err != nil {
				t.Fatalf("ProcessBatchClose() error = %v", err)
			}
			if batch.State() == valueobject.BatchStateOpen {
				t.Error("batch still OPEN, want it closed")
			}
		})
	}
}

// Un error al consultar la auditoría es transitorio: el cierre se reintenta.
func TestProcessBatchClose_ReopenCheckErrorIsTransient(t *testing.T) {
	batch := closableFoundBatch(t)
	h := newHandler(newMockPool(t), &fakeBatchRepo{findOpenResult: batch}, &fakePublisher{}, &fakeProcessor{})
	h.SetBatchReopens(&fakeReopens{err: errors.New("connection reset")})

	cmd := closeCmd(batch.TerminalID(), 2, 3000)
	cmd.BatchDate, cmd.RequestedAt = "2026-11-06", time.Date(2026, 11, 9, 15, 0, 0, 0, time.UTC).Format(time.RFC3339)
	err := h.ProcessBatchClose(context.Background(), cmd)
	var ve *pkgerrors.ValidationError
	if err == nil || errors.As(err, &ve) {
		t.Fatalf("error = %v, want a transient error", err)
	}
}
