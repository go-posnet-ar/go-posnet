package command_test

import (
	"context"
	"errors"
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
)

// ─── #4: revisiones de settlement.completed ──────────────────────────────────

// fakeCompletions es settlement_completions en memoria.
type fakeCompletions struct {
	last     *repository.DailyCompletion
	lastErr  error
	inserted []repository.DailyCompletion
}

func (f *fakeCompletions) LastCompletionTx(_ context.Context, _ pgx.Tx, _ domain.MerchantID, _ time.Time, currency string) (*repository.DailyCompletion, error) {
	if f.last != nil && f.last.Currency != currency {
		return nil, f.lastErr
	}
	return f.last, f.lastErr
}

func (f *fakeCompletions) InsertCompletionTx(_ context.Context, _ pgx.Tx, c repository.DailyCompletion) error {
	f.inserted = append(f.inserted, c)
	return nil
}

func settleWith(t *testing.T, day []repository.MerchantDayBatch, completions *fakeCompletions, events int) *fakePublisher {
	t.Helper()
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 10000)
	repo := &fakeBatchRepo{findByIDResult: b, dayResult: day}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	h.SetSettlementCompletions(completions)
	expectEventTx(pool, events)
	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("SettleBatch() error = %v", err)
	}
	return publisher
}

// El primer resumen del día es la revisión 1, con las diferencias iguales al acumulado, y queda
// guardado para numerar el siguiente.
func TestSettlementCompleted_FirstRevision(t *testing.T) {
	completions := &fakeCompletions{}
	publisher := settleWith(t, []repository.MerchantDayBatch{{Currency: "ARS", Settled: true, TotalAmount: 10000}}, completions, 1)

	c := publisher.completions[0]
	if c.Revision != 1 || c.DeltaBatches != 1 || c.DeltaAmount != 10000 || c.DeltaNetAmount != c.NetAmount || c.NetAmount != 9750 {
		t.Errorf("completion = %+v, want revision 1 with deltas equal to the totals", c)
	}
	if len(completions.inserted) != 1 || completions.inserted[0].Revision != 1 || *completions.inserted[0].NetAmount != 9750 ||
		completions.inserted[0].EventID != "event-completed" {
		t.Errorf("inserted = %+v, want revision 1 with its net and event", completions.inserted)
	}
}

// Un lote liquidado después del resumen da la revisión siguiente, con el acumulado y lo nuevo.
func TestSettlementCompleted_LateBatchIsNextRevision(t *testing.T) {
	net := int64(9750)
	completions := &fakeCompletions{last: &repository.DailyCompletion{Currency: "ARS", Revision: 1, TotalBatches: 1, TotalAmount: 10000, NetAmount: &net}}
	publisher := settleWith(t, []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 10000},
		{Currency: "ARS", Settled: true, TotalAmount: 4000},
	}, completions, 1)

	c := publisher.completions[0]
	if c.Revision != 2 || c.TotalBatches != 2 || c.TotalAmount != 14000 || c.DeltaBatches != 1 || c.DeltaAmount != 4000 ||
		c.NetAmount != 13650 || c.DeltaNetAmount != 13650-9750 {
		t.Errorf("completion = %+v, want revision 2 with the late batch as delta", c)
	}
}

// Una fila de 000016 no tiene el neto: la diferencia se calcula con el MDR actual.
func TestSettlementCompleted_BackfilledPreviousWithoutNet(t *testing.T) {
	completions := &fakeCompletions{last: &repository.DailyCompletion{Currency: "ARS", Revision: 1, TotalBatches: 1, TotalAmount: 10000}}
	publisher := settleWith(t, []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 10000},
		{Currency: "ARS", Settled: true, TotalAmount: 4000},
	}, completions, 1)

	if c := publisher.completions[0]; c.DeltaNetAmount != 13650-9750 {
		t.Errorf("delta net = %d, want %d", c.DeltaNetAmount, 13650-9750)
	}
}

// La liquidación tardía de una moneda no repite el resumen de otra que no cambió.
func TestSettlementCompleted_UnchangedCurrencyIsNotRepeated(t *testing.T) {
	completions := &fakeCompletions{last: &repository.DailyCompletion{Currency: "ARS", Revision: 1, TotalBatches: 1, TotalAmount: 10000}}
	publisher := settleWith(t, []repository.MerchantDayBatch{
		{Currency: "ARS", Settled: true, TotalAmount: 10000},
		{Currency: "USD", Settled: true, TotalAmount: 300},
	}, completions, 1)

	if len(publisher.completions) != 1 || publisher.completions[0].Currency != "USD" || publisher.completions[0].Revision != 1 {
		t.Errorf("completions = %+v, want only USD, revision 1", publisher.completions)
	}
}

func TestSettlementCompleted_PreviousLookupErrorRollsBack(t *testing.T) {
	b := newSubmittedBatch(t, domain.NewMerchantID(), "ARS", 10000)
	repo := &fakeBatchRepo{findByIDResult: b, dayResult: []repository.MerchantDayBatch{{Currency: "ARS", Settled: true, TotalAmount: 10000}}}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	h.SetSettlementCompletions(&fakeCompletions{lastErr: errors.New("connection reset")})
	expectRolledBackTx(pool)

	if err := h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: b.ID(), OperatorID: "op-1"}); err == nil {
		t.Fatal("SettleBatch() error = nil, want the lookup error")
	}
}

// ─── #5: política del cierre automático ──────────────────────────────────────

func staleBatchWithSale(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	if err := b.AddTransaction(domain.NewTransactionID(), 1000, valueobject.BatchTxPurchase); err != nil {
		t.Fatalf("AddTransaction() error = %v", err)
	}
	return b
}

func TestCloseStaleBatches_Policy(t *testing.T) {
	tests := []struct {
		name       string
		hold       bool
		wantState  valueobject.BatchState
		wantRef    string
		wantSubmit int
	}{
		{"submit: sent with backend totals", false, valueobject.BatchStateSubmitted, aggregate.ReconciledWithBackend, 1},
		{"hold: disputed, not sent", true, valueobject.BatchStateDisputed, aggregate.ReconciledWithNone, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := staleBatchWithSale(t)
			repo := &fakeBatchRepo{staleIDs: []string{b.ID()}, findByIDResult: b}
			pool := newMockPool(t)
			expectClaimedWithEvent(pool)
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := newHandler(pool, repo, &fakePublisher{}, processor)
			h.HoldUnreconciledAutoCloses(tc.hold)

			if closed, _, err := h.CloseStaleBatches(context.Background(), time.Now()); err != nil || closed != 1 {
				t.Fatalf("CloseStaleBatches() = %d, %v", closed, err)
			}
			if b.State() != tc.wantState || b.ReconciledWith() != tc.wantRef || processor.calls != tc.wantSubmit {
				t.Errorf("state = %s, reconciled with %q, submits = %d, want %s, %q, %d",
					b.State(), b.ReconciledWith(), processor.calls, tc.wantState, tc.wantRef, tc.wantSubmit)
			}
		})
	}
}

// Un lote vacío queda VOID aunque la política retenga: no hay nada que revisar.
func TestCloseStaleBatches_HoldDoesNotRetainEmptyBatches(t *testing.T) {
	b := newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())
	repo := &fakeBatchRepo{staleIDs: []string{b.ID()}, findByIDResult: b}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{})
	h.HoldUnreconciledAutoCloses(true)

	if _, _, err := h.CloseStaleBatches(context.Background(), time.Now()); err != nil {
		t.Fatalf("CloseStaleBatches() error = %v", err)
	}
	if b.State() != valueobject.BatchStateVoid {
		t.Errorf("state = %s, want VOID", b.State())
	}
}

// Un lote retenido por la política no se libera solo al cerrarse sus revisiones.
func TestReleaseHeldBatch_HeldUnreconciledStaysDisputed(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithNone, 0, 0)
	repo := &fakeBatchRepo{findByIDResult: held}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	h := newHandler(newMockPool(t), repo, &fakePublisher{}, processor)
	h.SetReversalReviews(&fakeReviews{})

	if err := h.ReleaseHeldBatch(context.Background(), "b-1"); err != nil {
		t.Fatalf("ReleaseHeldBatch() error = %v", err)
	}
	if held.State() != valueobject.BatchStateDisputed || processor.calls != 0 || repo.saveCallCount != 0 {
		t.Errorf("state = %s, submits = %d, saves = %d, want DISPUTED, 0, 0", held.State(), processor.calls, repo.saveCallCount)
	}
}

// resolve-dispute saca un lote retenido: el operador acepta los totales del backend.
func TestResolveDispute_ReleasesAHeldUnreconciledBatch(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithNone, 0, 0)
	repo := &fakeBatchRepo{findByIDResult: held}
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, repo, publisher, 2.5)
	expectEventTx(pool, 1)

	err := h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: "b-1", OperatorID: "op-1", Reason: "revisado con el comercio"})
	if err != nil {
		t.Fatalf("ResolveDispute() error = %v", err)
	}
	if held.State() != valueobject.BatchStateClosed || held.ReconciledWith() != aggregate.ReconciledWithBackend {
		t.Errorf("state = %s, reconciled with %q, want CLOSED and BACKEND", held.State(), held.ReconciledWith())
	}
	// Los totales se vuelven a informar, como una revisión del cierre.
	if len(publisher.triggers) != 1 || publisher.triggers[0] != "resolve_dispute" {
		t.Errorf("triggers = %v, want [resolve_dispute]", publisher.triggers)
	}
}

// ─── #6: force-close con los totales del terminal ────────────────────────────

func TestForceClose_WithTerminalTotals(t *testing.T) {
	count, matching, wrong := 1, int64(1000), int64(900)
	tests := []struct {
		name      string
		amount    *int64
		wantState valueobject.BatchState
		wantRef   string
		wantDisc  int
	}{
		{"match: closed against the terminal", &matching, valueobject.BatchStateClosed, aggregate.ReconciledWithTerminal, 0},
		{"mismatch: disputed", &wrong, valueobject.BatchStateDisputed, aggregate.ReconciledWithTerminal, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := staleBatchWithSale(t)
			repo := &fakeBatchRepo{findByIDResult: b}
			h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
			expectEventTx(pool, 1)

			cmd := port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1", TerminalCount: &count, TerminalAmount: tc.amount}
			if err := h.ForceClose(context.Background(), cmd); err != nil {
				t.Fatalf("ForceClose() error = %v", err)
			}
			if b.State() != tc.wantState || b.ReconciledWith() != tc.wantRef || b.Discrepancies() != tc.wantDisc {
				t.Errorf("state = %s, reconciled with %q, discrepancies = %d, want %s, %q, %d",
					b.State(), b.ReconciledWith(), b.Discrepancies(), tc.wantState, tc.wantRef, tc.wantDisc)
			}
			if len(repo.auditEntries) != 1 || repo.auditEntries[0].ToState != tc.wantState.String() {
				t.Errorf("audit = %+v, want to_state %s", repo.auditEntries, tc.wantState)
			}
		})
	}
}

func TestForceClose_TerminalTotalsValidation(t *testing.T) {
	count, negative, amount := 1, -1, int64(1000)
	for name, cmd := range map[string]port.ForceCloseCommand{
		"only count":     {BatchID: "b", OperatorID: "op-1", TerminalCount: &count},
		"only amount":    {BatchID: "b", OperatorID: "op-1", TerminalAmount: &amount},
		"negative count": {BatchID: "b", OperatorID: "op-1", TerminalCount: &negative, TerminalAmount: &amount},
	} {
		t.Run(name, func(t *testing.T) {
			h := command.NewAdminHandler(&fakeBatchRepo{findByIDErr: errors.New("must not load the batch")}, nil)
			var ve *pkgerrors.ValidationError
			if err := h.ForceClose(context.Background(), cmd); !errors.As(err, &ve) {
				t.Errorf("ForceClose() error = %v, want ValidationError", err)
			}
		})
	}
}

// Sin totales, cierra como antes, con los del backend.
func TestForceClose_WithoutTerminalTotalsUsesBackend(t *testing.T) {
	b := staleBatchWithSale(t)
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: b}, &fakePublisher{}, 2.5)
	expectEventTx(pool, 1)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: b.ID(), OperatorID: "op-1"}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if b.State() != valueobject.BatchStateClosed || b.ReconciledWith() != aggregate.ReconciledWithBackend {
		t.Errorf("state = %s, reconciled with %q, want CLOSED and BACKEND", b.State(), b.ReconciledWith())
	}
}

// ─── #9: los lotes que salen de DISPUTED se vuelven a informar ───────────────

func TestReleaseHeldBatch_ReportsTheRevisedTotals(t *testing.T) {
	held := heldBatch(t, valueobject.BatchStateDisputed, aggregate.ReconciledWithBackend, 0, 0)
	pool := newMockPool(t)
	expectReleasedTx(pool)
	publisher := &fakePublisher{}
	h := newHandler(pool, &fakeBatchRepo{findByIDResult: held}, publisher, &fakeProcessor{confirmationID: "conf-1"})
	h.SetReversalReviews(&fakeReviews{})

	if err := h.ReleaseHeldBatch(context.Background(), "b-1"); err != nil {
		t.Fatalf("ReleaseHeldBatch() error = %v", err)
	}
	if len(publisher.triggers) != 1 || publisher.triggers[0] != "release" {
		t.Errorf("triggers = %v, want [release]", publisher.triggers)
	}
}

// Un lote en disputa sin transacciones queda VOID al resolverlo, sin evento: no hay nada que informar.
func TestResolveDispute_EmptyBatchIsVoidedSilently(t *testing.T) {
	empty := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID: "b-1", TerminalID: domain.NewTerminalID(), MerchantID: domain.NewMerchantID(),
		BatchDate: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC), State: valueobject.BatchStateDisputed, Currency: "ARS",
		CreatedAt: time.Now(), ReconciledWith: aggregate.ReconciledWithTerminal,
	})
	publisher := &fakePublisher{}
	h, pool := newEventAdmin(t, &fakeBatchRepo{findByIDResult: empty}, publisher, 2.5)
	expectEventTx(pool, 0)

	err := h.ResolveDispute(context.Background(), port.ResolveDisputeCommand{BatchID: "b-1", OperatorID: "op-1", Reason: "vacío"})
	if err != nil {
		t.Fatalf("ResolveDispute() error = %v", err)
	}
	if empty.State() != valueobject.BatchStateVoid || len(publisher.triggers) != 0 {
		t.Errorf("state = %s, triggers = %v, want VOID and none", empty.State(), publisher.triggers)
	}
}
