package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// fakePending es pending_terminal_closes en memoria.
type fakePending struct {
	saved   []repository.PendingClose
	saveErr error
	found   *repository.PendingClose
	deleted int
}

func (f *fakePending) SavePendingClose(_ context.Context, p repository.PendingClose) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, p)
	return nil
}

func (f *fakePending) FindPendingClose(context.Context, domain.TerminalID, time.Time, string) (*repository.PendingClose, error) {
	return f.found, nil
}

func (f *fakePending) DeletePendingCloseTx(context.Context, pgx.Tx, domain.TerminalID, time.Time, string) error {
	f.deleted++
	return nil
}

// Un cierre sin ningún lote del día guarda los totales del terminal; uno sin ventas, no.
func TestProcessBatchClose_WithoutBatchSavesTheTotals(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int
		amount    int64
		wantSaved bool
	}{
		{"with sales", 2, 3000, true},
		{"without sales", 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
			h.SetTerminalDayBatches(&fakeTerminalDays{})
			pending := &fakePending{}
			h.SetPendingTerminalCloses(pending)

			cmd := validProcessCloseCmd()
			cmd.TerminalCount, cmd.TerminalAmount = tc.count, tc.amount
			if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
				t.Fatalf("ProcessBatchClose() error = %v", err)
			}
			if (len(pending.saved) == 1) != tc.wantSaved {
				t.Fatalf("saved = %+v, want saved: %v", pending.saved, tc.wantSaved)
			}
			if tc.wantSaved {
				p := pending.saved[0]
				if p.TerminalID.String() != cmd.TerminalID || p.BatchDate.Format("2006-01-02") != cmd.BatchDate ||
					p.TerminalCount != 2 || p.TerminalAmount != 3000 || p.EventID != cmd.EventID {
					t.Errorf("saved = %+v, want the close's totals", p)
				}
			}
		})
	}
}

// Si no se pueden guardar, el cierre se reintenta: los totales no se pierden.
func TestProcessBatchClose_WithoutBatchSaveErrorRetries(t *testing.T) {
	h := newHandler(newMockPool(t), &fakeBatchRepo{}, &fakePublisher{}, &fakeProcessor{})
	h.SetTerminalDayBatches(&fakeTerminalDays{})
	h.SetPendingTerminalCloses(&fakePending{saveErr: errors.New("db down")})

	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err == nil {
		t.Fatal("ProcessBatchClose() error = nil, want the save error so the event is retried")
	}
}

// Un cierre normal reemplaza los totales guardados del mismo día.
func TestProcessBatchClose_DeletesThePendingTotals(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{findOpenResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "conf-1"})
	pending := &fakePending{}
	h.SetPendingTerminalCloses(pending)

	cmd := validProcessCloseCmd()
	cmd.BatchDate = time.Now().UTC().Format("2006-01-02")
	if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}
	if pending.deleted != 1 {
		t.Errorf("deleted = %d, want 1", pending.deleted)
	}
}

// El cierre automático concilia contra los totales de un cierre anterior sin lote.
func TestCloseStaleBatches_ReconcilesAgainstAnEarlierCloseWithoutBatch(t *testing.T) {
	tests := []struct {
		name        string
		count       int
		amount      int64
		wantState   valueobject.BatchState
		wantDiscrep int
	}{
		{"matches: sent", 2, 3000, valueobject.BatchStateSubmitted, 0},
		{"a sale is missing: disputed", 3, 4000, valueobject.BatchStateDisputed, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batch := closableFoundBatch(t) // 2 compras, 3000
			repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
			pool := newMockPool(t)
			expectClaimedWithEvent(pool)
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := newHandler(pool, repo, &fakePublisher{}, processor)
			pending := &fakePending{found: &repository.PendingClose{TerminalCount: tc.count, TerminalAmount: tc.amount}}
			h.SetPendingTerminalCloses(pending)

			if closed, failed, err := h.CloseStaleBatches(context.Background(), time.Now()); err != nil || closed != 1 || failed != 0 {
				t.Fatalf("CloseStaleBatches() = %d, %d, %v, want 1, 0, nil", closed, failed, err)
			}
			if batch.State() != tc.wantState || batch.Discrepancies() != tc.wantDiscrep {
				t.Errorf("state = %s, discrepancies = %d, want %s, %d", batch.State(), batch.Discrepancies(), tc.wantState, tc.wantDiscrep)
			}
			if batch.ReconciledWith() != aggregate.ReconciledWithTerminal {
				t.Errorf("ReconciledWith() = %q, want TERMINAL", batch.ReconciledWith())
			}
			if pending.deleted != 1 {
				t.Errorf("deleted = %d, want 1: the totals are used", pending.deleted)
			}
			if (processor.calls == 1) != (tc.wantState == valueobject.BatchStateSubmitted) {
				t.Errorf("processor calls = %d", processor.calls)
			}
		})
	}
}

// Sin totales guardados, el cierre automático usa los del backend, como antes.
func TestCloseStaleBatches_WithoutPendingTotalsUsesTheBackend(t *testing.T) {
	batch := closableFoundBatch(t)
	repo := &fakeBatchRepo{staleIDs: []string{batch.ID()}, findByIDResult: batch}
	pool := newMockPool(t)
	expectClaimedWithEvent(pool)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "conf-1"})
	pending := &fakePending{}
	h.SetPendingTerminalCloses(pending)

	if _, _, err := h.CloseStaleBatches(context.Background(), time.Now()); err != nil {
		t.Fatalf("CloseStaleBatches() error = %v", err)
	}
	if batch.ReconciledWith() != aggregate.ReconciledWithBackend || pending.deleted != 0 {
		t.Errorf("ReconciledWith() = %q, deleted = %d, want BACKEND and 0", batch.ReconciledWith(), pending.deleted)
	}
}
