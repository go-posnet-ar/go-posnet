package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
)

// fakeSubmissions es batch_submissions en memoria.
type fakeSubmissions struct {
	previous   string // Confirmación de un envío anterior
	startErr   error
	confirmErr error
	starts     int
	confirmed  []string
}

func (f *fakeSubmissions) StartSubmission(context.Context, string) (string, error) {
	f.starts++
	return f.previous, f.startErr
}

func (f *fakeSubmissions) ConfirmSubmission(_ context.Context, _ string, confirmationID string) error {
	if f.confirmErr != nil {
		return f.confirmErr
	}
	f.confirmed = append(f.confirmed, confirmationID)
	return nil
}

func resubmit(t *testing.T, repo repository.SettlementBatchRepository, processor *fakeProcessor, subs *fakeSubmissions) error {
	t.Helper()
	h := command.NewAdminHandler(repo, processor)
	h.SetBatchSubmissions(subs)
	return h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-1", OperatorID: "op-1"})
}

// El caso de la brecha: el procesador aceptó, falló guardar SUBMITTED, y el operador hace resubmit.
// La confirmación quedó registrada: no se envía otra vez.
func TestResubmit_AlreadyAcceptedIsNotSentAgain(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	processor := &fakeProcessor{confirmationID: "conf-2"}
	subs := &fakeSubmissions{previous: "conf-1"}

	if err := resubmit(t, repo, processor, subs); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if processor.calls != 0 {
		t.Errorf("processor calls = %d, want 0: it was already accepted", processor.calls)
	}
	if saved := repo.lastSaved(); saved == nil || saved.State() != valueobject.BatchStateSubmitted {
		t.Fatalf("saved = %v, want the batch SUBMITTED", saved)
	}
}

func TestResubmit_RecordsAttemptAndConfirmation(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	processor := &fakeProcessor{confirmationID: "conf-1"}
	subs := &fakeSubmissions{}

	if err := resubmit(t, repo, processor, subs); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if subs.starts != 1 || processor.calls != 1 || len(subs.confirmed) != 1 || subs.confirmed[0] != "conf-1" {
		t.Errorf("starts = %d, calls = %d, confirmed = %v, want 1, 1, [conf-1]", subs.starts, processor.calls, subs.confirmed)
	}
}

// Sin registrar el intento no envía: no habría forma de saber después que se envió.
func TestResubmit_AttemptNotRecordedIsNotSent(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	processor := &fakeProcessor{confirmationID: "conf-1"}

	err := resubmit(t, repo, processor, &fakeSubmissions{startErr: errors.New("db down")})
	if err == nil || !strings.Contains(err.Error(), "record submission attempt") {
		t.Fatalf("ResubmitBatch() error = %v, want the attempt error", err)
	}
	if processor.calls != 0 || repo.saveCallCount != 0 {
		t.Errorf("calls = %d, saves = %d, want 0 and 0", processor.calls, repo.saveCallCount)
	}
}

// Si no se puede guardar la confirmación, el lote se marca SUBMITTED igual: el envío ya se hizo.
func TestResubmit_ConfirmationNotRecordedStillSubmits(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	processor := &fakeProcessor{confirmationID: "conf-1"}

	if err := resubmit(t, repo, processor, &fakeSubmissions{confirmErr: errors.New("db down")}); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if saved := repo.lastSaved(); saved == nil || saved.State() != valueobject.BatchStateSubmitted {
		t.Fatalf("saved = %v, want the batch SUBMITTED", saved)
	}
}

// staleOnceRepo rechaza el primer guardado por versión vieja y recarga un lote nuevo, como la base.
type staleOnceRepo struct {
	*fakeBatchRepo
	fresh  func() *aggregate.SettlementBatch
	saves  int
	reload int
}

func (r *staleOnceRepo) Save(ctx context.Context, b *aggregate.SettlementBatch) error {
	r.saves++
	if r.saves == 1 {
		return repository.ErrStaleBatch
	}
	return r.fakeBatchRepo.Save(ctx, b)
}

func (r *staleOnceRepo) FindByID(context.Context, string) (*aggregate.SettlementBatch, error) {
	r.reload++
	return r.fresh(), nil
}

// Un conflicto de versión después de enviarlo no pierde el envío: recarga el lote y lo marca
// SUBMITTED, sin volver a enviarlo.
func TestResubmit_StaleSaveAfterSubmitReloads(t *testing.T) {
	tests := []struct {
		name      string
		fresh     func() *aggregate.SettlementBatch
		wantErr   bool
		wantSaves int
	}{
		{"still CLOSED: saved", func() *aggregate.SettlementBatch { return newClosedBatch(t) }, false, 2},
		{"another process marked it SUBMITTED", func() *aggregate.SettlementBatch {
			b := newClosedBatch(t)
			_ = b.Submit()
			return b
		}, false, 1},
		{"no longer CLOSED: error", func() *aggregate.SettlementBatch { return newDisputedBatch(t) }, true, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			first := newClosedBatch(t)
			repo := &staleOnceRepo{fakeBatchRepo: &fakeBatchRepo{}, fresh: tc.fresh}
			processor := &fakeProcessor{confirmationID: "conf-1"}
			h := command.NewAdminHandler(repo, processor)
			h.SetBatchSubmissions(&fakeSubmissions{})
			// La primera carga es la de ResubmitBatch; las siguientes, las recargas tras el conflicto.
			loads := 0
			repo.fresh = func() *aggregate.SettlementBatch {
				loads++
				if loads == 1 {
					return first
				}
				return tc.fresh()
			}

			err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-1", OperatorID: "op-1"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("ResubmitBatch() error = %v, wantErr %v", err, tc.wantErr)
			}
			if processor.calls != 1 {
				t.Errorf("processor calls = %d, want 1", processor.calls)
			}
			if repo.saves != tc.wantSaves {
				t.Errorf("saves = %d, want %d", repo.saves, tc.wantSaves)
			}
		})
	}
}
