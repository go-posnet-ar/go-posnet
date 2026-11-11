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
	// sent son los totales del primer intento. Un intento con otros totales devuelve
	// SubmittedTotalsChangedError, como la base.
	sent   *repository.SubmissionTotals
	totals []repository.SubmissionTotals
}

func (f *fakeSubmissions) StartSubmission(_ context.Context, _ string, totals repository.SubmissionTotals) (string, error) {
	f.totals = append(f.totals, totals)
	if f.startErr != nil {
		return "", f.startErr
	}
	if f.sent != nil && *f.sent != totals {
		return "", &repository.SubmittedTotalsChangedError{Sent: *f.sent, Current: totals, ConfirmationID: f.previous}
	}
	f.starts++
	if f.sent == nil {
		f.sent = &totals
	}
	return f.previous, nil
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

// El caso de la brecha: el procesador aceptó el lote con unos totales, una revisión lo retuvo y los
// totales cambiaron. No se reenvía ni se marca SUBMITTED con la confirmación anterior.
func TestResubmit_TotalsChangedSinceSentIsRefused(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	processor := &fakeProcessor{confirmationID: "conf-2"}
	subs := &fakeSubmissions{previous: "conf-1", sent: &repository.SubmissionTotals{TotalCount: 99, TotalAmount: 1}}

	err := resubmit(t, repo, processor, subs)
	if !errors.Is(err, repository.ErrSubmittedTotalsChanged) {
		t.Fatalf("ResubmitBatch() error = %v, want ErrSubmittedTotalsChanged", err)
	}
	var detail *repository.SubmittedTotalsChangedError
	if !errors.As(err, &detail) || detail.ConfirmationID != "conf-1" || detail.Sent.TotalCount != 99 {
		t.Errorf("detail = %+v, want the previous submission", detail)
	}
	if processor.calls != 0 || repo.saveCallCount != 0 {
		t.Errorf("calls = %d, saves = %d, want 0 and 0", processor.calls, repo.saveCallCount)
	}
}

// Los totales que se registran son los del lote: los que recibe el procesador.
func TestResubmit_RecordsTheBatchTotals(t *testing.T) {
	batch := newClosedBatch(t)
	repo := &fakeBatchRepo{findByIDResult: batch}
	subs := &fakeSubmissions{}

	if err := resubmit(t, repo, &fakeProcessor{confirmationID: "conf-1"}, subs); err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	want := repository.SubmissionTotals{
		TotalCount:       batch.Summary().TotalCount(),
		TotalAmount:      batch.Summary().TotalAmount().Cents(),
		AdjustmentCount:  batch.Summary().AdjustmentCount(),
		AdjustmentAmount: batch.Summary().AdjustmentAmount(),
	}
	if len(subs.totals) != 1 || subs.totals[0] != want {
		t.Errorf("totals = %+v, want [%+v]", subs.totals, want)
	}
}
