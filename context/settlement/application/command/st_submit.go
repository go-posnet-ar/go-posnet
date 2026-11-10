package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// confirmAttempts y confirmWait acotan los reintentos de guardar la confirmación del procesador:
// sin ella, un resubmit enviaría otra vez un lote ya aceptado.
const (
	confirmAttempts = 3
	confirmWait     = 200 * time.Millisecond
)

// submission es el resultado de sendToProcessor.
type submission struct {
	confirmationID string
	// recovered indica que el procesador ya había aceptado el lote en un envío anterior: no se
	// envió otra vez, y solo falta marcarlo SUBMITTED.
	recovered bool
}

// sendToProcessor envía el lote, salvo que un envío anterior ya tenga confirmación. Registra el
// intento antes de enviarlo y la confirmación apenas la recibe, aparte del lote: si después falla
// guardar el lote en SUBMITTED, la confirmación queda y un resubmit no lo envía otra vez.
//
// Sin registro de envíos (submissions nil) envía siempre, como antes. Si no puede registrar el
// intento, no envía: sin el registro, no habría forma de saber que ya se envió.
func sendToProcessor(ctx context.Context, p service.SettlementProcessor, submissions repository.BatchSubmissions, batch *aggregate.SettlementBatch) (submission, error) {
	if p == nil {
		return submission{}, service.ErrProcessorUnavailable
	}
	if submissions != nil {
		previous, err := submissions.StartSubmission(ctx, batch.ID())
		if err != nil {
			return submission{}, fmt.Errorf("record submission attempt: %w", err)
		}
		if previous != "" {
			return submission{confirmationID: previous, recovered: true}, nil
		}
	}
	confirmationID, err := p.Submit(ctx, batch)
	if err != nil {
		return submission{}, err
	}
	if submissions != nil {
		recordConfirmation(ctx, submissions, batch.ID(), confirmationID)
	}
	return submission{confirmationID: confirmationID}, nil
}

// recordConfirmation guarda la confirmación, con unos pocos reintentos. Si no puede, el lote se
// marca SUBMITTED igual: la confirmación queda en el log, y un resubmit repetiría el envío con el
// mismo batch_id, que el procesador usa como clave de idempotencia.
func recordConfirmation(ctx context.Context, submissions repository.BatchSubmissions, batchID, confirmationID string) {
	var err error
	for attempt := 1; attempt <= confirmAttempts; attempt++ {
		if err = submissions.ConfirmSubmission(ctx, batchID, confirmationID); err == nil {
			return
		}
		if attempt < confirmAttempts {
			select {
			case <-ctx.Done():
				attempt = confirmAttempts
			case <-time.After(time.Duration(attempt) * confirmWait):
			}
		}
	}
	observability.FromContext(ctx).Error("batch accepted by processor but the confirmation was not recorded",
		slog.String("batch_id", batchID),
		slog.String("confirmation_id", confirmationID),
		slog.String("error", err.Error()),
	)
}

// errNotSubmittable es un lote que, al recargarlo, ya no está CLOSED ni SUBMITTED.
var errNotSubmittable = errors.New("batch is no longer CLOSED")

// markSubmitted pasa el lote a SUBMITTED y lo guarda. Si otro proceso lo cambió mientras se
// enviaba, lo recarga y lo vuelve a intentar: el envío ya se hizo, y perderlo dejaría el lote
// CLOSED para un resubmit. Devuelve el lote guardado.
func markSubmitted(ctx context.Context, repo repository.SettlementBatchRepository, batch *aggregate.SettlementBatch, save func(*aggregate.SettlementBatch) error) (*aggregate.SettlementBatch, error) {
	current := batch
	err := retryOnStale(ctx, func() error {
		if current.State() == valueobject.BatchStateSubmitted {
			return nil
		}
		// Submit admite DISPUTED → SUBMITTED: un lote que otro proceso retuvo mientras se enviaba
		// no se marca enviado. Queda retenido, con la confirmación registrada.
		if current.State() != valueobject.BatchStateClosed {
			return fmt.Errorf("%w: state is %s", errNotSubmittable, current.State())
		}
		if err := current.Submit(); err != nil {
			return fmt.Errorf("%w: %w", errNotSubmittable, err)
		}
		err := save(current)
		if !errors.Is(err, repository.ErrStaleBatch) {
			return err
		}
		fresh, ferr := repo.FindByID(ctx, batch.ID())
		if ferr != nil {
			return fmt.Errorf("reload after stale save: %w", ferr)
		}
		if fresh == nil {
			return fmt.Errorf("reload after stale save: batch %s not found", batch.ID())
		}
		current = fresh
		return err
	})
	return current, err
}
