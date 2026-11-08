package command

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// fraudTimeoutBatch limita cuántas transacciones rechaza cada corrida.
const fraudTimeoutBatch = 100

// ExpireFraudChecks rechaza las transacciones que esperan el score de Fraud Detection desde hace
// más de timeout, con FRAUD_TIMEOUT. No llegaron al adquirente, así que no hay nada que revertir,
// y el rechazo es reintentable.
//
// Varias instancias pueden tomar la misma transacción, y un score puede llegar a la vez:
// SaveIfState deja que solo uno la mueva. Un score que llega después se ignora, como cualquier
// otro que la transacción ya no espera.
func (h *AuthorizationHandler) ExpireFraudChecks(ctx context.Context, now time.Time, timeout time.Duration) (expired, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.ExpireFraudChecks")
	defer span.End()

	txs, err := h.repo.FindFraudCheckExpired(ctx, now.Add(-timeout), fraudTimeoutBatch)
	if err != nil {
		return 0, 0, fmt.Errorf("ExpireFraudChecks: %w", err)
	}
	for _, tx := range txs {
		log := observability.FromContext(ctx).With(
			slog.String("transaction_id", tx.ID().String()),
			slog.String("terminal_id", tx.TerminalID().String()),
		)
		if err := tx.ExpireFraudCheck(); err != nil {
			failed++
			log.Error("fraud check not expired", slog.String("error", err.Error()))
			continue
		}
		saved, err := h.saveAndEnqueue(ctx, tx, valueobject.StateFraudChecking, func(dbTx pgx.Tx) error {
			return h.publisher.PublishRejected(ctx, dbTx, tx)
		})
		if err != nil {
			failed++
			observability.RecordError(ctx, err)
			log.Error("fraud check not expired — retrying in the next run", slog.String("error", err.Error()))
			continue
		}
		if !saved {
			continue
		}
		expired++
		h.recordRejection(ctx, tx)
		log.Warn("no fraud score in time — transaction rejected", slog.Duration("timeout", timeout))
	}
	return expired, failed, nil
}
