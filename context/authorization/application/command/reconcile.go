package command

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/observability"
)

const (
	// reconcileBatch limita cuántas transacciones resuelve cada corrida.
	reconcileBatch = 50
)

// ReconcileUnresolved resuelve las transacciones sin un resultado conocido: las que están en
// INDETERMINATE y las que siguen en PROCESSING desde hace más de stuckAfter, lo que indica una
// caída durante la llamada al adquirente o un fallo al guardar su respuesta.
//
// No se puede saber si el emisor las aprobó, así que se revierten en el adquirente, como
// indica ISO 8583 ante un timeout, y se rechazan. La reversa no toca a Settlement: la venta
// nunca se informó como aprobada. Una reversa que falla se reintenta en la corrida siguiente.
//
// Varias instancias pueden tomar la misma transacción: una reversa repetida no tiene efecto, y
// SaveIfState deja que solo una informe el resultado.
//
// También reintenta los pedidos de reversa de transacciones aprobadas. Estas sí se informan con
// reversal-completed, porque Settlement ya registró la venta. Toma las pedidas hace más que el
// timeout de una llamada: antes, la primera reversa todavía puede estar en curso.
func (h *AuthorizationHandler) ReconcileUnresolved(ctx context.Context, now time.Time, stuckAfter time.Duration) (resolved, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.ReconcileUnresolved")
	defer span.End()

	txs, err := h.repo.FindUnresolved(ctx, now.Add(-stuckAfter), reconcileBatch)
	if err != nil {
		return 0, 0, fmt.Errorf("ReconcileUnresolved: %w", err)
	}
	for _, tx := range txs {
		log := observability.FromContext(ctx).With(
			slog.String("transaction_id", tx.ID().String()),
			slog.String("terminal_id", tx.TerminalID().String()),
			slog.String("from_state", tx.State().String()),
		)
		done, err := h.reconcile(ctx, tx, log)
		if err != nil {
			failed++
			observability.RecordError(ctx, err)
			log.Error("transaction not reconciled — retrying in the next run", slog.String("error", err.Error()))
			continue
		}
		if done {
			resolved++
		}
	}

	reversals, err := h.repo.FindPendingReversals(ctx, now.Add(-h.reversalRetryAfter()), reconcileBatch)
	if err != nil {
		return resolved, failed, fmt.Errorf("ReconcileUnresolved: %w", err)
	}
	for _, tx := range reversals {
		log := observability.FromContext(ctx).With(
			slog.String("original_tx_id", tx.ID().String()),
			slog.String("terminal_id", tx.TerminalID().String()),
		)
		done, err := h.completeReversal(ctx, tx, log)
		if err != nil {
			failed++
			observability.RecordError(ctx, err)
			log.Error("reversal not completed — retrying in the next run", slog.String("error", err.Error()))
			continue
		}
		if done {
			resolved++
		}
	}
	return resolved, failed, nil
}

// reversalRetryAfter es cuánto debe tener un pedido de reversa para que el conciliador lo tome.
func (h *AuthorizationHandler) reversalRetryAfter() time.Duration {
	if h.acquirerTimeout > 0 {
		return h.acquirerTimeout
	}
	return defaultAcquirerTimeout
}

// reconcile revierte una transacción y la rechaza. Devuelve false si otro proceso la movió antes.
func (h *AuthorizationHandler) reconcile(ctx context.Context, tx *aggregate.Transaction, log *slog.Logger) (bool, error) {
	if tx.State() == valueobject.StateProcessing {
		if err := tx.MarkIndeterminate(); err != nil {
			return false, fmt.Errorf("mark indeterminate: %w", err)
		}
		// Desde acá, una respuesta tardía del adquirente ya no puede guardarse.
		saved, err := h.repo.SaveIfState(ctx, tx, valueobject.StateProcessing)
		if err != nil {
			return false, fmt.Errorf("save indeterminate: %w", err)
		}
		if !saved {
			return false, nil
		}
	}

	// Limita la llamada: un host colgado no debe frenar a las demás.
	callCtx, cancel := h.withAcquirerTimeout(ctx)
	defer cancel()
	if err := h.acquirer.Reverse(callCtx, tx); err != nil {
		h.metrics.RecordAcquirerError(ctx)
		return false, fmt.Errorf("acquirer reversal: %w", err)
	}

	if err := tx.Reject(valueobject.NewRejectionFromTimeout()); err != nil {
		return false, fmt.Errorf("reject: %w", err)
	}
	// Si esto falla, la corrida siguiente repite la reversa, que no tiene efecto.
	saved, err := h.saveAndEnqueue(ctx, tx, valueobject.StateIndeterminate, func(dbTx pgx.Tx) error {
		return h.publisher.PublishRejected(ctx, dbTx, tx)
	})
	if err != nil {
		return false, fmt.Errorf("save rejection: %w", err)
	}
	if !saved {
		return false, nil
	}

	h.recordRejection(ctx, tx)
	log.Warn("transaction reconciled — reversed in the acquirer and rejected")
	return true, nil
}
