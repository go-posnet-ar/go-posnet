package command

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// AdminHandler implementa port.AdminService.
type AdminHandler struct {
	batchRepo  repository.SettlementBatchRepository
	processor  service.SettlementProcessor
	publisher  service.EventPublisher // opcional; nil = no genera eventos
	outbox     *outbox.Store
	pool       pgutil.PgxPool
	mdrPercent float64
}

func NewAdminHandler(batchRepo repository.SettlementBatchRepository, processor service.SettlementProcessor) *AdminHandler {
	return &AdminHandler{batchRepo: batchRepo, processor: processor}
}

// ConfigureSettlement inyecta lo necesario para generar BatchClosed y
// SettlementCompleted (desde el wiring).
func (h *AdminHandler) ConfigureSettlement(pool pgutil.PgxPool, publisher service.EventPublisher, outboxStore *outbox.Store, mdrPercent float64) {
	h.pool = pool
	h.publisher = publisher
	h.outbox = outboxStore
	h.mdrPercent = mdrPercent
}

// saveWithEvents guarda el batch y encola sus eventos en una sola transacción. Sin
// publisher configurado, solo lo guarda.
func (h *AdminHandler) saveWithEvents(ctx context.Context, batch *aggregate.SettlementBatch, enqueue func(tx pgx.Tx) error) error {
	if h.publisher == nil {
		return h.batchRepo.Save(ctx, batch)
	}
	return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
			return err
		}
		return enqueue(tx)
	})
}

// ForceClose fuerza el cierre de un batch desde operaciones de soporte, con los
// totales del backend: no hay totales del terminal contra los que conciliar. No envía
// el batch al procesador: queda en CLOSED, listo para ResubmitBatch.
func (h *AdminHandler) ForceClose(ctx context.Context, cmd port.ForceCloseCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ForceClose")
	defer span.End()

	if cmd.BatchID == "" {
		return pkgerrors.NewValidationError("batch_id cannot be empty")
	}
	if cmd.OperatorID == "" {
		return pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}

	batch, err := h.batchRepo.FindByID(ctx, cmd.BatchID)
	if err != nil {
		return fmt.Errorf("ForceClose: find batch: %w", err)
	}
	if batch == nil {
		return pkgerrors.NewNotFoundError("SettlementBatch", cmd.BatchID)
	}

	if batch.State() != valueobject.BatchStateOpen {
		return pkgerrors.NewValidationError(
			fmt.Sprintf("batch must be OPEN to force-close, current state is %s", batch.State()),
		)
	}
	if err := batch.CloseWithBackendTotals(); err != nil {
		return fmt.Errorf("ForceClose: close: %w", err)
	}
	err = h.saveWithEvents(ctx, batch, func(tx pgx.Tx) error {
		// Un lote vacío no tiene nada que informar, igual que en el cierre automático.
		if len(batch.Transactions()) == 0 {
			return nil
		}
		return enqueueBatchClosed(ctx, tx, h.publisher, h.outbox, batch)
	})
	if err != nil {
		return fmt.Errorf("ForceClose: save: %w", err)
	}

	observability.FromContext(ctx).Warn("batch force-closed by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.Int("tx_count", len(batch.Transactions())),
		slog.Int64("total_amount", batch.Summary().TotalAmount().Cents()),
	)
	return nil
}

// ResubmitBatch reintenta el envío al procesador externo de un batch que quedó
// atascado en CLOSED — típicamente porque el envío original falló por una
// falla transitoria del procesador (ver docs/runbooks/batch-close-failure.md).
// No vuelve a calcular discrepancias: asume que el batch ya fue conciliado y
// solo faltó completar el envío.
func (h *AdminHandler) ResubmitBatch(ctx context.Context, cmd port.ResubmitBatchCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ResubmitBatch")
	defer span.End()

	if cmd.BatchID == "" {
		return pkgerrors.NewValidationError("batch_id cannot be empty")
	}
	if cmd.OperatorID == "" {
		return pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}

	batch, err := h.batchRepo.FindByID(ctx, cmd.BatchID)
	if err != nil {
		return fmt.Errorf("ResubmitBatch: find batch: %w", err)
	}
	if batch == nil {
		return pkgerrors.NewNotFoundError("SettlementBatch", cmd.BatchID)
	}
	if batch.State() != valueobject.BatchStateClosed {
		return pkgerrors.NewValidationError(
			fmt.Sprintf("batch must be CLOSED to resubmit, current state is %s", batch.State()),
		)
	}
	// Un lote con discrepancias no se envía: pasa a DISPUTED, de donde el operador
	// lo saca con ReopenBatch o ResolveDispute. Cubre los lotes de ForceClose y los
	// que quedaron en CLOSED porque falló el guardado de DISPUTED durante el cierre.
	if batch.Discrepancies() > 0 {
		if err := batch.MarkDisputed("resubmit blocked: batch has discrepancies"); err != nil {
			return fmt.Errorf("ResubmitBatch: %w", err)
		}
		if err := h.batchRepo.Save(ctx, batch); err != nil {
			return fmt.Errorf("ResubmitBatch: save: %w", err)
		}
		observability.FromContext(ctx).Warn("resubmit blocked by discrepancies — batch moved to DISPUTED",
			slog.String("batch_id", batch.ID()),
			slog.String("operator_id", cmd.OperatorID),
			slog.Int("discrepancies", batch.Discrepancies()),
		)
		return pkgerrors.NewValidationError(fmt.Sprintf(
			"batch has %d discrepancies and was moved to DISPUTED; use reopen or resolve-dispute",
			batch.Discrepancies(),
		))
	}

	confirmationID, err := submitToProcessor(ctx, h.processor, batch)
	if err != nil {
		return fmt.Errorf("ResubmitBatch: processor submit: %w", err)
	}
	if err := batch.Submit(); err != nil {
		return fmt.Errorf("ResubmitBatch: %w", err)
	}
	if err := h.batchRepo.Save(ctx, batch); err != nil {
		return fmt.Errorf("ResubmitBatch: save: %w", err)
	}

	observability.FromContext(ctx).Info("batch resubmitted to processor",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("confirmation_id", confirmationID),
	)
	return nil
}

// ReopenBatch devuelve un batch DISPUTED a OPEN para que acepte transacciones
// y pueda volver a cerrarse. El nuevo cierre compara otra vez contra el terminal.
func (h *AdminHandler) ReopenBatch(ctx context.Context, cmd port.ReopenBatchCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ReopenBatch")
	defer span.End()

	batch, err := h.loadDisputed(ctx, cmd.BatchID, cmd.OperatorID, cmd.Reason)
	if err != nil {
		return fmt.Errorf("ReopenBatch: %w", err)
	}

	// Solo puede haber un lote abierto por terminal, fecha y moneda. Si ya hay otro
	// (por ventas posteriores al cierre), este se resuelve con ResolveDispute.
	open, err := h.batchRepo.FindOpenByTerminal(ctx, batch.TerminalID(), batch.BatchDate(), batch.Currency())
	if err != nil {
		return fmt.Errorf("ReopenBatch: find open batch: %w", err)
	}
	if open != nil {
		return pkgerrors.NewValidationError(fmt.Sprintf(
			"terminal already has an OPEN batch for that date and currency (%s); use resolve-dispute", open.ID(),
		))
	}

	previousDiscrepancies := batch.Discrepancies()
	if err := batch.Reopen(); err != nil {
		return fmt.Errorf("ReopenBatch: %w", err)
	}
	if err := h.batchRepo.Save(ctx, batch); err != nil {
		return fmt.Errorf("ReopenBatch: save: %w", err)
	}

	observability.FromContext(ctx).Warn("disputed batch reopened by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("reason", cmd.Reason),
		slog.Int("previous_discrepancies", previousDiscrepancies),
	)
	return nil
}

// ResolveDispute cierra un batch DISPUTED aceptando los totales del backend,
// recalculados con las transacciones actuales. No lo envía al procesador: queda
// en CLOSED, listo para ResubmitBatch.
func (h *AdminHandler) ResolveDispute(ctx context.Context, cmd port.ResolveDisputeCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ResolveDispute")
	defer span.End()

	batch, err := h.loadDisputed(ctx, cmd.BatchID, cmd.OperatorID, cmd.Reason)
	if err != nil {
		return fmt.Errorf("ResolveDispute: %w", err)
	}

	previousDiscrepancies := batch.Discrepancies()
	if err := batch.ResolveDispute(); err != nil {
		return fmt.Errorf("ResolveDispute: %w", err)
	}
	if err := h.batchRepo.Save(ctx, batch); err != nil {
		return fmt.Errorf("ResolveDispute: save: %w", err)
	}

	observability.FromContext(ctx).Warn("batch dispute resolved by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("reason", cmd.Reason),
		slog.Int("previous_discrepancies", previousDiscrepancies),
		slog.Int64("total_amount", batch.Summary().TotalAmount().Cents()),
	)
	return nil
}

// SettleBatch confirma la liquidación de un batch SUBMITTED. La confirmación es
// manual porque todavía no hay un procesador real que la informe. Cuando se
// liquidan todos los batches del comercio en esa fecha, encola SettlementCompleted.
func (h *AdminHandler) SettleBatch(ctx context.Context, cmd port.SettleBatchCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.SettleBatch")
	defer span.End()

	if cmd.BatchID == "" {
		return pkgerrors.NewValidationError("batch_id cannot be empty")
	}
	if cmd.OperatorID == "" {
		return pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}

	batch, err := h.batchRepo.FindByID(ctx, cmd.BatchID)
	if err != nil {
		return fmt.Errorf("SettleBatch: find batch: %w", err)
	}
	if batch == nil {
		return pkgerrors.NewNotFoundError("SettlementBatch", cmd.BatchID)
	}
	if batch.State() != valueobject.BatchStateSubmitted {
		return pkgerrors.NewValidationError(
			fmt.Sprintf("batch must be SUBMITTED to settle, current state is %s", batch.State()),
		)
	}

	if err := batch.MarkSettled(); err != nil {
		return fmt.Errorf("SettleBatch: %w", err)
	}
	err = h.saveWithEvents(ctx, batch, func(tx pgx.Tx) error {
		return h.enqueueCompletedIfDone(ctx, tx, batch)
	})
	if err != nil {
		return fmt.Errorf("SettleBatch: save: %w", err)
	}

	observability.FromContext(ctx).Info("batch settled by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
	)
	return nil
}

// enqueueCompletedIfDone encola SettlementCompleted si ya se liquidaron todos los
// batches con transacciones del comercio en esa fecha, un evento por moneda.
func (h *AdminHandler) enqueueCompletedIfDone(ctx context.Context, tx pgx.Tx, settled *aggregate.SettlementBatch) error {
	batches, err := h.batchRepo.MerchantDayBatchesTx(ctx, tx, settled.MerchantID(), settled.BatchDate())
	if err != nil {
		return fmt.Errorf("check daily settlement completion: %w", err)
	}

	type totals struct {
		batches int
		gross   int64
	}
	byCurrency := map[string]*totals{}
	for _, b := range batches {
		if !b.Settled {
			return nil
		}
		t, ok := byCurrency[b.Currency]
		if !ok {
			t = &totals{}
			byCurrency[b.Currency] = t
		}
		t.batches++
		t.gross += b.TotalAmount
	}

	currencies := make([]string, 0, len(byCurrency))
	for c := range byCurrency {
		currencies = append(currencies, c)
	}
	sort.Strings(currencies)

	for _, c := range currencies {
		t := byCurrency[c]
		subject, eventID, payload, err := h.publisher.BuildSettlementCompleted(ctx,
			settled.MerchantID().String(), settled.BatchDate().Format("2006-01-02"),
			t.batches, t.gross, netAmount(t.gross, h.mdrPercent), c, h.mdrPercent,
		)
		if err != nil {
			return fmt.Errorf("build SettlementCompleted: %w", err)
		}
		if err := h.outbox.InsertTx(ctx, tx, subject, eventID, payload); err != nil {
			return err
		}
	}
	return nil
}

// netAmount descuenta la comisión (MDR) del monto bruto, redondeada al centavo.
func netAmount(gross int64, mdrPercent float64) int64 {
	return gross - int64(math.Round(float64(gross)*mdrPercent/100))
}

// loadDisputed valida el pedido del operador y carga el batch, que debe estar en DISPUTED.
func (h *AdminHandler) loadDisputed(ctx context.Context, batchID, operatorID, reason string) (*aggregate.SettlementBatch, error) {
	if batchID == "" {
		return nil, pkgerrors.NewValidationError("batch_id cannot be empty")
	}
	if operatorID == "" {
		return nil, pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}
	if reason == "" {
		return nil, pkgerrors.NewValidationError("reason cannot be empty for audit")
	}

	batch, err := h.batchRepo.FindByID(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("find batch: %w", err)
	}
	if batch == nil {
		return nil, pkgerrors.NewNotFoundError("SettlementBatch", batchID)
	}
	if batch.State() != valueobject.BatchStateDisputed {
		return nil, pkgerrors.NewValidationError(
			fmt.Sprintf("batch must be DISPUTED, current state is %s", batch.State()),
		)
	}
	return batch, nil
}
