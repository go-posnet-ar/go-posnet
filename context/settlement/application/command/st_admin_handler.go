package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// AdminHandler atiende las operaciones manuales de la API HTTP.
type AdminHandler struct {
	batchRepo  repository.SettlementBatchRepository
	processor  service.SettlementProcessor
	audit      repository.BatchAuditLog
	publisher  service.EventPublisher // opcional; nil = no genera eventos
	outbox     *outbox.Store
	pool       pgutil.PgxPool
	mdrPercent float64
	reviews    repository.ReversalReviewLog // opcional; nil = no considera las revisiones
	timezones  repository.MerchantTimezones // opcional
	location   *time.Location               // zona de los comercios sin una propia; nil = UTC
	releaser   BatchReleaser                // opcional; nil = cerrar una revisión no libera el lote

	submissions repository.BatchSubmissions // opcional; nil = no registra los envíos y un reenvío puede duplicarlos

	completions repository.SettlementCompletions // opcional; nil = cada resumen del día sale como revisión 1

	metrics       *Metrics // opcional; nil = no cuenta
	requireReason bool     // force-close, resubmit y settle rechazan un pedido sin reason
}

// SetSettlementCompletions numera las revisiones de settlement.completed y calcula su diferencia con
// la anterior.
func (h *AdminHandler) SetSettlementCompletions(c repository.SettlementCompletions) {
	h.completions = c
}

// SetMetrics habilita las métricas de las operaciones de administración.
func (h *AdminHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

// RequireReason hace obligatorio el reason de force-close, resubmit y settle. Sin esto, un pedido
// sin reason se acepta y se cuenta.
func (h *AdminHandler) RequireReason(required bool) {
	h.requireReason = required
}

// checkReason cuenta un pedido sin reason y, si es obligatorio, lo rechaza.
func (h *AdminHandler) checkReason(ctx context.Context, action, reason string) error {
	if strings.TrimSpace(reason) != "" {
		return nil
	}
	h.metrics.RecordMissingReason(ctx, action)
	if h.requireReason {
		return pkgerrors.NewValidationError("reason cannot be empty for audit")
	}
	return nil
}

// SetBatchSubmissions registra los envíos al procesador: resubmit no envía otra vez un lote ya aceptado.
func (h *AdminHandler) SetBatchSubmissions(s repository.BatchSubmissions) {
	h.submissions = s
}

// BatchReleaser libera un lote retenido cuando se cierra su última revisión de reversa.
type BatchReleaser interface {
	ReleaseHeldBatch(ctx context.Context, batchID string) error
}

// SetBatchReleaser habilita la liberación de los lotes retenidos al cerrar una revisión.
func (h *AdminHandler) SetBatchReleaser(r BatchReleaser) {
	h.releaser = r
}

// SetBusinessLocation define la zona de los comercios sin una propia: es la zona anterior al
// cambiar la de un comercio que no tenía.
func (h *AdminHandler) SetBusinessLocation(loc *time.Location) {
	h.location = loc
}

// SetReversalReviews habilita las revisiones de reversas: frenan el envío de sus lotes.
func (h *AdminHandler) SetReversalReviews(reviews repository.ReversalReviewLog) {
	h.reviews = reviews
}

// SetMerchantTimezones habilita la configuración de la zona de negocio por comercio.
func (h *AdminHandler) SetMerchantTimezones(timezones repository.MerchantTimezones) {
	h.timezones = timezones
}

func NewAdminHandler(batchRepo repository.SettlementBatchRepository, processor service.SettlementProcessor) *AdminHandler {
	return &AdminHandler{batchRepo: batchRepo, processor: processor}
}

// ConfigureSettlement inyecta lo necesario para auditar las operaciones y generar
// BatchClosed y SettlementCompleted (desde el wiring).
func (h *AdminHandler) ConfigureSettlement(pool pgutil.PgxPool, audit repository.BatchAuditLog, publisher service.EventPublisher, outboxStore *outbox.Store, mdrPercent float64) {
	h.pool = pool
	h.audit = audit
	h.publisher = publisher
	h.outbox = outboxStore
	h.mdrPercent = mdrPercent
}

// persist guarda el batch, su registro de auditoría y sus eventos (enqueue, opcional) en
// una sola transacción. Sin ConfigureSettlement, solo guarda el batch.
func (h *AdminHandler) persist(ctx context.Context, batch *aggregate.SettlementBatch, entry repository.BatchAuditEntry, enqueue func(tx pgx.Tx) error) error {
	if h.pool == nil {
		return h.batchRepo.Save(ctx, batch)
	}
	entry.BatchID = batch.ID()
	entry.ToState = batch.State().String()
	if s := batch.Summary(); s != nil {
		total := s.TotalAmount().Cents()
		entry.TotalAmount = &total
	}
	return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
			return err
		}
		if err := h.audit.InsertAuditTx(ctx, tx, entry); err != nil {
			return err
		}
		if enqueue == nil {
			return nil
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
	if err := h.checkReason(ctx, repository.AuditActionForceClose, cmd.Reason); err != nil {
		return err
	}
	if (cmd.TerminalCount == nil) != (cmd.TerminalAmount == nil) {
		return pkgerrors.NewValidationError("terminal_count and terminal_amount go together")
	}
	if cmd.TerminalCount != nil && *cmd.TerminalCount < 0 {
		return pkgerrors.NewValidationError("terminal_count cannot be negative")
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
	entry := repository.BatchAuditEntry{
		Action: repository.AuditActionForceClose, OperatorID: cmd.OperatorID, Reason: cmd.Reason,
		FromState: batch.State().String(), PreviousDiscrepancies: batch.Discrepancies(),
	}
	// Con los totales del terminal se concilia como en su cierre: si no coinciden, queda DISPUTED.
	// Sin ellos, no hay contra qué: se cierra con los del backend.
	if cmd.TerminalCount != nil {
		if err := batch.RequestClose(); err != nil {
			return fmt.Errorf("ForceClose: close: %w", err)
		}
		if err := batch.Close(*cmd.TerminalCount, *cmd.TerminalAmount); err != nil {
			return fmt.Errorf("ForceClose: close: %w", err)
		}
		if batch.Discrepancies() > 0 {
			if err := batch.MarkDisputed("terminal count/amount mismatch"); err != nil {
				return fmt.Errorf("ForceClose: %w", err)
			}
		}
	} else if err := batch.CloseWithBackendTotals(); err != nil {
		return fmt.Errorf("ForceClose: close: %w", err)
	}
	// Sin transacciones no hay nada que enviar: queda VOID, como en los otros cierres.
	if err := voidIfEmpty(batch); err != nil {
		return fmt.Errorf("ForceClose: %w", err)
	}
	err = h.persist(ctx, batch, entry, func(tx pgx.Tx) error {
		// Un lote vacío no tiene nada que informar, igual que en el cierre automático.
		if len(batch.Transactions()) == 0 {
			return nil
		}
		return enqueueBatchClosed(ctx, tx, h.publisher, h.outbox, batch, events.BatchCloseForce)
	})
	if err != nil {
		return fmt.Errorf("ForceClose: save: %w", err)
	}

	if cmd.TerminalCount == nil && !batch.IsEmpty() {
		h.metrics.RecordUnreconciledClose(ctx, unreconciledForceClose, unreconciledClosed)
	}
	observability.FromContext(ctx).Warn("batch force-closed by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("reason", cmd.Reason),
		slog.String("state", batch.State().String()),
		slog.String("reconciled_with", batch.ReconciledWith()),
		slog.Int("discrepancies", batch.Discrepancies()),
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
	if err := h.checkReason(ctx, repository.AuditActionResubmit, cmd.Reason); err != nil {
		return err
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
	entry := repository.BatchAuditEntry{
		Action: repository.AuditActionResubmit, OperatorID: cmd.OperatorID, Reason: cmd.Reason,
		FromState: batch.State().String(), PreviousDiscrepancies: batch.Discrepancies(),
	}
	openReviews, err := h.openReviews(ctx, batch.ID())
	if err != nil {
		return fmt.Errorf("ResubmitBatch: %w", err)
	}
	if batch.Discrepancies() > 0 || openReviews > 0 {
		if err := batch.MarkDisputed("resubmit blocked: batch has discrepancies or reversals under review"); err != nil {
			return fmt.Errorf("ResubmitBatch: %w", err)
		}
		entry.Action = repository.AuditActionResubmitBlocked
		if err := h.persist(ctx, batch, entry, nil); err != nil {
			return fmt.Errorf("ResubmitBatch: save: %w", err)
		}
		observability.FromContext(ctx).Warn("resubmit blocked — batch moved to DISPUTED",
			slog.String("batch_id", batch.ID()),
			slog.String("operator_id", cmd.OperatorID),
			slog.Int("discrepancies", batch.Discrepancies()),
			slog.Int("open_reviews", openReviews),
		)
		return pkgerrors.NewValidationError(fmt.Sprintf(
			"batch has %d discrepancies and %d reversals under review and was moved to DISPUTED; resolve the reviews, then use reopen or resolve-dispute",
			batch.Discrepancies(), openReviews,
		))
	}

	sent, err := sendToProcessor(ctx, h.processor, h.submissions, batch)
	if errors.Is(err, repository.ErrSubmittedTotalsChanged) {
		observability.FromContext(ctx).Error("resubmit refused — batch totals changed since it was sent to the processor",
			slog.String("batch_id", batch.ID()),
			slog.String("operator_id", cmd.OperatorID),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("ResubmitBatch: %w", err)
	}
	if err != nil {
		return fmt.Errorf("ResubmitBatch: processor submit: %w", err)
	}
	log := observability.FromContext(ctx).With(
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("confirmation_id", sent.confirmationID),
	)
	entry.ConfirmationID = sent.confirmationID
	if sent.recovered {
		entry.Reason = joinReasons(cmd.Reason, "already accepted by the processor in a previous submission: not sent again")
	}
	if _, err := markSubmitted(ctx, h.batchRepo, batch, func(b *aggregate.SettlementBatch) error {
		entry.FromState, entry.PreviousDiscrepancies = valueobject.BatchStateClosed.String(), b.Discrepancies()
		return h.persist(ctx, b, entry, nil)
	}); err != nil {
		log.Error("batch accepted by processor but not saved as SUBMITTED", slog.String("error", err.Error()))
		return fmt.Errorf("ResubmitBatch: save: %w", err)
	}

	if sent.recovered {
		log.Warn("batch was already accepted by the processor — marked SUBMITTED without sending it again")
		return nil
	}
	log.Info("batch resubmitted to processor")
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
	entry := repository.BatchAuditEntry{
		Action: repository.AuditActionReopen, OperatorID: cmd.OperatorID, Reason: cmd.Reason,
		FromState: batch.State().String(), PreviousDiscrepancies: previousDiscrepancies,
	}
	if err := batch.Reopen(); err != nil {
		return fmt.Errorf("ReopenBatch: %w", err)
	}
	if err := h.persist(ctx, batch, entry, nil); err != nil {
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
	// Aceptar los totales no puede saltear una revisión: el lote se enviaría con esa reversa.
	openReviews, err := h.openReviews(ctx, batch.ID())
	if err != nil {
		return fmt.Errorf("ResolveDispute: %w", err)
	}
	if openReviews > 0 {
		return pkgerrors.NewValidationError(fmt.Sprintf(
			"batch has %d reversals under review; resolve them first", openReviews,
		))
	}

	previousDiscrepancies := batch.Discrepancies()
	entry := repository.BatchAuditEntry{
		Action: repository.AuditActionResolveDispute, OperatorID: cmd.OperatorID, Reason: cmd.Reason,
		FromState: batch.State().String(), PreviousDiscrepancies: previousDiscrepancies,
	}
	if err := batch.ResolveDispute(); err != nil {
		return fmt.Errorf("ResolveDispute: %w", err)
	}
	// Sin transacciones no hay nada que enviar: queda VOID, como en los cierres.
	if err := voidIfEmpty(batch); err != nil {
		return fmt.Errorf("ResolveDispute: %w", err)
	}
	// Los totales pueden haber cambiado desde el cierre: se vuelven a informar, en el mismo guardado.
	err = h.persist(ctx, batch, entry, func(tx pgx.Tx) error {
		return enqueueRevisedBatch(ctx, tx, h.publisher, h.outbox, batch, events.BatchCloseResolveDispute)
	})
	if err != nil {
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
	if err := h.checkReason(ctx, repository.AuditActionSettle, cmd.Reason); err != nil {
		return err
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

	entry := repository.BatchAuditEntry{
		Action: repository.AuditActionSettle, OperatorID: cmd.OperatorID, Reason: cmd.Reason,
		FromState: batch.State().String(), PreviousDiscrepancies: batch.Discrepancies(),
	}
	if err := batch.MarkSettled(); err != nil {
		return fmt.Errorf("SettleBatch: %w", err)
	}
	err = h.persist(ctx, batch, entry, func(tx pgx.Tx) error {
		return h.enqueueCompletedIfDone(ctx, tx, batch)
	})
	if err != nil {
		return fmt.Errorf("SettleBatch: save: %w", err)
	}

	observability.FromContext(ctx).Info("batch settled by operator",
		slog.String("batch_id", batch.ID()),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("reason", cmd.Reason),
	)
	return nil
}

// joinReasons une el motivo del operador con el del sistema, si hay ambos.
func joinReasons(operator, system string) string {
	if strings.TrimSpace(operator) == "" {
		return system
	}
	return operator + " — " + system
}

// enqueueCompletedIfDone encola SettlementCompleted si ya se liquidaron todos los
// batches con transacciones del comercio en esa fecha, un evento por moneda.
func (h *AdminHandler) enqueueCompletedIfDone(ctx context.Context, tx pgx.Tx, settled *aggregate.SettlementBatch) error {
	batches, err := h.batchRepo.MerchantDayBatchesTx(ctx, tx, settled.MerchantID(), settled.BatchDate())
	if err != nil {
		return fmt.Errorf("check daily settlement completion: %w", err)
	}

	type totals struct {
		batches     int
		gross       int64
		adjustments int64
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
		t.adjustments += b.AdjustmentAmount
	}

	currencies := make([]string, 0, len(byCurrency))
	for c := range byCurrency {
		currencies = append(currencies, c)
	}
	sort.Strings(currencies)

	for _, c := range currencies {
		t := byCurrency[c]
		completion := service.SettlementCompletion{
			MerchantID: settled.MerchantID().String(), SettlementDate: settled.BatchDate().Format("2006-01-02"),
			Currency: c, MDRPercent: h.mdrPercent, Revision: 1,
			TotalBatches: t.batches, TotalAmount: t.gross, AdjustmentAmount: t.adjustments,
			NetAmount: netAmount(t.gross, h.mdrPercent),
		}
		completion.DeltaBatches, completion.DeltaAmount, completion.DeltaNetAmount = completion.TotalBatches, completion.TotalAmount, completion.NetAmount

		var previous *repository.DailyCompletion
		if h.completions != nil {
			if previous, err = h.completions.LastCompletionTx(ctx, tx, settled.MerchantID(), settled.BatchDate(), c); err != nil {
				return fmt.Errorf("find previous SettlementCompleted: %w", err)
			}
		}
		if previous != nil {
			// La condición de arriba abarca todas las monedas: la liquidación tardía de una no cambia
			// las demás, que ya se informaron.
			if previous.TotalBatches == t.batches && previous.TotalAmount == t.gross && previous.AdjustmentAmount == t.adjustments {
				continue
			}
			// Una fila de 000016 no guardó el neto: se calcula con el MDR actual, como entonces.
			previousNet := netAmount(previous.TotalAmount, h.mdrPercent)
			if previous.NetAmount != nil {
				previousNet = *previous.NetAmount
			}
			completion.Revision = previous.Revision + 1
			completion.DeltaBatches = t.batches - previous.TotalBatches
			completion.DeltaAmount = t.gross - previous.TotalAmount
			completion.DeltaNetAmount = completion.NetAmount - previousNet
		}

		subject, eventID, payload, err := h.publisher.BuildSettlementCompleted(ctx, completion)
		if err != nil {
			return fmt.Errorf("build SettlementCompleted: %w", err)
		}
		if err := h.outbox.InsertTx(ctx, tx, subject, eventID, payload); err != nil {
			return err
		}
		if h.completions != nil {
			net := completion.NetAmount
			if err := h.completions.InsertCompletionTx(ctx, tx, repository.DailyCompletion{
				MerchantID: settled.MerchantID(), Date: settled.BatchDate(), Currency: c, Revision: completion.Revision,
				TotalBatches: t.batches, TotalAmount: t.gross, AdjustmentAmount: t.adjustments,
				NetAmount: &net, EventID: eventID,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// openReviews cuenta las revisiones abiertas de las reversas del lote.
func (h *AdminHandler) openReviews(ctx context.Context, batchID string) (int, error) {
	if h.reviews == nil {
		return 0, nil
	}
	n, err := h.reviews.CountOpenReversalReviews(ctx, batchID)
	if err != nil {
		return 0, fmt.Errorf("count open reviews: %w", err)
	}
	return n, nil
}

// reviewAuditActions indica qué acción de auditoría deja cada forma de cerrar una revisión.
var reviewAuditActions = map[string]string{
	repository.ReviewActionAccept:            repository.AuditActionReviewAccept,
	repository.ReviewActionUsePurchaseAmount: repository.AuditActionReviewUsePurchaseAmount,
	repository.ReviewActionExclude:           repository.AuditActionReviewExclude,
}

// ResolveReversalReview cierra la revisión de una reversa y, si corresponde, corrige el
// lote. Solo se corrige un lote que no se envió: en OPEN o DISPUTED. La corrección, el cierre
// y la auditoría son atómicos. Si era la última revisión de un lote DISPUTED, después se lo
// vuelve a conciliar y, si coincide, se libera y se envía (ver BatchHandler.ReleaseHeldBatch).
func (h *AdminHandler) ResolveReversalReview(ctx context.Context, cmd port.ResolveReversalReviewCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ResolveReversalReview")
	defer span.End()

	if cmd.TransactionID == "" {
		return pkgerrors.NewValidationError("transaction_id cannot be empty")
	}
	if cmd.OperatorID == "" {
		return pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}
	if cmd.Resolution == "" {
		return pkgerrors.NewValidationError("resolution cannot be empty for audit")
	}
	auditAction, ok := reviewAuditActions[cmd.Action]
	if !ok {
		return pkgerrors.NewValidationError(fmt.Sprintf(
			"action must be %s, %s or %s", repository.ReviewActionAccept, repository.ReviewActionUsePurchaseAmount, repository.ReviewActionExclude,
		))
	}
	if h.reviews == nil || h.pool == nil {
		return fmt.Errorf("ResolveReversalReview: reviews are not configured")
	}

	var (
		batchID    string
		adjustment *adjustment
	)
	// Un ajuste guarda el lote abierto del terminal: si una venta lo guardó en el medio, se repite todo.
	err := retryOnStale(ctx, func() error {
		batchID, adjustment = "", nil
		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			review, err := h.reviews.LockReversalReviewTx(ctx, tx, cmd.TransactionID)
			if err != nil {
				return err
			}
			if review == nil {
				return pkgerrors.NewNotFoundError("ReversalReview", cmd.TransactionID)
			}
			if review.Resolved && !correctsSentBatch(review, cmd.Action) {
				return pkgerrors.NewValidationError("review is already resolved; it can only be corrected once its batch was sent, if it was resolved with ACCEPT or by the system")
			}
			batchID = review.BatchID
			adjustment, err = h.applyReviewAction(ctx, tx, review, cmd.Action)
			if err != nil {
				return err
			}
			if adjustment != nil {
				if err := h.audit.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
					BatchID: adjustment.batchID, Action: repository.AuditActionAdjustment, OperatorID: cmd.OperatorID,
					Reason: fmt.Sprintf("%s (ajuste de la transacción %s del lote %s, %s: %+d)",
						cmd.Resolution, cmd.TransactionID, review.BatchID, review.BatchState, adjustment.amount),
					FromState: valueobject.BatchStateOpen.String(), ToState: valueobject.BatchStateOpen.String(),
				}); err != nil {
					return err
				}
			}
			// ACCEPT sobre una reversa excluida la deja excluida: la acción guardada lo sigue diciendo.
			storedAction := cmd.Action
			if review.Action == repository.ReviewActionExclude && cmd.Action == repository.ReviewActionAccept {
				storedAction = repository.ReviewActionExclude
			}
			if err := h.reviews.CloseReversalReviewTx(ctx, tx, cmd.TransactionID, cmd.OperatorID, cmd.Resolution, storedAction); err != nil {
				return err
			}
			return h.audit.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
				BatchID: review.BatchID, Action: auditAction, OperatorID: cmd.OperatorID, Reason: cmd.Resolution,
				FromState: review.BatchState, ToState: review.BatchState, PreviousDiscrepancies: review.Discrepancies,
			})
		})
	})
	if err != nil {
		return fmt.Errorf("ResolveReversalReview: %w", err)
	}
	if adjustment != nil {
		observability.FromContext(ctx).Warn("sent batch corrected with an adjustment in the terminal's open batch",
			slog.String("transaction_id", cmd.TransactionID),
			slog.String("sent_batch_id", batchID),
			slog.String("adjustment_batch_id", adjustment.batchID),
			slog.Int64("amount_cents", adjustment.amount),
		)
	}

	observability.FromContext(ctx).Warn("reversal review resolved by operator",
		slog.String("transaction_id", cmd.TransactionID),
		slog.String("batch_id", batchID),
		slog.String("operator_id", cmd.OperatorID),
		slog.String("action", cmd.Action),
	)
	if h.releaser != nil {
		if err := h.releaser.ReleaseHeldBatch(ctx, batchID); err != nil {
			observability.FromContext(ctx).Error("could not release the held batch — it stays DISPUTED",
				slog.String("batch_id", batchID), slog.String("error", err.Error()))
		}
	}
	return nil
}

// correctsSentBatch indica si una revisión ya cerrada se puede volver a resolver: cuando se aceptó
// la reversa (ACCEPT, o la cerró el sistema porque la compra coincidió), el lote se envió, y
// después se ve que había que corregirla. La corrección es un ajuste en el lote abierto del
// terminal. Solo una vez: después la acción guardada ya es la corrección.
func correctsSentBatch(review *repository.StoredReversalReview, action string) bool {
	sent := review.BatchState == valueobject.BatchStateSubmitted.String() || review.BatchState == valueobject.BatchStateSettled.String()
	accepted := review.Action == "" || review.Action == repository.ReviewActionAccept
	corrects := action == repository.ReviewActionUsePurchaseAmount || action == repository.ReviewActionExclude
	return sent && accepted && corrects
}

// adjustment es la corrección de un lote ya enviado, agregada al lote abierto del terminal.
type adjustment struct {
	batchID string
	amount  int64 // Con signo: positivo devuelve al comercio
}

// applyReviewAction corrige la reversa en el lote, si la acción lo pide y el lote lo permite.
//
//   - Lote OPEN o DISPUTED: se corrige la reversa en el lote.
//   - Lote SUBMITTED o SETTLED: el dinero ya salió. La corrección entra como ajuste en el lote
//     abierto del mismo terminal, y se liquida con él (ver addAdjustment).
//   - Lote CLOSED: todavía no se envió; un resubmit lo pasa a DISPUTED, y ahí se corrige.
//
// Una revisión reabierta de una reversa excluida, porque su compra llegó y no coincide, tiene
// la reversa fuera de todo lote: ACCEPT la deja así, USE_PURCHASE_AMOUNT la vuelve a agregar
// con el monto de la compra, y EXCLUDE no aplica.
func (h *AdminHandler) applyReviewAction(ctx context.Context, tx pgx.Tx, review *repository.StoredReversalReview, action string) (*adjustment, error) {
	if action == repository.ReviewActionAccept {
		return nil, nil
	}
	excluded := review.Action == repository.ReviewActionExclude
	if action == repository.ReviewActionExclude && excluded {
		return nil, pkgerrors.NewValidationError("the reversal is already excluded; use ACCEPT to keep it that way")
	}
	if action == repository.ReviewActionUsePurchaseAmount && (review.Reason != repository.ReversalReviewAmountMismatch || review.Purchase == nil) {
		return nil, pkgerrors.NewValidationError(fmt.Sprintf(
			"%s only applies to %s, this review is %s", repository.ReviewActionUsePurchaseAmount, repository.ReversalReviewAmountMismatch, review.Reason,
		))
	}

	switch review.BatchState {
	case valueobject.BatchStateOpen.String(), valueobject.BatchStateDisputed.String():
	case valueobject.BatchStateSubmitted.String(), valueobject.BatchStateSettled.String():
		// Lo que se descontó de más es positivo: vuelve al comercio.
		var amount int64
		switch {
		case action == repository.ReviewActionExclude:
			amount = review.AmountCents
		case excluded:
			amount = -review.Purchase.AmountCents
		default:
			amount = review.AmountCents - review.Purchase.AmountCents
		}
		return h.addAdjustment(ctx, tx, review, amount)
	default:
		return nil, pkgerrors.NewValidationError(fmt.Sprintf(
			"batch is %s and was not sent: resubmit moves it to DISPUTED, then correct it", review.BatchState,
		))
	}

	if action == repository.ReviewActionExclude {
		return nil, h.reviews.DeleteReversalTx(ctx, tx, review.BatchID, review.TransactionID)
	}
	if excluded {
		return nil, h.reviews.InsertReversalTx(ctx, tx, review.BatchID, review.TransactionID, review.Purchase.AmountCents, review.Purchase.Currency)
	}
	return nil, h.reviews.UpdateReversalAmountTx(ctx, tx, review.BatchID, review.TransactionID, review.Purchase.AmountCents)
}

// addAdjustment agrega la corrección de un lote ya enviado al lote abierto del mismo terminal y
// moneda, en el día de negocio de hoy, o a uno nuevo. Ese lote la liquida: su total la incluye,
// pero la conciliación con el terminal no, porque el terminal no la conoce. El lote enviado no
// cambia.
func (h *AdminHandler) addAdjustment(ctx context.Context, tx pgx.Tx, review *repository.StoredReversalReview, amount int64) (*adjustment, error) {
	if amount == 0 {
		return nil, pkgerrors.NewValidationError("the correction does not change the amount: use ACCEPT")
	}
	sent, err := h.batchRepo.FindByID(ctx, review.BatchID)
	if err != nil {
		return nil, fmt.Errorf("find sent batch: %w", err)
	}
	if sent == nil {
		return nil, pkgerrors.NewNotFoundError("SettlementBatch", review.BatchID)
	}
	today, err := h.businessToday(ctx, sent.MerchantID().String())
	if err != nil {
		return nil, err
	}
	open, err := h.batchRepo.FindOrCreateOpenTx(ctx, tx, sent.TerminalID(), sent.MerchantID(), today, sent.Currency())
	if err != nil {
		return nil, fmt.Errorf("find or create the open batch: %w", err)
	}
	txID, err := domain.ParseTransactionID(review.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("adjustment: %w", err)
	}
	if err := open.AddAdjustment(txID, amount); err != nil {
		return nil, fmt.Errorf("adjustment: %w", err)
	}
	if err := h.batchRepo.SaveTx(ctx, tx, open); err != nil {
		return nil, err
	}
	return &adjustment{batchID: open.ID(), amount: amount}, nil
}

// businessToday es el día de negocio de hoy en la zona del comercio, o en la general.
func (h *AdminHandler) businessToday(ctx context.Context, merchantID string) (time.Time, error) {
	loc := time.UTC
	if h.location != nil {
		loc = h.location
	}
	if h.timezones != nil {
		name, err := h.timezones.MerchantTimezone(ctx, merchantID)
		if err != nil {
			return time.Time{}, fmt.Errorf("business date: %w", err)
		}
		if name != "" {
			if loc, err = time.LoadLocation(name); err != nil {
				return time.Time{}, fmt.Errorf("business date: load timezone: %w", err)
			}
		}
	}
	return midnightUTC(time.Now().In(loc)), nil
}

// SetMerchantTimezone define la zona del día de negocio de un comercio. Rige para las
// operaciones siguientes. Los lotes OPEN fechados hoy en la zona anterior pasan a hoy en la
// nueva, para que la venta siguiente no abra otro lote; los de días anteriores conservan su
// fecha y los cierra el job.
func (h *AdminHandler) SetMerchantTimezone(ctx context.Context, cmd port.SetMerchantTimezoneCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.SetMerchantTimezone")
	defer span.End()

	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid merchant_id")
	}
	if cmd.OperatorID == "" {
		return pkgerrors.NewValidationError("operator_id cannot be empty for audit")
	}
	// LoadLocation también acepta "" y "Local", que no significan lo mismo en cada servidor.
	if _, err := time.LoadLocation(cmd.Timezone); err != nil || cmd.Timezone == "" || cmd.Timezone == "Local" {
		return pkgerrors.NewValidationError("timezone must be a valid IANA name")
	}
	if h.timezones == nil {
		return fmt.Errorf("SetMerchantTimezone: timezones are not configured")
	}
	defaultTZ := "UTC"
	if h.location != nil {
		defaultTZ = h.location.String()
	}
	moved, err := h.timezones.SetMerchantTimezone(ctx, merchantID.String(), cmd.Timezone, defaultTZ, cmd.OperatorID)
	if errors.Is(err, repository.ErrUnknownTimezone) {
		return pkgerrors.NewValidationError("timezone must be a valid IANA name")
	}
	if err != nil {
		return fmt.Errorf("SetMerchantTimezone: %w", err)
	}
	log := observability.FromContext(ctx)
	log.Warn("merchant business timezone changed by operator",
		slog.String("merchant_id", merchantID.String()),
		slog.String("timezone", cmd.Timezone),
		slog.String("operator_id", cmd.OperatorID),
		slog.Int("batches_redated", len(moved)),
	)
	for _, b := range moved {
		log.Warn("open batch moved to the new business date",
			slog.String("batch_id", b.BatchID),
			slog.String("from", b.FromDate),
			slog.String("to", b.ToDate),
		)
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
