// Package command contiene los command handlers del BC Settlement.
package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// BatchHandler orquesta el ciclo de vida de los batches de liquidación.
type BatchHandler struct {
	batchRepo   repository.SettlementBatchRepository
	publisher   service.EventPublisher
	processor   service.SettlementProcessor
	outbox      *outbox.Store
	idempotency *natsutil.IdempotencyStore
	pool        pgutil.PgxPool
	metrics     *Metrics // opcional; nil = sin instrumentación (tests)
	location    *time.Location
	reviews     repository.ReversalReviewLog     // opcional; nil = no valida las reversas
	timezones   repository.MerchantTimezones     // opcional; nil = una sola zona para todos
	maxCloseAge int                              // días; 0 = no limita la antigüedad de batch_date
	closeGrace  time.Duration                    // plazo tras la medianoche del comercio para el cierre automático
	reopens     repository.BatchReopens          // opcional; nil = acepta cualquier día dentro de maxCloseAge
	audit       repository.BatchAuditLog         // opcional; nil = la liberación de un lote no se audita
	days        repository.TerminalDayBatches    // opcional; nil = sin ventas tardías ni motivo de un cierre sin lote
	submissions repository.BatchSubmissions      // opcional; nil = no registra los envíos y un reenvío puede duplicarlos
	pending     repository.PendingTerminalCloses // opcional; nil = los totales de un cierre sin lote solo van al log
}

// SetPendingTerminalCloses guarda los totales de un cierre sin lote, para que el cierre automático
// concilie contra ellos el lote que formen las ventas que lleguen después.
func (h *BatchHandler) SetPendingTerminalCloses(p repository.PendingTerminalCloses) {
	h.pending = p
}

// SetBatchSubmissions registra los envíos al procesador, para no enviar dos veces un lote aceptado.
func (h *BatchHandler) SetBatchSubmissions(s repository.BatchSubmissions) {
	h.submissions = s
}

// SetTerminalDayBatches habilita agregar una venta tardía al lote en disputa que le faltaba, y
// distinguir por qué un cierre no encontró lote.
func (h *BatchHandler) SetTerminalDayBatches(days repository.TerminalDayBatches) {
	h.days = days
}

// SetAuditLog habilita la auditoría de lo que el sistema hace sobre un lote retenido.
func (h *BatchHandler) SetAuditLog(audit repository.BatchAuditLog) {
	h.audit = audit
}

// SetBatchReopens limita los cierres de días anteriores a ayer a los lotes que un operador
// reabrió.
func (h *BatchHandler) SetBatchReopens(reopens repository.BatchReopens) {
	h.reopens = reopens
}

// SetCloseGrace define cuánto después de la medianoche del comercio se cierra
// automáticamente el lote del día anterior.
func (h *BatchHandler) SetCloseGrace(d time.Duration) {
	h.closeGrace = d
}

// SetMerchantTimezones habilita la zona de negocio por comercio.
func (h *BatchHandler) SetMerchantTimezones(timezones repository.MerchantTimezones) {
	h.timezones = timezones
}

// SetMaxCloseAge define la antigüedad máxima, en días, del batch_date que acepta un cierre.
func (h *BatchHandler) SetMaxCloseAge(days int) {
	h.maxCloseAge = days
}

// SetReversalReviews habilita la validación de las reversas contra sus compras.
func (h *BatchHandler) SetReversalReviews(reviews repository.ReversalReviewLog) {
	h.reviews = reviews
}

// SetBusinessLocation define la zona de los comercios sin una propia. Sin configurar, usa UTC.
func (h *BatchHandler) SetBusinessLocation(loc *time.Location) {
	h.location = loc
}

// defaultLocation devuelve la zona de los comercios sin una propia.
func (h *BatchHandler) defaultLocation() *time.Location {
	if h.location == nil {
		return time.UTC
	}
	return h.location
}

// businessDate devuelve el día de negocio de t en la zona del comercio, a medianoche UTC
// para que el valor no dependa de la zona al formatearlo o guardarlo como DATE.
func (h *BatchHandler) businessDate(ctx context.Context, merchantID string, t time.Time) (time.Time, error) {
	if h.timezones == nil {
		return h.inZone(t, "")
	}
	name, err := h.timezones.MerchantTimezone(ctx, merchantID)
	if err != nil {
		return time.Time{}, fmt.Errorf("business date: %w", err)
	}
	return h.inZone(t, name)
}

// businessDateTx es businessDate para una venta o reversa que se va a guardar en tx: lee la zona
// con el lock del comercio, para que un cambio de zona no mueva los lotes en el medio.
func (h *BatchHandler) businessDateTx(ctx context.Context, tx pgx.Tx, merchantID string, t time.Time) (time.Time, error) {
	if h.timezones == nil {
		return h.inZone(t, "")
	}
	name, err := h.timezones.MerchantTimezoneTx(ctx, tx, merchantID)
	if err != nil {
		return time.Time{}, fmt.Errorf("business date: %w", err)
	}
	return h.inZone(t, name)
}

// inZone devuelve la fecha de t en la zona name, o en la general si name es vacío.
func (h *BatchHandler) inZone(t time.Time, name string) (time.Time, error) {
	loc := h.defaultLocation()
	if name != "" {
		var err error
		if loc, err = time.LoadLocation(name); err != nil {
			return time.Time{}, fmt.Errorf("business date: load timezone: %w", err)
		}
	}
	return midnightUTC(t.In(loc)), nil
}

// midnightUTC devuelve la fecha de t a medianoche UTC.
func midnightUTC(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// SetMetrics inyecta los instrumentos de negocio (desde el wiring, tras InitMeter).
func (h *BatchHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

func NewBatchHandler(
	batchRepo repository.SettlementBatchRepository,
	publisher service.EventPublisher,
	outboxStore *outbox.Store,
	processor service.SettlementProcessor,
	idempotency *natsutil.IdempotencyStore,
	pool pgutil.PgxPool,
) *BatchHandler {
	return &BatchHandler{
		batchRepo:   batchRepo,
		publisher:   publisher,
		outbox:      outboxStore,
		processor:   processor,
		idempotency: idempotency,
		pool:        pool,
	}
}

// RegisterApproval agrega una transacción aprobada al batch del terminal.
func (h *BatchHandler) RegisterApproval(ctx context.Context, cmd port.RegisterApprovalCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.RegisterApproval")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("transaction_id", cmd.TransactionID),
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("event_id", cmd.EventID),
	)

	// Parsear Value Objects
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid merchant_id")
	}
	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id")
	}
	if _, err := domain.ParseCurrency(cmd.Currency); err != nil {
		return pkgerrors.NewValidationError("invalid currency")
	}
	if cmd.AmountCents <= 0 {
		return pkgerrors.NewValidationError("amount_cents must be positive")
	}
	authorizedAt, err := time.Parse(time.RFC3339, cmd.AuthorizedAt)
	authorizedKnown := err == nil
	if err != nil {
		authorizedAt = time.Now().UTC()
	}

	var (
		batchID string
		review  repository.ReviewReconciliation
		late    *lateSale
	)
	err = retryOnStale(ctx, func() error {
		batchID, review, late = "", repository.ReviewReconciliation{}, nil
		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
			if err != nil {
				return err
			}
			if !inserted {
				log.Info("approval event already registered — skipping")
				return nil
			}
			batchDate, err := h.businessDateTx(ctx, tx, merchantID.String(), authorizedAt)
			if err != nil {
				return err
			}

			// Una venta que el terminal contó al cerrar, pero que llegó después del cierre por un
			// reintento, va al lote en disputa que le faltaba, no a uno nuevo.
			if authorizedKnown {
				late, err = h.addLate(ctx, tx, lateTx{
					terminalID: terminalID, date: batchDate, currency: cmd.Currency, at: authorizedAt,
					txID: txID, amount: cmd.AmountCents, txType: valueobject.BatchTxPurchase,
				})
				if err != nil {
					return err
				}
			}
			if late != nil {
				batchID = late.batch.ID()
			} else {
				batch, err := h.batchRepo.FindOrCreateOpenTx(ctx, tx, terminalID, merchantID, batchDate, cmd.Currency)
				if err != nil {
					return fmt.Errorf("find or create batch: %w", err)
				}
				if err := batch.AddTransaction(txID, cmd.AmountCents, valueobject.BatchTxPurchase); err != nil {
					return fmt.Errorf("add transaction: %w", err)
				}
				batchID = batch.ID()
				if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
					return err
				}
			}
			if h.reviews == nil {
				return nil
			}
			// La reversa pudo llegar antes: si la estaba esperando, se compara ahora.
			review, err = h.reviews.ReconcileReversalReviewTx(ctx, tx, txID.String(), batchID, repository.ReversalPurchase{
				MerchantID: merchantID.String(), AmountCents: cmd.AmountCents, Currency: cmd.Currency,
			})
			return err
		})
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("RegisterApproval: %w", err)
	}

	switch {
	case review.Restored:
		log.Warn("excluded reversal restored — its purchase arrived and matches", slog.String("batch_id", review.BatchID))
	case review.Reopened:
		log.Warn("excluded reversal does not match the purchase that arrived — review reopened, batch held",
			slog.String("batch_id", review.BatchID), slog.String("reason", review.Reason))
	case review.Resolved:
		log.Info("reversal review resolved — the purchase arrived and matches")
		// Era la revisión que retenía el lote de la reversa: si era la última, se libera.
		if err := h.ReleaseHeldBatch(ctx, review.BatchID); err != nil {
			log.Error("could not release the held batch — it stays DISPUTED", slog.String("batch_id", review.BatchID), slog.String("error", err.Error()))
		}
	case review.Reason != "":
		log.Warn("reversal does not match the purchase that arrived later", slog.String("reason", review.Reason))
	}
	if batchID != "" {
		h.metrics.RecordAuthApproved(ctx)
		log.Info("transaction registered in batch",
			slog.String("batch_id", batchID),
			slog.Int64("amount_cents", cmd.AmountCents),
		)
	}
	if late != nil {
		h.finishLateSale(ctx, late)
	}
	return nil
}

// lateTx es una venta o reversa que llegó después del cierre del lote que el terminal la contó.
type lateTx struct {
	terminalID domain.TerminalID
	date       time.Time
	currency   string
	at         time.Time // Autorización de la venta o finalización de la reversa
	txID       domain.TransactionID
	amount     int64
	txType     valueobject.BatchTransactionType
	// hold, si está, revisa la transacción en el lote antes de decidir si se libera: true lo
	// retiene. La reversa se compara con su compra, y si no coinciden abre una revisión.
	hold func(batchID string) (bool, error)
}

// lateSale es una venta o reversa agregada a un lote en disputa después de su cierre.
type lateSale struct {
	batch    *aggregate.SettlementBatch
	txType   valueobject.BatchTransactionType
	previous int  // Discrepancias antes de la transacción
	released bool // Con la transacción coincide y pasó a CLOSED
}

// addLate agrega la transacción al lote en disputa del terminal que la había contado, si hay uno,
// y lo libera si ahora coincide y no le quedan revisiones abiertas. Todo en tx, con la auditoría.
// Devuelve nil si la transacción no corresponde a ningún lote en disputa.
func (h *BatchHandler) addLate(ctx context.Context, tx pgx.Tx, l lateTx) (*lateSale, error) {
	if h.days == nil {
		return nil, nil
	}
	id, err := h.days.FindLateBatchTx(ctx, tx, l.terminalID, l.date, l.currency, l.at)
	if err != nil || id == "" {
		return nil, err
	}
	batch, err := h.batchRepo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load disputed batch: %w", err)
	}
	if batch == nil {
		return nil, nil
	}
	previous := batch.Discrepancies()
	var added bool
	if l.txType == valueobject.BatchTxReversal {
		added, err = batch.AddLateReversal(l.txID, l.amount, l.at)
	} else {
		added, err = batch.AddLateSale(l.txID, l.amount, l.at)
	}
	if err != nil || !added {
		return nil, err
	}
	held := false
	if l.hold != nil {
		if held, err = l.hold(id); err != nil {
			return nil, err
		}
	}
	released := false
	if batch.Discrepancies() == 0 && !held {
		open, err := h.openReviews(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("count open reviews: %w", err)
		}
		if open == 0 {
			if released, _, err = batch.Release(); err != nil {
				return nil, fmt.Errorf("release: %w", err)
			}
		}
	}
	if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
		return nil, err
	}
	if h.audit != nil {
		action, what, when := repository.AuditActionLateSale, "sale", "authorized"
		if l.txType == valueobject.BatchTxReversal {
			action, what, when = repository.AuditActionLateReversal, "reversal", "completed"
		}
		disputed := valueobject.BatchStateDisputed.String()
		if err := h.audit.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
			BatchID: id, Action: action, OperatorID: "system",
			Reason: fmt.Sprintf("%s %s of %d, %s at %s before the close, arrived late: discrepancies %d → %d",
				what, l.txID, l.amount, when, l.at.UTC().Format(time.RFC3339), previous, batch.Discrepancies()),
			FromState: disputed, ToState: disputed, PreviousDiscrepancies: previous,
		}); err != nil {
			return nil, err
		}
		if released {
			if err := h.audit.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
				BatchID: id, Action: repository.AuditActionAutoRelease, OperatorID: "system",
				Reason:    "a late " + what + " completed the batch and it reconciles against " + batch.ReconciledWith(),
				FromState: disputed, ToState: batch.State().String(), PreviousDiscrepancies: batch.Discrepancies(),
			}); err != nil {
				return nil, err
			}
		}
	}
	return &lateSale{batch: batch, txType: l.txType, previous: previous, released: released}, nil
}

// finishLateSale informa la transacción tardía y, si liberó el lote, lo envía.
func (h *BatchHandler) finishLateSale(ctx context.Context, late *lateSale) {
	what, record := "sale", h.metrics.RecordLateSale
	if late.txType == valueobject.BatchTxReversal {
		what, record = "reversal", h.metrics.RecordLateReversal
	}
	log := observability.FromContext(ctx).With(
		slog.String("batch_id", late.batch.ID()),
		slog.Int("previous_discrepancies", late.previous),
		slog.Int("discrepancies", late.batch.Discrepancies()),
	)
	switch {
	case late.released:
		record(ctx, lateSaleReleased)
		log.Warn("late " + what + " added to its disputed batch — it now reconciles and is released")
		// Ya quedó CLOSED: si el envío falla, se reintenta con ResubmitBatch, como tras un cierre.
		if err := h.submitBatch(ctx, late.batch); err != nil {
			log.Error("submit after late "+what+" failed — batch stays CLOSED", slog.String("error", err.Error()))
		}
	case late.batch.Discrepancies() == 0:
		record(ctx, lateSaleHeld)
		log.Warn("late " + what + " added to its disputed batch — it reconciles, but a reversal review still holds it")
	default:
		record(ctx, lateSaleDisputed)
		log.Warn("late " + what + " added to its disputed batch — it still does not reconcile")
	}
}

// RegisterReversal descuenta una anulación del batch del terminal.
func (h *BatchHandler) RegisterReversal(ctx context.Context, cmd port.RegisterReversalCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.RegisterReversal")
	defer span.End()

	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid merchant_id")
	}
	origTxID, err := domain.ParseTransactionID(cmd.OriginalTransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid original_transaction_id")
	}
	if _, err := domain.ParseCurrency(cmd.Currency); err != nil {
		return pkgerrors.NewValidationError("invalid currency")
	}
	if cmd.AmountCents <= 0 {
		return pkgerrors.NewValidationError("amount_cents must be positive")
	}
	completedAt, err := time.Parse(time.RFC3339, cmd.CompletedAt)
	completedKnown := err == nil
	if err != nil {
		completedAt = time.Now().UTC()
	}

	var (
		registered   bool
		reviewReason string
		late         *lateSale
	)
	err = retryOnStale(ctx, func() error {
		registered, reviewReason, late = false, "", nil
		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
			if err != nil {
				return err
			}
			if !inserted {
				return nil
			}
			batchDate, err := h.businessDateTx(ctx, tx, merchantID.String(), completedAt)
			if err != nil {
				return err
			}

			// Una reversa que el terminal contó al cerrar, pero que llegó después, va al lote en
			// disputa que le faltaba. Se compara con su compra antes de decidir si el lote se libera.
			if completedKnown {
				late, err = h.addLate(ctx, tx, lateTx{
					terminalID: terminalID, date: batchDate, currency: cmd.Currency, at: completedAt,
					txID: origTxID, amount: cmd.AmountCents, txType: valueobject.BatchTxReversal,
					hold: func(batchID string) (bool, error) {
						reason, err := h.reviewReversal(ctx, tx, batchID, origTxID.String(), merchantID.String(), cmd)
						reviewReason = reason
						return reason != "", err
					},
				})
				if err != nil {
					return err
				}
				if late != nil {
					registered = true
					return nil
				}
			}

			batch, err := h.batchRepo.FindOrCreateOpenTx(ctx, tx, terminalID, merchantID, batchDate, cmd.Currency)
			if err != nil {
				return fmt.Errorf("find or create batch: %w", err)
			}
			if err := batch.RemoveTransaction(origTxID, cmd.AmountCents); err != nil {
				return fmt.Errorf("remove transaction: %w", err)
			}
			registered = true
			if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
				return err
			}
			reviewReason, err = h.reviewReversal(ctx, tx, batch.ID(), origTxID.String(), merchantID.String(), cmd)
			return err
		})
	})
	if err != nil {
		return err
	}
	if registered {
		h.metrics.RecordReversal(ctx)
	}
	if reviewReason != "" {
		observability.FromContext(ctx).Warn("reversal registered for review",
			slog.String("transaction_id", cmd.OriginalTransactionID),
			slog.String("event_id", cmd.EventID),
			slog.String("reason", reviewReason),
			slog.Int64("amount_cents", cmd.AmountCents),
			slog.String("currency", cmd.Currency),
		)
	}
	if late != nil {
		h.finishLateSale(ctx, late)
	}
	return nil
}

// reviewReversal compara la reversa con su compra y, si no coinciden, abre una revisión.
// Devuelve el motivo, o vacío si coinciden.
func (h *BatchHandler) reviewReversal(ctx context.Context, tx pgx.Tx, batchID, transactionID, merchantID string, cmd port.RegisterReversalCommand) (string, error) {
	if h.reviews == nil {
		return "", nil
	}
	purchase, err := h.reviews.FindPurchaseTx(ctx, tx, transactionID)
	if err != nil {
		return "", err
	}
	reason := reversalMismatch(purchase, merchantID, cmd.AmountCents, cmd.Currency)
	if reason == "" {
		return "", nil
	}
	err = h.reviews.InsertReversalReviewTx(ctx, tx, repository.ReversalReview{
		TransactionID: transactionID,
		BatchID:       batchID,
		Reason:        reason,
		MerchantID:    merchantID,
		AmountCents:   cmd.AmountCents,
		Currency:      cmd.Currency,
		Purchase:      purchase,
	})
	return reason, err
}

// reversalMismatch devuelve por qué la reversa no coincide con la compra, o vacío si
// coincide. Authorization siempre revierte el monto completo: no hay reversas parciales.
func reversalMismatch(purchase *repository.ReversalPurchase, merchantID string, amountCents int64, currency string) string {
	switch {
	case purchase == nil:
		return repository.ReversalReviewPurchaseNotFound
	case purchase.MerchantID != merchantID:
		return repository.ReversalReviewMerchantMismatch
	case purchase.Currency != currency:
		return repository.ReversalReviewCurrencyMismatch
	case purchase.AmountCents != amountCents:
		return repository.ReversalReviewAmountMismatch
	}
	return ""
}

// ProcessBatchClose procesa el cierre de lote solicitado por el terminal.
func (h *BatchHandler) ProcessBatchClose(ctx context.Context, cmd port.ProcessBatchCloseCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ProcessBatchClose")
	defer span.End()

	closeStart := time.Now()

	log := observability.FromContext(ctx).With(
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("batch_date", cmd.BatchDate),
		slog.String("event_id", cmd.EventID),
	)

	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}

	if _, err := domain.ParseCurrency(cmd.Currency); err != nil {
		return pkgerrors.NewValidationError("invalid currency")
	}

	closeDate, beforeYesterday, err := h.closeDate(ctx, cmd)
	if err != nil {
		return fmt.Errorf("ProcessBatchClose: %w", err)
	}

	// Carga, cierre y guardado se reintentan juntos: si entró una venta en el medio,
	// el cierre tiene que volver a calcular los totales con ella.
	var batch *aggregate.SettlementBatch
	processed := false
	err = retryOnStale(ctx, func() error {
		processed = false
		var err error
		batch, err = h.batchRepo.FindOpenByTerminal(ctx, terminalID, closeDate, cmd.Currency)
		if err != nil {
			return fmt.Errorf("find open batch: %w", err)
		}
		if batch == nil {
			return nil
		}
		if beforeYesterday {
			if err := h.checkReopened(ctx, batch, cmd.BatchDate); err != nil {
				return err
			}
		}
		terminalCount, terminalAmount, err := h.pendingTerminalTotals(ctx, batch, cmd)
		if err != nil {
			return fmt.Errorf("terminal totals: %w", err)
		}
		// Se lee antes de guardar. Una reversa posterior cambia la versión del lote: el
		// guardado da ErrStaleBatch y este bloque se repite.
		openReviews, err := h.openReviews(ctx, batch.ID())
		if err != nil {
			return fmt.Errorf("count open reviews: %w", err)
		}

		// Solicitar cierre → calcular totales → comparar con terminal
		if err := batch.RequestClose(); err != nil {
			return fmt.Errorf("request close: %w", err)
		}
		if err := batch.Close(terminalCount, terminalAmount); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}
		// Con discrepancias o una reversa en revisión, el lote no se envía: pasa a
		// DISPUTED en el mismo guardado.
		switch {
		case batch.Discrepancies() > 0:
			if err := batch.MarkDisputed("terminal count/amount mismatch"); err != nil {
				return fmt.Errorf("mark disputed: %w", err)
			}
		case openReviews > 0:
			if err := batch.MarkDisputed(disputeReasonReversalReview); err != nil {
				return fmt.Errorf("mark disputed: %w", err)
			}
		}
		if err := voidIfEmpty(batch); err != nil {
			return err
		}

		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
			if err != nil {
				return err
			}
			if !inserted {
				log.Info("batch close event already processed — skipping")
				return nil
			}
			processed = true
			if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
				return err
			}
			// Unos totales guardados de un cierre sin lote quedan reemplazados por estos.
			if h.pending != nil {
				if err := h.pending.DeletePendingCloseTx(ctx, tx, terminalID, closeDate, cmd.Currency); err != nil {
					return err
				}
			}
			return enqueueBatchClosed(ctx, tx, h.publisher, h.outbox, batch)
		})
	})
	if err != nil {
		return fmt.Errorf("ProcessBatchClose: %w", err)
	}
	if batch == nil {
		if err := h.closeWithoutBatch(ctx, terminalID, closeDate, cmd); err != nil {
			return fmt.Errorf("ProcessBatchClose: %w", err)
		}
		return nil
	}
	if !processed {
		return nil
	}
	h.metrics.RecordBatchClosed(ctx, batch.Currency(), batch.State().String(), time.Since(closeStart))

	log.Info("batch closed successfully",
		slog.String("batch_id", batch.ID()),
		slog.String("state", batch.State().String()),
		slog.Int("discrepancies", batch.Discrepancies()),
	)

	if batch.State() == valueobject.BatchStateDisputed {
		if batch.Discrepancies() == 0 {
			log.Warn("batch held — reversals under review", slog.String("batch_id", batch.ID()))
			return nil
		}
		log.Warn("batch has discrepancies — marked as disputed",
			slog.String("batch_id", batch.ID()),
			slog.Int("discrepancies", batch.Discrepancies()),
		)
		return nil
	}
	if batch.State() == valueobject.BatchStateVoid {
		log.Info("empty batch closed as VOID — nothing to submit", slog.String("batch_id", batch.ID()))
		return nil
	}

	// Sin discrepancias → enviar al procesador externo
	return h.submitBatch(ctx, batch)
}

// closeWithoutBatch informa un cierre que no encontró un lote OPEN. Terminal Gateway ya respondió
// 202, así que el terminal no se entera: la métrica es lo que lo hace visible.
//
//   - El día tiene lotes, ninguno OPEN: suele ser un cierre repetido (already_closed).
//   - El día no tiene lotes y el terminal informó ventas: el backend no tiene ninguna. Pueden estar
//     reintentándose, o haberse perdido (no_batch). Hay que revisarlo. Los totales se guardan: el
//     cierre automático concilia contra ellos el lote que formen las ventas que lleguen después.
//     Si no se pueden guardar, devuelve el error y el cierre se reintenta.
//   - Sin lotes ni ventas: un día sin actividad. No se cuenta.
func (h *BatchHandler) closeWithoutBatch(ctx context.Context, terminalID domain.TerminalID, date time.Time, cmd port.ProcessBatchCloseCommand) error {
	log := observability.FromContext(ctx).With(
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("batch_date", date.Format("2006-01-02")),
		slog.String("currency", cmd.Currency),
		slog.Int("terminal_count", cmd.TerminalCount),
		slog.Int64("terminal_amount", cmd.TerminalAmount),
	)
	batches := -1
	if h.days != nil {
		n, err := h.days.CountTerminalDayBatches(ctx, terminalID, date, cmd.Currency)
		if err != nil {
			log.Error("could not check the terminal's batches for the day", slog.String("error", err.Error()))
		} else {
			batches = n
		}
	}
	switch {
	case batches > 0:
		h.metrics.RecordCloseRejected(ctx, closeRejectedAlreadyClosed)
		log.Warn("no open batch for the terminal — the day's batches are already closed", slog.Int("batches", batches))
	case cmd.TerminalCount > 0 || cmd.TerminalAmount != 0:
		saved := h.pending != nil
		if saved {
			requestedAt, err := time.Parse(time.RFC3339, cmd.RequestedAt)
			if err != nil {
				requestedAt = time.Now().UTC()
			}
			if err := h.pending.SavePendingClose(ctx, repository.PendingClose{
				TerminalID: terminalID, BatchDate: date, Currency: cmd.Currency,
				TerminalCount: cmd.TerminalCount, TerminalAmount: cmd.TerminalAmount,
				EventID: cmd.EventID, RequestedAt: requestedAt,
			}); err != nil {
				return fmt.Errorf("save the totals of a close without batch: %w", err)
			}
		}
		h.metrics.RecordCloseRejected(ctx, closeRejectedNoBatch)
		// Con totals_saved, el cierre automático concilia contra ellos el lote que formen las ventas
		// que lleguen después.
		log.Error("terminal reported sales for a day without any batch — they may still be retrying, or be lost",
			slog.Bool("totals_saved", saved))
	default:
		log.Info("no open batch for the terminal and no sales reported — nothing to close")
	}
	return nil
}

// ReleaseHeldBatch vuelve a conciliar un lote DISPUTED cuando ya no tiene revisiones de reversa
// abiertas (ver SettlementBatch.Release). Si concilia, pasa a CLOSED, con auditoría, y se envía
// al procesador. No hace nada si el lote no está DISPUTED o le quedan revisiones. Lo llaman el
// cierre de una revisión, por el sistema o por un operador: si falla, el lote sigue DISPUTED y
// ReleaseHeldBatches lo reintenta a los pocos minutos, aparte del cierre automático.
func (h *BatchHandler) ReleaseHeldBatch(ctx context.Context, batchID string) error {
	log := observability.FromContext(ctx).With(slog.String("batch_id", batchID))
	var (
		batch    *aggregate.SettlementBatch
		released bool
	)
	err := retryOnStale(ctx, func() error {
		batch, released = nil, false
		loaded, err := h.batchRepo.FindByID(ctx, batchID)
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) || (err == nil && loaded == nil) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("find batch: %w", err)
		}
		if loaded.State() != valueobject.BatchStateDisputed {
			return nil
		}
		open, err := h.openReviews(ctx, batchID)
		if err != nil {
			return fmt.Errorf("count open reviews: %w", err)
		}
		if open > 0 {
			return nil
		}
		previous := loaded.Discrepancies()
		rel, changed, err := loaded.Release()
		if err != nil {
			return fmt.Errorf("release: %w", err)
		}
		if !changed {
			batch = loaded
			return nil
		}
		if rel {
			if err := voidIfEmpty(loaded); err != nil {
				return err
			}
		}
		err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			if err := h.batchRepo.SaveTx(ctx, tx, loaded); err != nil {
				return err
			}
			if !rel || h.audit == nil {
				return nil
			}
			return h.audit.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
				BatchID: batchID, Action: repository.AuditActionAutoRelease, OperatorID: "system",
				Reason:    "last reversal review closed and the batch reconciles against " + loaded.ReconciledWith(),
				FromState: valueobject.BatchStateDisputed.String(), ToState: loaded.State().String(),
				PreviousDiscrepancies: previous,
			})
		})
		if err != nil {
			return err
		}
		batch, released = loaded, rel
		return nil
	})
	if err != nil {
		return fmt.Errorf("ReleaseHeldBatch: %w", err)
	}
	switch {
	case batch == nil:
		return nil
	case !released && batch.ReconciledWith() == "":
		log.Warn("batch has no open reviews but was closed before its reconciliation was recorded — resolve it with resolve-dispute or reopen")
		return nil
	case !released:
		log.Warn("batch has no open reviews but still does not reconcile — stays DISPUTED", slog.Int("discrepancies", batch.Discrepancies()))
		return nil
	}
	log.Warn("held batch released — no open reviews and it reconciles",
		slog.String("reconciled_with", batch.ReconciledWith()), slog.String("state", batch.State().String()))
	if batch.State() == valueobject.BatchStateVoid {
		return nil
	}
	// Ya quedó CLOSED: si el envío falla, se reintenta con ResubmitBatch, como tras un cierre.
	if err := h.submitBatch(ctx, batch); err != nil {
		log.Error("submit after release failed — batch stays CLOSED", slog.String("error", err.Error()))
	}
	return nil
}

// ReleaseHeldBatches reintenta liberar los lotes retenidos cuyas revisiones ya se cerraron: los
// que ReleaseHeldBatch no pudo liberar porque falló, y los cerrados antes de 000007 que la
// migración 000010 completó. Lo llama el job de cierre automático en cada barrido. Devuelve
// cuántos intentó y cuántos fallaron; los que fallan se reintentan en el barrido siguiente.
func (h *BatchHandler) ReleaseHeldBatches(ctx context.Context) (retried, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.ReleaseHeldBatches")
	defer span.End()

	if h.reviews == nil {
		return 0, 0, nil
	}
	ids, err := h.reviews.FindReleasableHeldBatchIDs(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("ReleaseHeldBatches: find batches: %w", err)
	}
	for _, id := range ids {
		observability.FromContext(ctx).Info("retrying the release of a held batch with no open reviews", slog.String("batch_id", id))
		if err := h.ReleaseHeldBatch(ctx, id); err != nil {
			failed++
			observability.RecordError(ctx, err)
			observability.FromContext(ctx).Error("could not release the held batch — it stays DISPUTED, retried on the next sweep",
				slog.String("batch_id", id), slog.String("error", err.Error()))
		}
	}
	return len(ids), failed, nil
}

// disputeReasonReversalReview es el motivo de disputa de un lote con reversas en revisión.
const disputeReasonReversalReview = "reversal under review"

// openReviews cuenta las revisiones abiertas de las reversas del lote.
func (h *BatchHandler) openReviews(ctx context.Context, batchID string) (int, error) {
	if h.reviews == nil {
		return 0, nil
	}
	return h.reviews.CountOpenReversalReviews(ctx, batchID)
}

// pendingTerminalTotals devuelve los totales del terminal que corresponden a este lote.
// Un terminal que no reinicia sus contadores informa el acumulado del día: si coincide
// exactamente con este lote más los ya cerrados, se concilia solo lo pendiente. Cualquier
// otra diferencia se deja como está, y el cierre da DISPUTED.
func (h *BatchHandler) pendingTerminalTotals(ctx context.Context, batch *aggregate.SettlementBatch, cmd port.ProcessBatchCloseCommand) (int, int64, error) {
	count, amount, err := batch.BackendTotals()
	if err != nil {
		return 0, 0, err
	}
	if cmd.TerminalCount == count && cmd.TerminalAmount == amount {
		return count, amount, nil
	}
	priorCount, priorAmount, err := h.batchRepo.ClosedTotalsForTerminalDay(ctx, batch.TerminalID(), batch.BatchDate(), batch.Currency(), batch.ID())
	if err != nil {
		return 0, 0, err
	}
	if priorCount > 0 && cmd.TerminalCount == count+priorCount && cmd.TerminalAmount == amount+priorAmount {
		observability.FromContext(ctx).Info("terminal reported cumulative totals — reconciling the pending batch only",
			slog.String("batch_id", batch.ID()),
			slog.Int("terminal_count", cmd.TerminalCount),
			slog.Int("already_closed_count", priorCount),
		)
		return count, amount, nil
	}
	return cmd.TerminalCount, cmd.TerminalAmount, nil
}

// enqueueBatchClosed guarda el evento BatchClosed en el outbox, dentro de tx.
func enqueueBatchClosed(ctx context.Context, tx pgx.Tx, publisher service.EventPublisher, store *outbox.Store, batch *aggregate.SettlementBatch) error {
	subject, eventID, payload, err := publisher.BuildBatchClosed(ctx, batch)
	if err != nil {
		return fmt.Errorf("build BatchClosed: %w", err)
	}
	return store.InsertTx(ctx, tx, subject, eventID, payload)
}

// Reintentos ante un conflicto de versión. Son internos porque las entregas de NATS
// se reintentan de inmediato: con varias ventas simultáneas de un terminal, un
// conflicto agotaba MaxDeliver y la venta se perdía.
const (
	staleRetryAttempts = 8
	staleRetryBaseWait = 10 * time.Millisecond
	staleRetryMaxWait  = 400 * time.Millisecond
)

// retryOnStale ejecuta fn y la repite con espera aleatoria mientras el batch haya
// cambiado desde que se cargó. fn debe volver a cargarlo en cada intento.
func retryOnStale(ctx context.Context, fn func() error) error {
	wait := staleRetryBaseWait
	var err error
	for attempt := 1; attempt <= staleRetryAttempts; attempt++ {
		if err = fn(); !errors.Is(err, repository.ErrStaleBatch) {
			return err
		}
		if attempt == staleRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(wait/2 + rand.N(wait/2+1)):
		}
		wait = min(wait*2, staleRetryMaxWait)
	}
	return err
}

// closeDate resuelve qué día cierra el terminal: el que informó o, si no informó uno
// válido, el día de negocio del pedido. Rechaza un día futuro, o más antiguo que maxCloseAge:
// reintentarlo no lo arregla, y un lote así lo cierra el job automático o un operador.
//
// beforeYesterday indica un día anterior a ayer. Hoy y ayer son los casos normales: el cierre
// del día, o el de ayer pedido después de medianoche. Uno más viejo solo vale para un lote que
// un operador reabrió (ver checkReopened).
func (h *BatchHandler) closeDate(ctx context.Context, cmd port.ProcessBatchCloseCommand) (date time.Time, beforeYesterday bool, err error) {
	requestedAt, err := time.Parse(time.RFC3339, cmd.RequestedAt)
	if err != nil {
		requestedAt = time.Now()
	}
	today, err := h.requestBusinessDate(ctx, cmd.MerchantID, requestedAt)
	if err != nil {
		return time.Time{}, false, err
	}

	date, err = time.Parse("2006-01-02", cmd.BatchDate)
	if err != nil {
		return today, false, nil
	}
	if date.After(today) {
		h.metrics.RecordCloseRejected(ctx, closeRejectedFuture)
		return time.Time{}, false, pkgerrors.NewValidationError(
			fmt.Sprintf("batch_date %s is after the business date %s", cmd.BatchDate, today.Format("2006-01-02")),
		)
	}
	if h.maxCloseAge > 0 && date.Before(today.AddDate(0, 0, -h.maxCloseAge)) {
		h.metrics.RecordCloseRejected(ctx, closeRejectedTooOld)
		return time.Time{}, false, pkgerrors.NewValidationError(
			fmt.Sprintf("batch_date %s is more than %d days old", cmd.BatchDate, h.maxCloseAge),
		)
	}
	return date, date.Before(today.AddDate(0, 0, -1)), nil
}

// checkReopened rechaza el cierre de un lote anterior a ayer que ningún operador reabrió: el
// terminal se equivocó de día, y sus totales darían una disputa falsa sobre un lote que el job
// automático ya iba a cerrar.
func (h *BatchHandler) checkReopened(ctx context.Context, batch *aggregate.SettlementBatch, batchDate string) error {
	if h.reopens == nil {
		return nil
	}
	reopened, err := h.reopens.WasReopened(ctx, batch.ID())
	if err != nil {
		return fmt.Errorf("check reopened: %w", err)
	}
	if reopened {
		return nil
	}
	h.metrics.RecordCloseRejected(ctx, closeRejectedNotReopened)
	return pkgerrors.NewValidationError(fmt.Sprintf(
		"batch_date %s is before yesterday and batch %s was not reopened by an operator", batchDate, batch.ID(),
	))
}

// requestBusinessDate calcula el día de negocio de un pedido cuyo comercio puede no ser
// válido: el cierre nunca lo exigió, así que usa la zona general en vez de fallar.
func (h *BatchHandler) requestBusinessDate(ctx context.Context, merchantID string, t time.Time) (time.Time, error) {
	merchant, err := domain.ParseMerchantID(merchantID)
	if err != nil {
		return midnightUTC(t.In(h.defaultLocation())), nil
	}
	return h.businessDate(ctx, merchant.String(), t)
}

// CloseStaleBatches cierra los batches OPEN de días de negocio anteriores a now, para los
// que el terminal nunca pidió el cierre, una vez pasado closeGrace desde la medianoche de su
// comercio. No toca el lote del día: cerrarlo dejaría afuera las ventas posteriores. Un error
// en un batch no frena a los demás.
func (h *BatchHandler) CloseStaleBatches(ctx context.Context, now time.Time) (closed, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.CloseStaleBatches")
	defer span.End()

	loc := h.defaultLocation()
	// Un lote vence cuando now − closeGrace ya es otro día en la zona de su comercio.
	ids, err := h.batchRepo.FindStaleOpenIDs(ctx, now.Add(-h.closeGrace), loc.String())
	if err != nil {
		return 0, 0, fmt.Errorf("CloseStaleBatches: find batches: %w", err)
	}

	runDate := now.In(loc).Format("2006-01-02")
	for _, id := range ids {
		wasClosed, err := h.autoCloseBatch(ctx, id, runDate)
		if err != nil {
			failed++
			observability.RecordError(ctx, err)
			observability.FromContext(ctx).Error("automatic batch close failed",
				slog.String("batch_id", id),
				slog.String("error", err.Error()),
			)
			continue
		}
		if wasClosed {
			closed++
		}
	}
	return closed, failed, nil
}

// autoCloseBatch cierra un batch con los totales del backend. El evento sintético,
// determinístico por batch y día, evita que dos instancias cierren el mismo batch;
// incluir el día permite cerrar en la siguiente corrida un batch reabierto.
func (h *BatchHandler) autoCloseBatch(ctx context.Context, batchID, runDate string) (bool, error) {
	closeStart := time.Now()
	log := observability.FromContext(ctx).With(slog.String("batch_id", batchID))

	batch, err := h.batchRepo.FindByID(ctx, batchID)
	if err != nil {
		return false, fmt.Errorf("find batch: %w", err)
	}
	if batch == nil || batch.State() != valueobject.BatchStateOpen {
		return false, nil
	}
	openReviews, err := h.openReviews(ctx, batchID)
	if err != nil {
		return false, fmt.Errorf("count open reviews: %w", err)
	}
	// Si el terminal pidió el cierre del día antes de que hubiera un lote, se concilia contra sus
	// totales, como en su cierre; si no, con los del backend.
	var pending *repository.PendingClose
	if h.pending != nil {
		if pending, err = h.pending.FindPendingClose(ctx, batch.TerminalID(), batch.BatchDate(), batch.Currency()); err != nil {
			return false, fmt.Errorf("find pending terminal close: %w", err)
		}
	}
	if pending != nil {
		count, amount, err := h.pendingTerminalTotals(ctx, batch, port.ProcessBatchCloseCommand{
			TerminalCount: pending.TerminalCount, TerminalAmount: pending.TerminalAmount,
		})
		if err != nil {
			return false, fmt.Errorf("terminal totals: %w", err)
		}
		if err := batch.RequestClose(); err != nil {
			return false, fmt.Errorf("request close: %w", err)
		}
		if err := batch.Close(count, amount); err != nil {
			return false, fmt.Errorf("close batch: %w", err)
		}
	} else if err := batch.CloseWithBackendTotals(); err != nil {
		return false, fmt.Errorf("close batch: %w", err)
	}
	switch {
	case batch.Discrepancies() > 0:
		if err := batch.MarkDisputed("terminal count/amount mismatch"); err != nil {
			return false, fmt.Errorf("mark disputed: %w", err)
		}
	case openReviews > 0:
		if err := batch.MarkDisputed(disputeReasonReversalReview); err != nil {
			return false, fmt.Errorf("mark disputed: %w", err)
		}
	}
	if err := voidIfEmpty(batch); err != nil {
		return false, err
	}

	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("settlement-auto-close:"+batchID+":"+runDate)).String()
	processed := false
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, eventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		processed = true
		if err := h.batchRepo.SaveTx(ctx, tx, batch); err != nil {
			return err
		}
		if pending != nil {
			if err := h.pending.DeletePendingCloseTx(ctx, tx, batch.TerminalID(), batch.BatchDate(), batch.Currency()); err != nil {
				return err
			}
		}
		// Un lote vacío (lo crea una reversa de otra fecha) no tiene nada que informar.
		if len(batch.Transactions()) == 0 {
			return nil
		}
		return enqueueBatchClosed(ctx, tx, h.publisher, h.outbox, batch)
	})
	if err != nil {
		return false, fmt.Errorf("persist batch: %w", err)
	}
	if !processed {
		return false, nil
	}
	h.metrics.RecordBatchClosed(ctx, batch.Currency(), batch.State().String(), time.Since(closeStart))
	if pending != nil {
		log.Warn("batch closed automatically against the totals of an earlier terminal close without batch",
			slog.String("batch_date", batch.BatchDate().Format("2006-01-02")),
			slog.Int("tx_count", len(batch.Transactions())),
			slog.Int("terminal_count", pending.TerminalCount),
			slog.Int64("terminal_amount", pending.TerminalAmount),
			slog.Int("discrepancies", batch.Discrepancies()),
		)
	} else {
		log.Warn("batch closed automatically without terminal totals",
			slog.String("batch_date", batch.BatchDate().Format("2006-01-02")),
			slog.Int("tx_count", len(batch.Transactions())),
		)
	}

	if batch.State() == valueobject.BatchStateDisputed {
		if batch.Discrepancies() > 0 {
			log.Warn("batch has discrepancies — marked as disputed", slog.Int("discrepancies", batch.Discrepancies()))
		} else {
			log.Warn("batch held — reversals under review")
		}
		return true, nil
	}

	// Un lote vacío quedó VOID: no tiene nada que enviar.
	if batch.State() == valueobject.BatchStateVoid {
		log.Info("empty batch closed as VOID — nothing to submit")
		return true, nil
	}

	// El lote ya quedó CLOSED: si el envío falla, se reintenta con ResubmitBatch.
	if err := h.submitBatch(ctx, batch); err != nil {
		log.Error("automatic batch submit failed", slog.String("error", err.Error()))
	}
	return true, nil
}

// voidIfEmpty anula un lote que quedó CLOSED sin transacciones, en el mismo guardado del cierre o
// de la liberación: no tiene nada que enviar ni liquidar, y en CLOSED quedaba para siempre.
func voidIfEmpty(batch *aggregate.SettlementBatch) error {
	if batch.State() != valueobject.BatchStateClosed || !batch.IsEmpty() {
		return nil
	}
	if err := batch.Void(); err != nil {
		return fmt.Errorf("void empty batch: %w", err)
	}
	return nil
}

// submitBatch envía la remesa al procesador externo y, si acepta el envío,
// transiciona el batch a SUBMITTED y persiste el cambio.
//
// Sin procesador configurado no es un error del mensaje: reintentarlo no cambia
// nada, así que el batch queda en CLOSED para un reenvío manual (ResubmitBatch).
func (h *BatchHandler) submitBatch(ctx context.Context, batch *aggregate.SettlementBatch) error {
	log := observability.FromContext(ctx).With(slog.String("batch_id", batch.ID()))
	sent, err := sendToProcessor(ctx, h.processor, h.submissions, batch)
	if errors.Is(err, service.ErrProcessorUnavailable) {
		h.metrics.RecordSubmission(ctx, submissionUnavailable)
		log.Warn("no settlement processor configured — batch remains CLOSED")
		return nil
	}
	// Como sin procesador, reintentar no cambia nada: queda CLOSED y lo informa la alerta.
	if errors.Is(err, repository.ErrSubmittedTotalsChanged) {
		h.metrics.RecordSubmission(ctx, submissionTotalsChanged)
		log.Error("batch totals changed since it was sent to the processor — not sent again, batch remains CLOSED",
			slog.String("error", err.Error()))
		return nil
	}
	if err != nil {
		h.metrics.RecordSubmission(ctx, submissionError)
		return fmt.Errorf("submitBatch: processor submit: %w", err)
	}
	if sent.recovered {
		h.metrics.RecordSubmission(ctx, submissionRecovered)
	} else {
		h.metrics.RecordSubmission(ctx, submissionSubmitted)
	}

	if _, err := markSubmitted(ctx, h.batchRepo, batch, func(b *aggregate.SettlementBatch) error {
		return h.batchRepo.Save(ctx, b)
	}); err != nil {
		log.Error("batch accepted by processor but not saved as SUBMITTED — resubmit will not send it again",
			slog.String("confirmation_id", sent.confirmationID), slog.String("error", err.Error()))
		return fmt.Errorf("submitBatch: save: %w", err)
	}

	if sent.recovered {
		log.Warn("batch was already accepted by the processor — marked SUBMITTED without sending it again",
			slog.String("confirmation_id", sent.confirmationID))
		return nil
	}
	log.Info("batch submitted to processor", slog.String("confirmation_id", sent.confirmationID))
	return nil
}
