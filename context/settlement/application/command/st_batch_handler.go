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

// BatchHandler implementa port.BatchService.
// Orquesta el ciclo de vida de los batches de liquidación.
type BatchHandler struct {
	batchRepo   repository.SettlementBatchRepository
	publisher   service.EventPublisher
	processor   service.SettlementProcessor
	outbox      *outbox.Store
	idempotency *natsutil.IdempotencyStore
	pool        pgutil.PgxPool
	mdrPercent  float64
	metrics     *Metrics // opcional; nil = sin instrumentación (tests)
	location    *time.Location
	reviews     repository.ReversalReviewLog // opcional; nil = no valida las reversas
}

// SetReversalReviews habilita la validación de las reversas contra sus compras.
func (h *BatchHandler) SetReversalReviews(reviews repository.ReversalReviewLog) {
	h.reviews = reviews
}

// SetBusinessLocation define la zona horaria del día de negocio. Sin configurar, usa UTC.
func (h *BatchHandler) SetBusinessLocation(loc *time.Location) {
	h.location = loc
}

// businessDate devuelve el día de negocio de t, a medianoche UTC para que el valor
// no dependa de la zona al formatearlo o guardarlo como DATE.
func (h *BatchHandler) businessDate(t time.Time) time.Time {
	loc := h.location
	if loc == nil {
		loc = time.UTC
	}
	y, m, d := t.In(loc).Date()
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
	mdrPercent float64,
) *BatchHandler {
	return &BatchHandler{
		batchRepo:   batchRepo,
		publisher:   publisher,
		outbox:      outboxStore,
		processor:   processor,
		idempotency: idempotency,
		pool:        pool,
		mdrPercent:  mdrPercent,
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
	if err != nil {
		authorizedAt = time.Now().UTC()
	}

	var (
		batchID        string
		reviewReason   string
		reviewResolved bool
	)
	err = retryOnStale(ctx, func() error {
		batchID, reviewReason, reviewResolved = "", "", false
		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
			if err != nil {
				return err
			}
			if !inserted {
				log.Info("approval event already registered — skipping")
				return nil
			}

			batch, err := h.batchRepo.FindOrCreateOpen(ctx, terminalID, merchantID, h.businessDate(authorizedAt), cmd.Currency)
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
			if h.reviews == nil {
				return nil
			}
			// La reversa pudo llegar antes: si la estaba esperando, se compara ahora.
			reviewReason, reviewResolved, err = h.reviews.ReconcileReversalReviewTx(ctx, tx, txID.String(), repository.ReversalPurchase{
				MerchantID: merchantID.String(), AmountCents: cmd.AmountCents, Currency: cmd.Currency,
			})
			return err
		})
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("RegisterApproval: %w", err)
	}

	if reviewResolved {
		log.Info("reversal review resolved — the purchase arrived and matches")
	} else if reviewReason != "" {
		log.Warn("reversal does not match the purchase that arrived later", slog.String("reason", reviewReason))
	}
	if batchID != "" {
		h.metrics.RecordAuthApproved(ctx)
		log.Info("transaction registered in batch",
			slog.String("batch_id", batchID),
			slog.Int64("amount_cents", cmd.AmountCents),
		)
	}
	return nil
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
	if err != nil {
		completedAt = time.Now().UTC()
	}

	var (
		registered   bool
		reviewReason string
	)
	err = retryOnStale(ctx, func() error {
		registered, reviewReason = false, ""
		return pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
			inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
			if err != nil {
				return err
			}
			if !inserted {
				return nil
			}

			batch, err := h.batchRepo.FindOrCreateOpen(ctx, terminalID, merchantID, h.businessDate(completedAt), cmd.Currency)
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

	// Carga, cierre y guardado se reintentan juntos: si entró una venta en el medio,
	// el cierre tiene que volver a calcular los totales con ella.
	var batch *aggregate.SettlementBatch
	processed := false
	err = retryOnStale(ctx, func() error {
		processed = false
		var err error
		batch, err = h.batchRepo.FindOpenByTerminal(ctx, terminalID, h.closeDate(cmd), cmd.Currency)
		if err != nil {
			return fmt.Errorf("find open batch: %w", err)
		}
		if batch == nil {
			return nil
		}

		// Solicitar cierre → calcular totales → comparar con terminal
		if err := batch.RequestClose(); err != nil {
			return fmt.Errorf("request close: %w", err)
		}
		if err := batch.Close(cmd.TerminalCount, cmd.TerminalAmount); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}
		// Con discrepancias, el lote no se envía: pasa a DISPUTED en el mismo guardado.
		if batch.Discrepancies() > 0 {
			if err := batch.MarkDisputed("terminal count/amount mismatch"); err != nil {
				return fmt.Errorf("mark disputed: %w", err)
			}
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
			return enqueueBatchClosed(ctx, tx, h.publisher, h.outbox, batch)
		})
	})
	if err != nil {
		return fmt.Errorf("ProcessBatchClose: %w", err)
	}
	if batch == nil {
		log.Warn("no open batch found for terminal — nothing to close")
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
		log.Warn("batch has discrepancies — marked as disputed",
			slog.String("batch_id", batch.ID()),
			slog.Int("discrepancies", batch.Discrepancies()),
		)
		return nil
	}

	// Sin discrepancias → enviar al procesador externo
	return h.submitBatch(ctx, batch)
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

// closeDate resuelve qué día cierra el terminal: el que informó o, si no informó
// uno válido, el día de negocio del pedido.
func (h *BatchHandler) closeDate(cmd port.ProcessBatchCloseCommand) time.Time {
	if d, err := time.Parse("2006-01-02", cmd.BatchDate); err == nil {
		return d
	}
	if t, err := time.Parse(time.RFC3339, cmd.RequestedAt); err == nil {
		return h.businessDate(t)
	}
	return h.businessDate(time.Now())
}

// CloseStaleBatches cierra los batches OPEN de días de negocio anteriores a now, para los
// que el terminal nunca pidió el cierre. No toca el lote del día: cerrarlo dejaría
// afuera las ventas posteriores. Un error en un batch no frena a los demás.
func (h *BatchHandler) CloseStaleBatches(ctx context.Context, now time.Time) (closed, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.CloseStaleBatches")
	defer span.End()

	today := h.businessDate(now)
	ids, err := h.batchRepo.FindStaleOpenIDs(ctx, today)
	if err != nil {
		return 0, 0, fmt.Errorf("CloseStaleBatches: find batches: %w", err)
	}

	runDate := today.Format("2006-01-02")
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
	if err := batch.CloseWithBackendTotals(); err != nil {
		return false, fmt.Errorf("close batch: %w", err)
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
	log.Warn("batch closed automatically without terminal totals",
		slog.String("batch_date", batch.BatchDate().Format("2006-01-02")),
		slog.Int("tx_count", len(batch.Transactions())),
	)

	// Un lote vacío no tiene nada que enviar.
	if len(batch.Transactions()) == 0 {
		return true, nil
	}

	// El lote ya quedó CLOSED: si el envío falla, se reintenta con ResubmitBatch.
	if err := h.submitBatch(ctx, batch); err != nil {
		log.Error("automatic batch submit failed", slog.String("error", err.Error()))
	}
	return true, nil
}

// submitBatch envía la remesa al procesador externo y, si acepta el envío,
// transiciona el batch a SUBMITTED y persiste el cambio.
//
// Sin procesador configurado no es un error del mensaje: reintentarlo no cambia
// nada, así que el batch queda en CLOSED para un reenvío manual (ResubmitBatch).
func (h *BatchHandler) submitBatch(ctx context.Context, batch *aggregate.SettlementBatch) error {
	confirmationID, err := submitToProcessor(ctx, h.processor, batch)
	if errors.Is(err, service.ErrProcessorUnavailable) {
		h.metrics.RecordSubmission(ctx, submissionUnavailable)
		observability.FromContext(ctx).Warn("no settlement processor configured — batch remains CLOSED",
			slog.String("batch_id", batch.ID()),
		)
		return nil
	}
	if err != nil {
		h.metrics.RecordSubmission(ctx, submissionError)
		return fmt.Errorf("submitBatch: processor submit: %w", err)
	}
	h.metrics.RecordSubmission(ctx, submissionSubmitted)

	if err := batch.Submit(); err != nil {
		return fmt.Errorf("submitBatch: %w", err)
	}
	if err := h.batchRepo.Save(ctx, batch); err != nil {
		return fmt.Errorf("submitBatch: save: %w", err)
	}

	observability.FromContext(ctx).Info("batch submitted to processor",
		slog.String("batch_id", batch.ID()),
		slog.String("confirmation_id", confirmationID),
	)
	return nil
}

// submitToProcessor envía el batch, o devuelve ErrProcessorUnavailable si no hay
// procesador configurado. Evita un nil pointer: en un handler de NATS, un panic
// no se recupera y termina el proceso.
func submitToProcessor(ctx context.Context, p service.SettlementProcessor, batch *aggregate.SettlementBatch) (string, error) {
	if p == nil {
		return "", service.ErrProcessorUnavailable
	}
	return p.Submit(ctx, batch)
}
