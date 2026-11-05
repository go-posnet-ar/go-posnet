// Package command contiene los command handlers del BC Settlement.
package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// BatchHandler implementa port.BatchService.
// Orquesta el ciclo de vida de los batches de liquidación.
type BatchHandler struct {
	batchRepo   repository.SettlementBatchRepository
	publisher   service.EventPublisher
	processor   service.SettlementProcessor
	idempotency *natsutil.IdempotencyStore
	pool        pgutil.PgxPool
	mdrPercent  float64
	metrics     *Metrics // opcional; nil = sin instrumentación (tests)
}

// SetMetrics inyecta los instrumentos de negocio (desde el wiring, tras InitMeter).
func (h *BatchHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

func NewBatchHandler(
	batchRepo repository.SettlementBatchRepository,
	publisher service.EventPublisher,
	processor service.SettlementProcessor,
	idempotency *natsutil.IdempotencyStore,
	pool pgutil.PgxPool,
	mdrPercent float64,
) *BatchHandler {
	return &BatchHandler{
		batchRepo:   batchRepo,
		publisher:   publisher,
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
	authorizedAt, err := time.Parse(time.RFC3339, cmd.AuthorizedAt)
	if err != nil {
		authorizedAt = time.Now().UTC()
	}

	var batchID string
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			log.Info("approval event already registered — skipping")
			return nil
		}

		batch, err := h.batchRepo.FindOrCreateOpen(ctx, terminalID, merchantID, authorizedAt, cmd.Currency)
		if err != nil {
			return fmt.Errorf("find or create batch: %w", err)
		}
		if err := batch.AddTransaction(txID, cmd.AmountCents, valueobject.BatchTxPurchase); err != nil {
			return fmt.Errorf("add transaction: %w", err)
		}
		batchID = batch.ID()
		return h.batchRepo.Save(ctx, batch)
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("RegisterApproval: %w", err)
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
	completedAt, err := time.Parse(time.RFC3339, cmd.CompletedAt)
	if err != nil {
		completedAt = time.Now().UTC()
	}

	var registered bool
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}

		batch, err := h.batchRepo.FindOrCreateOpen(ctx, terminalID, merchantID, completedAt, cmd.Currency)
		if err != nil {
			return fmt.Errorf("find or create batch: %w", err)
		}
		if err := batch.RemoveTransaction(origTxID); err != nil {
			return fmt.Errorf("remove transaction: %w", err)
		}
		registered = true
		return h.batchRepo.Save(ctx, batch)
	})
	if err != nil {
		return err
	}
	if registered {
		h.metrics.RecordReversal(ctx)
	}
	return nil
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

	batchDate, err := time.Parse("2006-01-02", cmd.BatchDate)
	if err != nil {
		batchDate = time.Now().UTC()
	}

	// Recuperar el batch OPEN del terminal
	batch, err := h.batchRepo.FindOpenByTerminal(ctx, terminalID, batchDate)
	if err != nil {
		return fmt.Errorf("ProcessBatchClose: find open batch: %w", err)
	}
	if batch == nil {
		log.Warn("no open batch found for terminal — nothing to close")
		return nil
	}

	// Solicitar cierre → calcular totales → comparar con terminal
	if err := batch.RequestClose(); err != nil {
		return fmt.Errorf("ProcessBatchClose: request close: %w", err)
	}
	if err := batch.Close(cmd.TerminalCount, cmd.TerminalAmount); err != nil {
		return fmt.Errorf("ProcessBatchClose: close batch: %w", err)
	}

	processed := false
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			log.Info("batch close event already processed — skipping")
			return nil
		}
		processed = true
		return h.batchRepo.Save(ctx, batch)
	})
	if err != nil {
		return fmt.Errorf("ProcessBatchClose: %w", err)
	}
	if !processed {
		return nil
	}
	h.metrics.RecordBatchClosed(ctx, batch.Currency(), batch.State().String(), time.Since(closeStart))

	// Publicar BatchClosed a NATS → Notification BC
	if err := h.publisher.PublishBatchClosed(ctx, batch); err != nil {
		log.Error("failed to publish BatchClosed", slog.String("error", err.Error()))
	}

	log.Info("batch closed successfully",
		slog.String("batch_id", batch.ID()),
		slog.Int("discrepancies", batch.Discrepancies()),
	)

	// Si hay discrepancias, marcar como DISPUTED en lugar de continuar
	if batch.Discrepancies() > 0 {
		log.Warn("batch has discrepancies — marking as disputed",
			slog.Int("discrepancies", batch.Discrepancies()),
		)
		if err := batch.MarkDisputed("terminal count/amount mismatch"); err != nil {
			return fmt.Errorf("ProcessBatchClose: mark disputed: %w", err)
		}
		return h.batchRepo.Save(ctx, batch)
	}

	// Sin discrepancias → enviar al procesador externo
	return h.submitBatch(ctx, batch)
}

// CloseStaleBatches cierra los batches OPEN de días UTC anteriores a now, para los
// que el terminal nunca pidió el cierre. No toca el lote del día: cerrarlo dejaría
// afuera las ventas posteriores. Un error en un batch no frena a los demás.
func (h *BatchHandler) CloseStaleBatches(ctx context.Context, now time.Time) (closed, failed int, err error) {
	ctx, span := observability.StartSpan(ctx, "command.CloseStaleBatches")
	defer span.End()

	utc := now.UTC()
	ids, err := h.batchRepo.FindStaleOpenIDs(ctx, utc)
	if err != nil {
		return 0, 0, fmt.Errorf("CloseStaleBatches: find batches: %w", err)
	}

	runDate := utc.Format("2006-01-02")
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
		return h.batchRepo.Save(ctx, batch)
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

	// Un lote vacío (lo crea una reversa de otra fecha) no tiene nada que informar ni enviar.
	if len(batch.Transactions()) == 0 {
		return true, nil
	}

	if err := h.publisher.PublishBatchClosed(ctx, batch); err != nil {
		log.Error("failed to publish BatchClosed", slog.String("error", err.Error()))
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
