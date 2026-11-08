// Package command contiene los command handlers del BC Fraud Detection.
package command

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/fraud-detection/application/port"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// EvaluateTransactionHandler implementa port.FraudService.
// Orquesta: idempotencia → crear FraudCase → ejecutar motor → persistir y encolar el score.
type EvaluateTransactionHandler struct {
	fraudCaseRepo repository.FraudCaseRepository
	engine        *service.RuleEngine
	publisher     service.EventPublisher
	idempotency   *natsutil.IdempotencyStore
	pool          pgutil.PgxPool
	metrics       *Metrics // opcional; nil = sin instrumentación (tests)
}

// SetMetrics inyecta los instrumentos de negocio (desde el wiring, tras InitMeter).
func (h *EvaluateTransactionHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

// NewEvaluateTransactionHandler construye el handler con sus dependencias.
func NewEvaluateTransactionHandler(
	fraudCaseRepo repository.FraudCaseRepository,
	engine *service.RuleEngine,
	publisher service.EventPublisher,
	idempotency *natsutil.IdempotencyStore,
	pool pgutil.PgxPool,
) *EvaluateTransactionHandler {
	return &EvaluateTransactionHandler{
		fraudCaseRepo: fraudCaseRepo,
		engine:        engine,
		publisher:     publisher,
		idempotency:   idempotency,
		pool:          pool,
	}
}

// outcome indica de dónde salió el score publicado.
type outcome string

const (
	outcomeEvaluated     outcome = "evaluated"      // Lo calculó el motor
	outcomeEngineFailure outcome = "engine_failure" // El motor falló: score neutral, REVIEW
	outcomeInvalidEvent  outcome = "invalid_event"  // El pedido no se puede evaluar: REJECT
	outcomeStored        outcome = "stored"         // La transacción ya tenía caso: se repite su score
)

// Largos de las columnas de fraud_cases. Un valor más largo haría fallar el INSERT en cada
// reintento: se trata como pedido inválido.
const (
	maxCurrencyLen    = 3
	maxCardNetworkLen = 20
	maxEntryModeLen   = 20
)

// EvaluateTransaction es el caso de uso principal del BC Fraud Detection.
//
// Flujo:
//  1. Resolver el transaction_id: el del payload o, si es ilegible, el correlation_id del
//     envelope. Sin ninguno no hay a quién responder, y el evento se descarta
//  2. En una transacción:
//     a. Marcar el evento como procesado. Si ya lo estaba, se descarta sin evaluar. Una entrega
//     simultánea del mismo evento espera en la clave única hasta que esta confirme
//     b. Si la transacción ya tiene un caso, por otro pedido, se responde con su score
//     c. Si no, evaluar: REJECT si el pedido no se puede evaluar, el score del motor, o el
//     neutral si el motor falla. Guardar el caso
//     d. Encolar FraudScoreCalculated en el outbox. El relay lo publica a NATS para Authorization
//
// La evaluación corre dentro de la transacción: retiene una conexión hasta EvalTimeout. El
// subscriber procesa un mensaje a la vez, así que es una por instancia.
func (h *EvaluateTransactionHandler) EvaluateTransaction(
	ctx context.Context,
	cmd port.EvaluateTransactionCommand,
) error {
	ctx, span := observability.StartSpan(ctx, "command.EvaluateTransaction")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("transaction_id", cmd.TransactionID),
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("event_id", cmd.EventID),
	)

	// ── 1. transaction_id ────────────────────────────────────────────────────
	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	fromEnvelope := false
	if err != nil {
		envID, envErr := domain.ParseTransactionID(cmd.CorrelationID)
		if envErr != nil {
			log.Error("fraud check request without a valid transaction_id — dropped, there is no one to answer",
				slog.String("error", err.Error()), slog.String("correlation_id", cmd.CorrelationID))
			h.metrics.RecordUnevaluated(ctx, "unanswerable")
			return pkgerrors.NewValidationError("invalid transaction_id: " + err.Error())
		}
		txID, fromEnvelope = envID, true
		log = log.With(slog.String("correlation_id", cmd.CorrelationID))
	}

	// ── 2. Marcar, evaluar, guardar y encolar (atómico) ──────────────────────
	var (
		fc       *aggregate.FraudCase
		result   outcome
		enqueued bool
	)
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		enqueued = false
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			log.Info("fraud check event already processed — skipping")
			return nil
		}

		fc, err = h.fraudCaseRepo.FindByTransactionIDTx(ctx, tx, txID)
		if err != nil {
			return err
		}
		if fc != nil {
			result = outcomeStored
		} else {
			if fc, result, err = h.evaluate(ctx, log, txID, cmd, fromEnvelope); err != nil {
				return err
			}
			saved, err := h.fraudCaseRepo.Save(ctx, tx, fc)
			if err != nil {
				return err
			}
			if !saved {
				// Otro pedido de la misma transacción guardó primero: se responde con su score.
				if fc, err = h.fraudCaseRepo.FindByTransactionIDTx(ctx, tx, txID); err != nil {
					return err
				}
				if fc == nil {
					return fmt.Errorf("fraud case for %s neither inserted nor found", txID)
				}
				result = outcomeStored
			}
		}

		if err := h.publisher.PublishFraudScoreCalculated(ctx, tx, fc); err != nil {
			return err
		}
		enqueued = true
		return nil
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("EvaluateTransaction: persist: %w", err)
	}
	if !enqueued {
		return nil
	}

	if result == outcomeStored {
		log.Warn("transaction already has a fraud case — answering with the stored score",
			slog.String("decision", fc.Score().Decision().String()))
		return nil
	}

	// Las métricas, después de confirmar: un reintento no las duplica.
	h.metrics.RecordEvaluation(ctx,
		fc.Score().Decision().String(),
		fc.Score().Score(),
		fc.Score().RulesHit(),
	)
	if result != outcomeEvaluated {
		h.metrics.RecordUnevaluated(ctx, string(result))
	}
	log.Info("fraud evaluation completed",
		slog.Int("score", fc.Score().Score()),
		slog.String("decision", fc.Score().Decision().String()),
		slog.Any("rules_hit", fc.Score().RulesHit()),
		slog.String("outcome", string(result)),
	)
	return nil
}

// evaluate construye el caso y le aplica un score: el del motor, el neutral si el motor falla, o
// REJECT si el pedido no se puede evaluar. Solo devuelve error si no puede construir ninguno.
func (h *EvaluateTransactionHandler) evaluate(
	ctx context.Context,
	log *slog.Logger,
	txID domain.TransactionID,
	cmd port.EvaluateTransactionCommand,
	fromEnvelope bool,
) (*aggregate.FraudCase, outcome, error) {
	occurredAt, err := time.Parse(time.RFC3339, cmd.OccurredAt)
	if err != nil {
		occurredAt = time.Now().UTC() // fallback seguro
	}

	fc, problems := newFraudCase(txID, cmd, occurredAt)
	if fromEnvelope {
		// El transaction_id vino del envelope: el del payload era ilegible.
		fc, problems = nil, append([]string{"invalid transaction_id in the payload"}, problems...)
	}
	if fc == nil {
		reason := strings.Join(problems, "; ")
		log.Warn("fraud check request cannot be evaluated — rejecting", slog.String("reason", reason))
		invalid, err := aggregate.NewInvalidEventCase(invalidEventParams(txID, cmd, occurredAt, reason))
		if err != nil {
			return nil, "", fmt.Errorf("build invalid event case: %w", err)
		}
		return invalid, outcomeInvalidEvent, nil
	}

	engineStart := time.Now()
	err = h.engine.Evaluate(ctx, fc)
	h.metrics.RecordEngineDuration(ctx, time.Since(engineStart))
	if err == nil {
		return fc, outcomeEvaluated, nil
	}

	// Si el motor falla completamente, un score neutral (REVIEW) para no bloquear la
	// transacción por un problema de infraestructura.
	log.Error("rule engine failed — using neutral score", slog.String("error", err.Error()))
	observability.RecordError(ctx, err)
	if _, err := fc.BuildNeutralScore("RULE_ENGINE_FAILURE"); err != nil {
		return nil, "", fmt.Errorf("build neutral score: %w", err)
	}
	return fc, outcomeEngineFailure, nil
}

// newFraudCase valida el pedido y crea el caso. Si algo no es válido, devuelve nil y la lista de
// problemas.
func newFraudCase(txID domain.TransactionID, cmd port.EvaluateTransactionCommand, occurredAt time.Time) (*aggregate.FraudCase, []string) {
	var problems []string
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		problems = append(problems, "invalid terminal_id")
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		problems = append(problems, "invalid merchant_id")
	}
	if cmd.AmountCents <= 0 {
		problems = append(problems, "amount_cents must be positive")
	}
	if len(cmd.Currency) > maxCurrencyLen {
		problems = append(problems, "currency too long")
	}
	if len(cmd.CardNetwork) > maxCardNetworkLen {
		problems = append(problems, "card_network too long")
	}
	if len(cmd.EntryMode) > maxEntryModeLen {
		problems = append(problems, "entry_mode too long")
	}
	if len(problems) > 0 {
		return nil, problems
	}

	fc, err := aggregate.NewFraudCase(
		txID, terminalID, merchantID,
		cmd.AmountCents, cmd.Currency,
		cmd.CardNetwork, cmd.EntryMode,
		occurredAt,
	)
	if err != nil {
		return nil, []string{err.Error()}
	}
	return fc, nil
}

// invalidEventParams copia del pedido solo los campos válidos y que entran en sus columnas.
func invalidEventParams(txID domain.TransactionID, cmd port.EvaluateTransactionCommand, occurredAt time.Time, reason string) aggregate.InvalidEventParams {
	p := aggregate.InvalidEventParams{
		TransactionID: txID,
		AmountCents:   cmd.AmountCents,
		OccurredAt:    occurredAt,
		Reason:        reason,
	}
	if id, err := domain.ParseTerminalID(cmd.TerminalID); err == nil {
		p.TerminalID = id
	}
	if id, err := domain.ParseMerchantID(cmd.MerchantID); err == nil {
		p.MerchantID = id
	}
	if len(cmd.Currency) <= maxCurrencyLen {
		p.Currency = cmd.Currency
	}
	if len(cmd.CardNetwork) <= maxCardNetworkLen {
		p.CardNetwork = cmd.CardNetwork
	}
	if len(cmd.EntryMode) <= maxEntryModeLen {
		p.EntryMode = cmd.EntryMode
	}
	return p
}
