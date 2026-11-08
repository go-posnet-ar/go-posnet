// Package service contiene los Domain Services del BC Fraud Detection.
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/aggregate"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/entity"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/repository"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ruleEvalFn es la firma de una función que evalúa una regla específica.
// Recibe el FraudCase y el contexto de historia del terminal/comercio,
// y retorna si activó y el motivo.
type ruleEvalFn func(ctx context.Context, fc *aggregate.FraudCase, history RuleContext) (activated bool, reason string, err error)

// RuleContext agrupa los datos de historial necesarios para evaluar las reglas.
// Se construye una sola vez por transacción antes de evaluar todas las reglas.
type RuleContext struct {
	TxPerHour        int   // Transacciones del terminal en la última hora
	AvgMerchantAmt   int64 // Monto promedio del comercio (últimos 30 días)
	RecentRejections int   // Rechazos del terminal en los últimos 10 minutos
	SameAmountCount  int   // Intentos con el mismo monto en los últimos 5 minutos
}

// RuleEngine es el Domain Service que ejecuta todas las reglas de fraude
// en paralelo y agrega los scores para producir el FraudScore final.
// Es stateless — el estado está en el FraudCase.
type RuleEngine struct {
	ruleRepo      repository.FraudRuleRepository
	histRepo      repository.TransactionHistoryRepository
	evalFns       map[string]ruleEvalFn // ruleID → función de evaluación
	engineTimeout time.Duration
	timeouts      metric.Int64Counter // posnet_fraud_engine_timeouts_total (opcional)
}

// SetTimeoutCounter inyecta el counter de timeouts del motor. Se llama desde el
// wiring (tras InitMeter) para no acoplar el dominio al paquete observability.
// Nil-safe: si no se inyecta, no se contabiliza.
func (re *RuleEngine) SetTimeoutCounter(c metric.Int64Counter) {
	re.timeouts = c
}

// NewRuleEngine construye el motor con sus repositorios.
// engineTimeout es el tiempo máximo total para evaluar todas las reglas.
func NewRuleEngine(
	ruleRepo repository.FraudRuleRepository,
	histRepo repository.TransactionHistoryRepository,
	engineTimeout time.Duration,
) *RuleEngine {
	re := &RuleEngine{
		ruleRepo:      ruleRepo,
		histRepo:      histRepo,
		engineTimeout: engineTimeout,
	}
	re.registerEvalFns()
	return re
}

// Evaluate ejecuta todas las reglas activas sobre el FraudCase en paralelo.
// Aplica las evaluaciones al aggregate y retorna el score final.
// Timeout total configurable — si alguna regla no responde, se omite con score 0.
// Sin reglas activas, aprueba con score 0 y la evaluación NO_ACTIVE_RULES.
func (re *RuleEngine) Evaluate(ctx context.Context, fc *aggregate.FraudCase) error {
	// Timeout propio del motor — independiente del contexto padre
	ctx, cancel := context.WithTimeout(ctx, re.engineTimeout)
	defer cancel()

	// Cargar reglas activas desde Postgres (cacheadas en memoria)
	rules, err := re.ruleRepo.FindAllActive(ctx)
	if err != nil {
		return fmt.Errorf("rule_engine: load active rules: %w", err)
	}
	if len(rules) == 0 {
		// Sin reglas activas el control de fraude está apagado: score 0, APPROVE. No es una
		// falla del motor, que daría un score neutral.
		return fc.ApplyNoActiveRules()
	}

	// Construir el contexto de historial una sola vez (queries en paralelo)
	ruleCtx, err := re.buildRuleContext(ctx, fc)
	if err != nil {
		// Si falla el historial, continuar con contexto vacío
		// para no bloquear la transacción por un problema de infraestructura
		ruleCtx = RuleContext{}
	}

	// Evaluar todas las reglas en goroutines paralelas
	type evalResult struct {
		evaluation valueobject.RuleEvaluation
		err        error
	}

	resultCh := make(chan evalResult, len(rules))

	for _, rule := range rules {
		rule := rule // captura de loop
		go func() {
			eval := re.evaluateRule(ctx, fc, rule, ruleCtx)
			resultCh <- evalResult{evaluation: eval}
		}()
	}

	// Recolectar resultados
	evaluations := make([]valueobject.RuleEvaluation, 0, len(rules))
	for range rules {
		result := <-resultCh
		if result.err == nil {
			evaluations = append(evaluations, result.evaluation)
		}
	}

	// El motor agotó su presupuesto de tiempo (EvalTimeout): alguna regla no
	// respondió a tiempo. Se contabiliza aunque el score parcial siga siendo válido.
	if ctx.Err() == context.DeadlineExceeded && re.timeouts != nil {
		re.timeouts.Add(ctx, 1)
	}

	if len(evaluations) == 0 {
		return fmt.Errorf("rule_engine: all rule evaluations failed")
	}

	return fc.ApplyEvaluations(evaluations)
}

// evaluateRule ejecuta una regla individual y retorna su RuleEvaluation.
// Si la regla no tiene función de evaluación registrada, se omite sin activar.
func (re *RuleEngine) evaluateRule(
	ctx context.Context,
	fc *aggregate.FraudCase,
	rule *entity.FraudRule,
	ruleCtx RuleContext,
) valueobject.RuleEvaluation {
	evalFn, ok := re.evalFns[rule.ID()]
	if !ok {
		// Regla sin implementación — no activar, score 0
		eval, _ := valueobject.NewRuleEvaluation(rule.ID(), rule.Name(), false, 0, "no eval function registered")
		return eval
	}

	activated, reason, err := evalFn(ctx, fc, ruleCtx)
	if err != nil || !activated {
		eval, _ := valueobject.NewRuleEvaluation(rule.ID(), rule.Name(), false, 0, reason)
		return eval
	}

	eval, _ := valueobject.NewRuleEvaluation(rule.ID(), rule.Name(), true, rule.ScoreWeight(), reason)
	return eval
}

// buildRuleContext construye el contexto de historial en paralelo.
func (re *RuleEngine) buildRuleContext(ctx context.Context, fc *aggregate.FraudCase) (RuleContext, error) {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		ruleCtx RuleContext
		errs    []error
	)

	queries := []struct {
		name string
		fn   func() (interface{}, error)
	}{
		{"txPerHour", func() (interface{}, error) {
			return re.histRepo.CountByTerminalLastHour(ctx, fc.TerminalID())
		}},
		{"avgMerchantAmt", func() (interface{}, error) {
			return re.histRepo.AverageAmountByMerchant(ctx, fc.MerchantID())
		}},
		{"recentRejections", func() (interface{}, error) {
			return re.histRepo.CountRecentRejectionsByTerminal(ctx, fc.TerminalID(), 10)
		}},
		{"sameAmountCount", func() (interface{}, error) {
			return re.histRepo.CountSameAmountAttempts(ctx, fc.TerminalID(), fc.AmountCents(), 5)
		}},
	}

	for _, q := range queries {
		q := q
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, err := q.fn()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", q.name, err))
				return
			}
			switch q.name {
			case "txPerHour":
				ruleCtx.TxPerHour = val.(int)
			case "avgMerchantAmt":
				ruleCtx.AvgMerchantAmt = val.(int64)
			case "recentRejections":
				ruleCtx.RecentRejections = val.(int)
			case "sameAmountCount":
				ruleCtx.SameAmountCount = val.(int)
			}
		}()
	}
	wg.Wait()

	if len(errs) == len(queries) {
		return RuleContext{}, fmt.Errorf("all history queries failed")
	}
	return ruleCtx, nil
}

// registerEvalFns registra las funciones de evaluación por ruleID.
// Los IDs deben coincidir con los registrados en la tabla fraud_rules de Postgres.
func (re *RuleEngine) registerEvalFns() {
	re.evalFns = map[string]ruleEvalFn{

		// RULE-001: Velocidad — más de 60 transacciones por hora en el terminal
		"RULE-001": func(ctx context.Context, fc *aggregate.FraudCase, h RuleContext) (bool, string, error) {
			if h.TxPerHour > 60 {
				return true, fmt.Sprintf("terminal processed %d tx in last hour (limit: 60)", h.TxPerHour), nil
			}
			return false, "", nil
		},

		// RULE-002: Monto inusual — más de 3x el promedio del comercio
		"RULE-002": func(ctx context.Context, fc *aggregate.FraudCase, h RuleContext) (bool, string, error) {
			if h.AvgMerchantAmt > 0 && fc.AmountCents() > h.AvgMerchantAmt*3 {
				return true, fmt.Sprintf("amount %d is >3x merchant average %d", fc.AmountCents(), h.AvgMerchantAmt), nil
			}
			return false, "", nil
		},

		// RULE-003: Múltiples rechazos recientes — más de 3 en los últimos 10 minutos
		"RULE-003": func(ctx context.Context, fc *aggregate.FraudCase, h RuleContext) (bool, string, error) {
			if h.RecentRejections > 3 {
				return true, fmt.Sprintf("terminal had %d rejections in last 10 min (limit: 3)", h.RecentRejections), nil
			}
			return false, "", nil
		},

		// RULE-004: Mismo monto repetido — mismo monto exacto más de una vez en 5 minutos
		"RULE-004": func(ctx context.Context, fc *aggregate.FraudCase, h RuleContext) (bool, string, error) {
			if h.SameAmountCount > 1 {
				return true, fmt.Sprintf("amount %d attempted %d times in last 5 min", fc.AmountCents(), h.SameAmountCount), nil
			}
			return false, "", nil
		},

		// RULE-005: Tarjeta sin chip (magstripe) con monto alto
		"RULE-005": func(ctx context.Context, fc *aggregate.FraudCase, h RuleContext) (bool, string, error) {
			// Monto > ARS 50.000 (5_000_000 centavos) con magstripe
			if fc.EntryMode() == "MAGSTRIPE" && fc.AmountCents() > 5_000_000 {
				return true, fmt.Sprintf("magstripe entry with high amount %d cents", fc.AmountCents()), nil
			}
			return false, "", nil
		},
	}
}

// EventPublisher es el puerto de salida hacia NATS JetStream.
type EventPublisher interface {
	// PublishFraudScoreCalculated encola el resultado en el outbox, dentro de dbTx: sale a
	// POSNET_FRAUD solo si la transacción confirma. Consumido por: Authorization BC.
	PublishFraudScoreCalculated(ctx context.Context, dbTx pgx.Tx, fc *aggregate.FraudCase) error
}

// ParseTerminalID re-exporta para uso en el motor sin imports adicionales.
func parseTerminalID(s string) (domain.TerminalID, error) {
	return domain.ParseTerminalID(s)
}
