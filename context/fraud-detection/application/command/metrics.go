package command

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/pkg/observability"
)

// Metrics agrupa los instrumentos de negocio del BC Fraud Detection.
//
// Nombre final en Prometheus (el exporter agrega _total a counters y _seconds
// a histogramas con unidad "s"):
//
//	posnet_fraud_evaluations       → posnet_fraud_evaluations_total{decision}
//	posnet_fraud_score_histogram   → posnet_fraud_score_histogram
//	posnet_fraud_engine_duration   → posnet_fraud_engine_duration_seconds
//	posnet_fraud_rule_hits         → posnet_fraud_rule_hits_total{rule}
//	posnet_fraud_unevaluated       → posnet_fraud_unevaluated_total{reason}
//
// El label "bc" lo inyecta Prometheus como target label (prometheus.yml).
type Metrics struct {
	evaluations    metric.Int64Counter
	score          metric.Int64Histogram
	engineDuration metric.Float64Histogram
	ruleHits       metric.Int64Counter
	unevaluated    metric.Int64Counter
}

var (
	latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
	scoreBuckets   = []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
)

// NewMetrics crea los instrumentos usando el MeterProvider global.
// Debe llamarse después de observability.InitMeter().
func NewMetrics() (*Metrics, error) {
	m := observability.Meter("posnet.fraud_detection")

	evaluations, err := m.Int64Counter("posnet_fraud_evaluations",
		metric.WithDescription("Evaluaciones de fraude completadas, desglosadas por decision."))
	if err != nil {
		return nil, err
	}
	score, err := m.Int64Histogram("posnet_fraud_score_histogram",
		metric.WithDescription("Distribución de scores de fraude (0-100)."),
		metric.WithExplicitBucketBoundaries(scoreBuckets...))
	if err != nil {
		return nil, err
	}
	engineDuration, err := m.Float64Histogram("posnet_fraud_engine_duration",
		metric.WithDescription("Latencia de engine.Evaluate() incluyendo goroutines paralelas."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}
	ruleHits, err := m.Int64Counter("posnet_fraud_rule_hits",
		metric.WithDescription("Veces que cada regla fue disparada."))
	if err != nil {
		return nil, err
	}

	unevaluated, err := m.Int64Counter("posnet_fraud_unevaluated",
		metric.WithDescription("Pedidos sin score del motor, por reason: engine_failure (REVIEW neutral), invalid_event (REJECT) o unanswerable (sin transaction_id, sin respuesta)."))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		evaluations:    evaluations,
		score:          score,
		engineDuration: engineDuration,
		ruleHits:       ruleHits,
		unevaluated:    unevaluated,
	}, nil
}

// RecordEvaluation registra una evaluación completada con su decisión y score,
// y contabiliza cada regla disparada. Nil-safe.
func (m *Metrics) RecordEvaluation(ctx context.Context, decision string, score int, rulesHit []string) {
	if m == nil {
		return
	}
	m.evaluations.Add(ctx, 1, metric.WithAttributes(attribute.String("decision", decision)))
	m.score.Record(ctx, int64(score))
	for _, rule := range rulesHit {
		m.ruleHits.Add(ctx, 1, metric.WithAttributes(attribute.String("rule", rule)))
	}
}

// RecordEngineDuration registra la latencia del motor de reglas. Nil-safe.
func (m *Metrics) RecordEngineDuration(ctx context.Context, d time.Duration) {
	if m == nil {
		return
	}
	m.engineDuration.Record(ctx, d.Seconds())
}

// RecordUnevaluated cuenta un pedido cuyo score no salió del motor. Nil-safe.
func (m *Metrics) RecordUnevaluated(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.unevaluated.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}
