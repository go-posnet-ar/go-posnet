package command

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/pkg/observability"
)

// Metrics agrupa los instrumentos de negocio del BC Settlement.
//
// Nombre final en Prometheus (el exporter agrega _total a counters y _seconds
// a histogramas con unidad "s"):
//
//	posnet_settlement_auth_approved → posnet_settlement_auth_approved_total
//	posnet_settlement_reversals     → posnet_settlement_reversals_total
//	posnet_settlement_batches       → posnet_settlement_batches_total{currency,state}
//	posnet_settlement_batch_duration → posnet_settlement_batch_duration_seconds
//	posnet_settlement_submissions   → posnet_settlement_submissions_total{result}
//
// El label "bc" lo inyecta Prometheus como target label (prometheus.yml).
type Metrics struct {
	authApproved  metric.Int64Counter
	reversals     metric.Int64Counter
	batches       metric.Int64Counter
	batchDuration metric.Float64Histogram
	submissions   metric.Int64Counter
}

// Valores del label result de posnet_settlement_submissions_total.
const (
	submissionSubmitted   = "submitted"
	submissionUnavailable = "unavailable"
	submissionError       = "error"
)

var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}

// NewMetrics crea los instrumentos usando el MeterProvider global.
func NewMetrics() (*Metrics, error) {
	m := observability.Meter("posnet.settlement")

	authApproved, err := m.Int64Counter("posnet_settlement_auth_approved",
		metric.WithDescription("Eventos AuthApproved registrados en un batch."))
	if err != nil {
		return nil, err
	}
	reversals, err := m.Int64Counter("posnet_settlement_reversals",
		metric.WithDescription("Anulaciones descontadas de un batch."))
	if err != nil {
		return nil, err
	}
	batches, err := m.Int64Counter("posnet_settlement_batches",
		metric.WithDescription("Lotes procesados, desglosados por currency y state."))
	if err != nil {
		return nil, err
	}
	batchDuration, err := m.Float64Histogram("posnet_settlement_batch_duration",
		metric.WithDescription("Latencia del cierre de batch."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}
	submissions, err := m.Int64Counter("posnet_settlement_submissions",
		metric.WithDescription("Intentos de envío al procesador tras un cierre, por result."))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		authApproved:  authApproved,
		reversals:     reversals,
		batches:       batches,
		batchDuration: batchDuration,
		submissions:   submissions,
	}, nil
}

// RecordSubmission contabiliza un intento de envío al procesador. Nil-safe.
func (m *Metrics) RecordSubmission(ctx context.Context, result string) {
	if m == nil {
		return
	}
	m.submissions.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func (m *Metrics) RecordAuthApproved(ctx context.Context) {
	if m == nil {
		return
	}
	m.authApproved.Add(ctx, 1)
}

func (m *Metrics) RecordReversal(ctx context.Context) {
	if m == nil {
		return
	}
	m.reversals.Add(ctx, 1)
}

// RecordBatchClosed contabiliza un batch cerrado y su latencia de cierre.
func (m *Metrics) RecordBatchClosed(ctx context.Context, currency, state string, d time.Duration) {
	if m == nil {
		return
	}
	m.batches.Add(ctx, 1, metric.WithAttributes(
		attribute.String("currency", currency),
		attribute.String("state", state),
	))
	m.batchDuration.Record(ctx, d.Seconds())
}
