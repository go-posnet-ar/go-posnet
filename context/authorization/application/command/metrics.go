package command

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// Metrics agrupa los instrumentos de negocio del BC Authorization.
//
// Convención de nombres (Fase 2): el exporter OTel→Prometheus agrega el sufijo
// _total a los counters y _seconds a los histogramas con unidad "s". Por eso los
// instrumentos se declaran SIN esos sufijos; el nombre final en Prometheus es:
//
//	posnet_transactions_received  → posnet_transactions_received_total
//	posnet_transactions_approved  → posnet_transactions_approved_total
//	posnet_transactions_rejected  → posnet_transactions_rejected_total
//	posnet_acquirer_errors        → posnet_acquirer_errors_total
//	posnet_acquirer_request_duration → posnet_acquirer_request_duration_seconds
//	posnet_saga_duration          → posnet_saga_duration_seconds
//
// El label "bc" NO se agrega en código: lo inyecta Prometheus como target label
// en el scrape_config (deployments/docker/prometheus.yml).
type Metrics struct {
	received         metric.Int64Counter
	approved         metric.Int64Counter
	rejected         metric.Int64Counter
	acquirerErrors   metric.Int64Counter
	acquirerDuration metric.Float64Histogram
	sagaDuration     metric.Float64Histogram
	reversalsIgnored metric.Int64Counter
}

// latencyBuckets son fronteras en segundos apropiadas para la saga (~1s p50)
// y el call al adquirente (~50ms), con buena resolución sub-segundo.
// El bucket en 3 es el umbral del SLO de latencia de saga (95% < 3s).
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10}

// NewMetrics crea los instrumentos usando el MeterProvider global.
// Debe llamarse después de observability.InitMeter().
func NewMetrics() (*Metrics, error) {
	m := observability.Meter("posnet.authorization")

	received, err := m.Int64Counter("posnet_transactions_received",
		metric.WithDescription("Transacciones recibidas desde Terminal Gateway vía NATS."))
	if err != nil {
		return nil, err
	}
	approved, err := m.Int64Counter("posnet_transactions_approved",
		metric.WithDescription("Transacciones que completaron la saga con APPROVED."))
	if err != nil {
		return nil, err
	}
	rejected, err := m.Int64Counter("posnet_transactions_rejected",
		metric.WithDescription("Transacciones rechazadas (fraud o adquirente)."))
	if err != nil {
		return nil, err
	}
	acquirerErrors, err := m.Int64Counter("posnet_acquirer_errors",
		metric.WithDescription("Errores del adquirente; INDETERMINATE requiere conciliación."))
	if err != nil {
		return nil, err
	}
	acquirerDuration, err := m.Float64Histogram("posnet_acquirer_request_duration",
		metric.WithDescription("Latencia del call al host adquirente."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}
	sagaDuration, err := m.Float64Histogram("posnet_saga_duration",
		metric.WithDescription("Latencia completa de la saga desde received hasta approved/rejected."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}
	reversalsIgnored, err := m.Int64Counter("posnet_reversals_ignored",
		metric.WithDescription("Pedidos de reversa sin nada que revertir, por el estado de la transacción."))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		received:         received,
		approved:         approved,
		rejected:         rejected,
		acquirerErrors:   acquirerErrors,
		acquirerDuration: acquirerDuration,
		sagaDuration:     sagaDuration,
		reversalsIgnored: reversalsIgnored,
	}, nil
}

// Todos los métodos son nil-safe: si el handler no fue instrumentado
// (por ejemplo en tests), no hacen nada.

func (m *Metrics) RecordReceived(ctx context.Context) {
	if m == nil {
		return
	}
	m.received.Add(ctx, 1)
}

func (m *Metrics) RecordApproved(ctx context.Context) {
	if m == nil {
		return
	}
	m.approved.Add(ctx, 1)
}

func (m *Metrics) RecordRejected(ctx context.Context, rejectionCode, source string) {
	if m == nil {
		return
	}
	m.rejected.Add(ctx, 1, metric.WithAttributes(
		attribute.String("rejection_code", rejectionCode),
		attribute.String("source", source),
	))
}

func (m *Metrics) RecordReversalIgnored(ctx context.Context, state string) {
	if m == nil {
		return
	}
	m.reversalsIgnored.Add(ctx, 1, metric.WithAttributes(attribute.String("state", state)))
}

func (m *Metrics) RecordAcquirerError(ctx context.Context) {
	if m == nil {
		return
	}
	m.acquirerErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("reason", "indeterminate"),
	))
}

// RecordAcquirerDuration registra la latencia del call al adquirente.
// result es "approved", "declined" o "error".
func (m *Metrics) RecordAcquirerDuration(ctx context.Context, d time.Duration, result string) {
	if m == nil {
		return
	}
	m.acquirerDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("result", result),
	))
}

// RecordSagaDuration registra la latencia total de la saga.
// outcome es "approved" o "rejected".
func (m *Metrics) RecordSagaDuration(ctx context.Context, since time.Time, outcome string) {
	if m == nil {
		return
	}
	m.sagaDuration.Record(ctx, time.Since(since).Seconds(), metric.WithAttributes(
		attribute.String("outcome", outcome),
	))
}

// unresolvedQueryTimeout limita la consulta en cada scrape.
const unresolvedQueryTimeout = 5 * time.Second

// RegisterUnresolvedGauge publica las transacciones sin un resultado conocido, consultando la
// base en cada scrape. Si la consulta falla, la serie no se informa: un cero las ocultaría.
func RegisterUnresolvedGauge(repo repository.TransactionRepository, stuckAfter, reversalsAfter time.Duration) error {
	meter := observability.Meter("posnet.authorization")
	gauge, err := meter.Int64ObservableGauge(
		"posnet_authorization_unresolved_transactions",
		metric.WithDescription("Transacciones sin resolver, por state: INDETERMINATE, PROCESSING trabadas o REVERSAL_PENDING."),
	)
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, unresolvedQueryTimeout)
		defer cancel()
		now := time.Now()
		counts, err := repo.CountUnresolved(ctx, now.Add(-stuckAfter), now.Add(-reversalsAfter))
		if err != nil {
			slog.ErrorContext(ctx, "failed to count unresolved transactions", slog.String("error", err.Error()))
			return nil
		}
		o.ObserveInt64(gauge, counts.Indeterminate, metric.WithAttributes(attribute.String("state", "INDETERMINATE")))
		o.ObserveInt64(gauge, counts.Processing, metric.WithAttributes(attribute.String("state", "PROCESSING")))
		o.ObserveInt64(gauge, counts.ReversalPending, metric.WithAttributes(attribute.String("state", "REVERSAL_PENDING")))
		return nil
	}, gauge)
	return err
}
