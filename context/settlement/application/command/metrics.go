package command

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
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
//	posnet_settlement_batches_attention → posnet_settlement_batches_attention{reason}
//	posnet_settlement_outbox_stuck → posnet_settlement_outbox_stuck
//	posnet_settlement_reversals_in_review → posnet_settlement_reversals_in_review
//	posnet_settlement_batch_close_rejected → posnet_settlement_batch_close_rejected_total{reason}
//	posnet_settlement_reversal_recovery_pending → posnet_settlement_reversal_recovery_pending
//
// El label "bc" lo inyecta Prometheus como target label (prometheus.yml).
type Metrics struct {
	authApproved  metric.Int64Counter
	reversals     metric.Int64Counter
	batches       metric.Int64Counter
	batchDuration metric.Float64Histogram
	submissions   metric.Int64Counter
	closeRejected metric.Int64Counter
}

// Valores del label reason de posnet_settlement_batch_close_rejected_total.
const (
	closeRejectedFuture      = "future"       // batch_date posterior al día de negocio
	closeRejectedTooOld      = "too_old"      // más antiguo que SETTLEMENT_CLOSE_MAX_AGE_DAYS
	closeRejectedNotReopened = "not_reopened" // anterior a ayer, de un lote que nadie reabrió
)

// Valores del label result de posnet_settlement_submissions_total.
const (
	submissionSubmitted   = "submitted"
	submissionUnavailable = "unavailable"
	submissionError       = "error"
)

const (
	// closedUnsentAfter es cuánto puede seguir un lote en CLOSED antes de considerarlo
	// trabado: da margen a un operador que está resolviendo una disputa o reenviando.
	closedUnsentAfter = time.Hour
	// outboxStuckAfter es cuánto puede esperar un evento en el outbox: el Relay lo
	// publica en segundos, salvo que NATS no esté disponible.
	outboxStuckAfter = time.Minute
	// reversalReviewAfter da tiempo a que llegue la compra de una reversa adelantada: un
	// evento puede tardar unos 12 minutos en reintentos (ver retryDelays en el subscriber).
	reversalReviewAfter = 15 * time.Minute
	// attentionQueryTimeout acota la consulta para no demorar el scrape de /metrics.
	attentionQueryTimeout = 2 * time.Second
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

	closeRejected, err := m.Int64Counter("posnet_settlement_batch_close_rejected",
		metric.WithDescription("Pedidos de cierre de un terminal rechazados por su batch_date, por reason. El terminal no recibe respuesta."))
	if err != nil {
		return nil, err
	}

	return &Metrics{
		authApproved:  authApproved,
		reversals:     reversals,
		batches:       batches,
		batchDuration: batchDuration,
		submissions:   submissions,
		closeRejected: closeRejected,
	}, nil
}

// RecordSubmission contabiliza un intento de envío al procesador. Nil-safe.
func (m *Metrics) RecordSubmission(ctx context.Context, result string) {
	if m == nil {
		return
	}
	m.submissions.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

// RecordCloseRejected cuenta un pedido de cierre rechazado por su batch_date. Nil-safe.
func (m *Metrics) RecordCloseRejected(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.closeRejected.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
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

// RegisterBatchAttentionGauge publica los lotes que requieren atención, consultando la
// base en cada scrape. Loc es la zona de los comercios sin una propia, igual que en el cierre
// automático.
// Si la consulta falla, la serie no se informa en ese scrape: un cero ocultaría los lotes.
//
// closeGrace es el plazo del cierre automático: un lote OPEN cuenta como vencido recién
// cuando el job ya debería haberlo cerrado.
func RegisterBatchAttentionGauge(counter repository.BatchAttentionCounter, loc *time.Location, closeGrace time.Duration) error {
	gauge, err := observability.Meter("posnet.settlement").Int64ObservableGauge(
		"posnet_settlement_batches_attention",
		metric.WithDescription("Lotes que requieren atención, por reason: disputed, closed_unsent u open_overdue."),
	)
	if err != nil {
		return err
	}
	outboxGauge, err := observability.Meter("posnet.settlement").Int64ObservableGauge(
		"posnet_settlement_outbox_stuck",
		metric.WithDescription("Eventos que esperan en el outbox desde hace más de un minuto."),
	)
	if err != nil {
		return err
	}
	reviewGauge, err := observability.Meter("posnet.settlement").Int64ObservableGauge(
		"posnet_settlement_reversals_in_review",
		metric.WithDescription("Reversas que no coinciden con su compra, en revisión hace más de 15 minutos."),
	)
	if err != nil {
		return err
	}
	recoveryGauge, err := observability.Meter("posnet.settlement").Int64ObservableGauge(
		"posnet_settlement_reversal_recovery_pending",
		metric.WithDescription("1 mientras no se haya registrado la recuperación de reversas anteriores con settlement-recover-reversals."),
	)
	if err != nil {
		return err
	}
	_, err = observability.Meter("posnet.settlement").RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			ctx, cancel := context.WithTimeout(ctx, attentionQueryTimeout)
			defer cancel()

			now := time.Now()
			counts, err := counter.CountBatchesNeedingAttention(ctx, now.Add(-closeGrace), loc.String(), now.Add(-closedUnsentAfter), now.Add(-outboxStuckAfter), now.Add(-reversalReviewAfter))
			if err != nil {
				slog.ErrorContext(ctx, "failed to count batches needing attention", slog.String("error", err.Error()))
				return nil
			}
			observe := func(reason string, value int64) {
				o.ObserveInt64(gauge, value, metric.WithAttributes(attribute.String("reason", reason)))
			}
			observe("disputed", counts.Disputed)
			observe("closed_unsent", counts.ClosedUnsent)
			observe("open_overdue", counts.OpenOverdue)
			o.ObserveInt64(outboxGauge, counts.OutboxStuck)
			o.ObserveInt64(reviewGauge, counts.ReversalsInReview)
			var pending int64
			if counts.RecoveryPending {
				pending = 1
			}
			o.ObserveInt64(recoveryGauge, pending)
			return nil
		},
		gauge, outboxGauge, reviewGauge, recoveryGauge,
	)
	return err
}
