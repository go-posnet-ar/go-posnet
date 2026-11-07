package middleware

import (
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/pkg/observability"
)

// latencyBuckets van de 5 ms a 30 s, el timeout hacia los servicios.
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// Metrics cuenta los pedidos por ruta y código. La ruta es el patrón (por ejemplo
// "POST /api/sessions/{id}/pay"), no el path: así los IDs no multiplican las series.
type Metrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
}

// NewMetrics crea los instrumentos: posnet_gateway_requests_total y
// posnet_gateway_request_duration_seconds.
func NewMetrics() (*Metrics, error) {
	m := observability.Meter("posnet.gateway")
	requests, err := m.Int64Counter("posnet_gateway_requests",
		metric.WithDescription("Pedidos al gateway, por ruta y código de respuesta."))
	if err != nil {
		return nil, err
	}
	duration, err := m.Float64Histogram("posnet_gateway_request_duration",
		metric.WithDescription("Latencia de los pedidos al gateway, incluido el servicio."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(latencyBuckets...))
	if err != nil {
		return nil, err
	}
	return &Metrics{requests: requests, duration: duration}, nil
}

// Route registra los pedidos de route. Sin Metrics, no registra nada.
func (m *Metrics) Route(route string, next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)
		attrs := metric.WithAttributes(
			attribute.String("route", route),
			attribute.String("code", strconv.Itoa(rec.status)),
		)
		m.requests.Add(r.Context(), 1, attrs)
		m.duration.Record(r.Context(), time.Since(start).Seconds(), attrs)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status, s.wroteHeader = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap deja que http.ResponseController llegue al ResponseWriter original, como necesita el
// reverse proxy para hacer flush.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
