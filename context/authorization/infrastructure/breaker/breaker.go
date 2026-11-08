// Package breaker corta las llamadas al adquirente cuando deja de responder.
//
// Con el host conectado pero sin responder, cada llamada espera su timeout entero: ocupa un
// worker, demora a las demás ventas y termina en INDETERMINATE, con una reversa. Después de
// varias llamadas seguidas sin respuesta, el breaker se abre y las rechaza al instante con
// service.ErrNotSent, que Authorization informa como 91: no salieron, así que no hay nada que
// revertir.
package breaker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/pkg/observability"
	"go.opentelemetry.io/otel/metric"
)

// ErrOpen indica que el breaker rechazó la llamada sin enviarla. Envuelve service.ErrNotSent.
var ErrOpen = fmt.Errorf("%w: acquirer circuit open", service.ErrNotSent)

type state int

const (
	closed   state = iota // Las llamadas pasan
	open                  // Se rechazan sin enviarlas, hasta que pase el cooldown
	halfOpen              // Pasa una sola llamada de prueba
)

func (s state) String() string {
	switch s {
	case open:
		return "open"
	case halfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// Gateway envuelve un service.AcquirerGateway con un circuit breaker.
//
//   - Cerrado: las llamadas pasan. Una respuesta del host, aprobada o rechazada, reinicia la
//     cuenta. Un error ambiguo, sin ErrNotSent, suma uno: con threshold seguidos, se abre.
//   - Abierto: rechaza todo con ErrOpen durante cooldown.
//   - Medio abierto: deja pasar una llamada de prueba, y rechaza las demás mientras tanto. Si el
//     host responde, se cierra; si no, se abre otro cooldown.
//
// Un ErrNotSent del host no cuenta: no dice nada de si responde, y ya se rechaza al instante.
// Las reversas pasan por el mismo breaker: con el host colgado, el conciliador tampoco espera.
type Gateway struct {
	next      service.AcquirerGateway
	threshold int
	cooldown  time.Duration
	now       func() time.Time
	log       *slog.Logger

	mu       sync.Mutex
	state    state
	failures int
	openedAt time.Time
	probing  bool
}

var _ service.AcquirerGateway = (*Gateway)(nil)

// New envuelve next. Se abre con threshold errores ambiguos seguidos, y prueba de nuevo después
// de cooldown. threshold debe ser al menos 1.
func New(next service.AcquirerGateway, threshold int, cooldown time.Duration, log *slog.Logger) *Gateway {
	if threshold < 1 {
		panic("breaker: threshold must be at least 1")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Gateway{next: next, threshold: threshold, cooldown: cooldown, now: time.Now, log: log}
}

// Open informa si el breaker está rechazando llamadas: abierto, o medio abierto con la prueba
// en curso.
func (g *Gateway) Open() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.state != closed
}

// RegisterOpenGauge publica el estado del breaker: 1 mientras rechaza llamadas, 0 cerrado.
func (g *Gateway) RegisterOpenGauge() error {
	meter := observability.Meter("posnet.authorization")
	gauge, err := meter.Int64ObservableGauge("posnet_acquirer_circuit_open",
		metric.WithDescription("Circuit breaker del adquirente: 1 abierto, rechazando con 91 sin llamar al host; 0 cerrado."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var value int64
		if g.Open() {
			value = 1
		}
		o.ObserveInt64(gauge, value)
		return nil
	}, gauge)
	return err
}

func (g *Gateway) Authorize(ctx context.Context, tx *aggregate.Transaction) (service.AcquirerResponse, error) {
	probe, err := g.acquire()
	if err != nil {
		return service.AcquirerResponse{}, err
	}
	resp, err := g.next.Authorize(ctx, tx)
	g.record(probe, err)
	return resp, err
}

func (g *Gateway) Reverse(ctx context.Context, tx *aggregate.Transaction) error {
	probe, err := g.acquire()
	if err != nil {
		return err
	}
	err = g.next.Reverse(ctx, tx)
	g.record(probe, err)
	return err
}

// acquire decide si la llamada pasa. probe indica que es la llamada de prueba.
func (g *Gateway) acquire() (probe bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch g.state {
	case open:
		if g.now().Sub(g.openedAt) < g.cooldown {
			return false, ErrOpen
		}
		g.state = halfOpen
		g.probing = true
		g.log.Info("acquirer circuit half-open — sending a probe")
		return true, nil
	case halfOpen:
		// La prueba sigue en curso: el resto espera su resultado.
		return false, ErrOpen
	default:
		return false, nil
	}
}

// record registra el resultado de una llamada que pasó.
func (g *Gateway) record(probe bool, err error) {
	failed := err != nil && !errors.Is(err, service.ErrNotSent)
	notSent := err != nil && !failed

	g.mu.Lock()
	defer g.mu.Unlock()
	if probe {
		g.probing = false
		switch {
		case failed:
			g.trip("the probe got no response")
		case notSent:
			// No llegó al host: no prueba nada. La próxima llamada vuelve a probar.
			g.state = open
			g.openedAt = g.now().Add(-g.cooldown)
		default:
			g.state = closed
			g.failures = 0
			g.log.Info("acquirer circuit closed — the host is responding again")
		}
		return
	}
	if g.state != closed {
		// Una llamada que pasó antes de abrirse: su resultado ya no cambia nada.
		return
	}
	switch {
	case failed:
		g.failures++
		if g.failures >= g.threshold {
			g.trip(fmt.Sprintf("%d calls in a row got no response", g.failures))
		}
	case !notSent:
		g.failures = 0
	}
}

func (g *Gateway) trip(reason string) {
	g.state = open
	g.openedAt = g.now()
	g.failures = 0
	g.log.Warn("acquirer circuit opened — rejecting with 91 without calling the host",
		slog.String("reason", reason), slog.Duration("cooldown", g.cooldown))
}
