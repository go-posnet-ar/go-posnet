package breaker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
)

var errNoResponse = errors.New("acquirer: timeout waiting for the response")

// fakeAcquirer responde con err, o con una respuesta si err es nil, y cuenta las llamadas.
type fakeAcquirer struct {
	mu    sync.Mutex
	err   error
	calls int
	// block, si no es nil, demora la llamada hasta que se cierre.
	block chan struct{}
}

func (f *fakeAcquirer) Authorize(context.Context, *aggregate.Transaction) (service.AcquirerResponse, error) {
	return service.AcquirerResponse{ResponseCode: "00", AuthCode: "AB1234"}, f.call()
}

func (f *fakeAcquirer) Reverse(context.Context, *aggregate.Transaction) error { return f.call() }

func (f *fakeAcquirer) call() error {
	f.mu.Lock()
	f.calls++
	err, block := f.err, f.block
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	return err
}

func (f *fakeAcquirer) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeAcquirer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newBreaker devuelve un breaker de 3 fallas y 30 s de cooldown, con un reloj que avanza a mano.
func newBreaker(t *testing.T, next service.AcquirerGateway) (*Gateway, *time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	g := New(next, 3, 30*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	g.now = func() time.Time { return now }
	return g, &now
}

func authorize(g *Gateway) error {
	_, err := g.Authorize(context.Background(), nil)
	return err
}

func TestBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	acq := &fakeAcquirer{err: errNoResponse}
	g, _ := newBreaker(t, acq)

	for i := range 3 {
		if err := authorize(g); !errors.Is(err, errNoResponse) {
			t.Fatalf("call %d: error = %v, want the acquirer's error", i+1, err)
		}
	}
	if !g.Open() {
		t.Fatal("Open() = false after 3 failures in a row, want true")
	}

	// Abierto: rechaza sin llamar al host, con un error que Authorization informa como 91.
	err := authorize(g)
	if !errors.Is(err, ErrOpen) || !errors.Is(err, service.ErrNotSent) {
		t.Fatalf("error = %v, want ErrOpen wrapping service.ErrNotSent", err)
	}
	if err := g.Reverse(context.Background(), nil); !errors.Is(err, ErrOpen) {
		t.Fatalf("Reverse() error = %v, want ErrOpen", err)
	}
	if acq.count() != 3 {
		t.Errorf("acquirer calls = %d, want 3: an open circuit must not call the host", acq.count())
	}
}

func TestBreaker_ResponsesResetTheCount(t *testing.T) {
	acq := &fakeAcquirer{err: errNoResponse}
	g, _ := newBreaker(t, acq)

	_ = authorize(g)
	_ = authorize(g)
	acq.set(nil) // Un rechazo del emisor también es una respuesta: no hay error.
	_ = authorize(g)
	acq.set(errNoResponse)
	_ = authorize(g)
	_ = authorize(g)

	if g.Open() {
		t.Fatal("Open() = true, want false: the failures were not consecutive")
	}
}

// Un ErrNotSent del host, como una conexión caída o una tarjeta desconocida, ya se rechaza al
// instante y no dice si el host responde: no cuenta, ni para abrir ni para reiniciar la cuenta.
func TestBreaker_NotSentDoesNotCount(t *testing.T) {
	acq := &fakeAcquirer{err: errNoResponse}
	g, _ := newBreaker(t, acq)

	_ = authorize(g)
	_ = authorize(g)
	acq.set(fmt.Errorf("%w: no connection", service.ErrNotSent))
	for range 5 {
		_ = authorize(g)
	}
	if g.Open() {
		t.Fatal("Open() = true after ErrNotSent, want false")
	}
	acq.set(errNoResponse)
	_ = authorize(g)
	if !g.Open() {
		t.Fatal("Open() = false, want true: ErrNotSent must not reset the count")
	}
}

func TestBreaker_ProbeAfterCooldown(t *testing.T) {
	tests := []struct {
		name     string
		probeErr error
		wantOpen bool
	}{
		{"the host answers: closes", nil, false},
		{"no response: opens again", errNoResponse, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			acq := &fakeAcquirer{err: errNoResponse}
			g, now := newBreaker(t, acq)
			for range 3 {
				_ = authorize(g)
			}

			*now = now.Add(29 * time.Second)
			if err := authorize(g); !errors.Is(err, ErrOpen) {
				t.Fatalf("before the cooldown: error = %v, want ErrOpen", err)
			}

			*now = now.Add(time.Second)
			acq.set(tc.probeErr)
			if err := authorize(g); !errors.Is(err, tc.probeErr) {
				t.Fatalf("probe: error = %v, want %v", err, tc.probeErr)
			}
			if g.Open() != tc.wantOpen {
				t.Fatalf("Open() = %v, want %v", g.Open(), tc.wantOpen)
			}
			if acq.count() != 4 {
				t.Errorf("acquirer calls = %d, want 4", acq.count())
			}
			if tc.wantOpen {
				// Otro cooldown completo desde la prueba fallida.
				*now = now.Add(29 * time.Second)
				if err := authorize(g); !errors.Is(err, ErrOpen) {
					t.Errorf("after the failed probe: error = %v, want ErrOpen", err)
				}
			}
		})
	}
}

// Mientras la prueba está en curso, las demás llamadas se rechazan: un host colgado recibe una
// sola llamada por cooldown.
func TestBreaker_SingleProbeAtATime(t *testing.T) {
	acq := &fakeAcquirer{err: errNoResponse}
	g, now := newBreaker(t, acq)
	for range 3 {
		_ = authorize(g)
	}
	*now = now.Add(30 * time.Second)

	acq.mu.Lock()
	acq.block = make(chan struct{})
	acq.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- authorize(g) }()
	for acq.count() < 4 {
		time.Sleep(time.Millisecond)
	}

	if err := authorize(g); !errors.Is(err, ErrOpen) {
		t.Fatalf("during the probe: error = %v, want ErrOpen", err)
	}
	close(acq.block)
	if err := <-done; !errors.Is(err, errNoResponse) {
		t.Fatalf("probe: error = %v, want the acquirer's error", err)
	}
	if acq.count() != 4 {
		t.Errorf("acquirer calls = %d, want 4", acq.count())
	}
}

// Una prueba que no llega al host no demuestra nada: la próxima llamada vuelve a probar.
func TestBreaker_NotSentProbeRetriesOnTheNextCall(t *testing.T) {
	acq := &fakeAcquirer{err: errNoResponse}
	g, now := newBreaker(t, acq)
	for range 3 {
		_ = authorize(g)
	}
	*now = now.Add(30 * time.Second)

	acq.set(fmt.Errorf("%w: no connection", service.ErrNotSent))
	_ = authorize(g)
	acq.set(nil)
	if err := authorize(g); err != nil {
		t.Fatalf("second probe: error = %v, want nil", err)
	}
	if g.Open() {
		t.Error("Open() = true, want false after a probe that got a response")
	}
}

func TestNew_RequiresAThreshold(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New(threshold=0) did not panic")
		}
	}()
	New(&fakeAcquirer{}, 0, time.Second, nil)
}
