package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
)

var _ service.SettlementProcessor = (*RetryingProcessor)(nil)

// scriptedProcessor devuelve los errores en orden y después acepta.
type scriptedProcessor struct {
	errs      []error
	calls     int
	deadlines []bool // Si cada intento tenía plazo
	block     bool   // Espera a que venza el plazo del intento
}

func (s *scriptedProcessor) Submit(ctx context.Context, b *aggregate.SettlementBatch) (string, error) {
	s.calls++
	_, ok := ctx.Deadline()
	s.deadlines = append(s.deadlines, ok)
	if s.block {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if s.calls <= len(s.errs) {
		return "", s.errs[s.calls-1]
	}
	return "conf-" + b.ID(), nil
}

func newBatch(t *testing.T) *aggregate.SettlementBatch {
	t.Helper()
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}
	return b
}

func newRetrying(next service.SettlementProcessor, retries int, timeout time.Duration) (*RetryingProcessor, *[]time.Duration) {
	p := NewRetryingProcessor(next, retries, timeout)
	var waits []time.Duration
	p.wait = func(ctx context.Context, d time.Duration) error {
		waits = append(waits, d)
		return ctx.Err()
	}
	return p, &waits
}

func TestRetryingProcessor_RetriesUntilAccepted(t *testing.T) {
	next := &scriptedProcessor{errs: []error{errors.New("503"), errors.New("reset")}}
	p, waits := newRetrying(next, 3, time.Second)
	b := newBatch(t)

	got, err := p.Submit(context.Background(), b)
	if err != nil || got != "conf-"+b.ID() {
		t.Fatalf("Submit() = %q, %v, want the confirmation", got, err)
	}
	if next.calls != 3 {
		t.Errorf("calls = %d, want 3", next.calls)
	}
	if want := []time.Duration{500 * time.Millisecond, time.Second}; fmt.Sprint(*waits) != fmt.Sprint(want) {
		t.Errorf("waits = %v, want %v", *waits, want)
	}
}

func TestRetryingProcessor_GivesUpAfterTheRetries(t *testing.T) {
	failure := errors.New("503")
	next := &scriptedProcessor{errs: []error{failure, failure, failure, failure, failure}}
	p, _ := newRetrying(next, 2, time.Second)

	_, err := p.Submit(context.Background(), newBatch(t))
	if !errors.Is(err, failure) {
		t.Fatalf("Submit() error = %v, want the last failure", err)
	}
	if next.calls != 3 {
		t.Errorf("calls = %d, want 3: the first attempt and 2 retries", next.calls)
	}
}

func TestRetryingProcessor_DoesNotRetryPermanentErrors(t *testing.T) {
	for _, permanent := range []error{
		service.ErrProcessorUnavailable,
		fmt.Errorf("invalid remittance file: %w", service.ErrSubmissionRejected),
	} {
		next := &scriptedProcessor{errs: []error{permanent}}
		p, _ := newRetrying(next, 3, time.Second)
		if _, err := p.Submit(context.Background(), newBatch(t)); !errors.Is(err, permanent) || next.calls != 1 {
			t.Errorf("Submit() = %v after %d calls, want %v after 1", err, next.calls, permanent)
		}
	}
}

// Cada intento tiene su plazo: uno que se cuelga no consume los siguientes.
func TestRetryingProcessor_TimeoutPerAttempt(t *testing.T) {
	next := &scriptedProcessor{block: true}
	p, _ := newRetrying(next, 1, 10*time.Millisecond)

	_, err := p.Submit(context.Background(), newBatch(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Submit() error = %v, want DeadlineExceeded", err)
	}
	if next.calls != 2 || !next.deadlines[0] || !next.deadlines[1] {
		t.Errorf("calls = %d, deadlines = %v, want 2 attempts with a deadline each", next.calls, next.deadlines)
	}
}

func TestRetryingProcessor_NoTimeout(t *testing.T) {
	next := &scriptedProcessor{}
	p, _ := newRetrying(next, 0, 0)
	if _, err := p.Submit(context.Background(), newBatch(t)); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if next.deadlines[0] {
		t.Error("attempt had a deadline, want none with timeout 0")
	}
}

// Si el llamador cancela, no se sigue reintentando.
func TestRetryingProcessor_StopsWhenTheCallerCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	next := &scriptedProcessor{errs: []error{errors.New("503"), errors.New("503")}}
	p := NewRetryingProcessor(next, 5, time.Second)
	p.wait = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}

	if _, err := p.Submit(ctx, newBatch(t)); err == nil {
		t.Fatal("Submit() error = nil, want the failure")
	}
	if next.calls != 1 {
		t.Errorf("calls = %d, want 1", next.calls)
	}
}

func TestRetryingProcessor_NegativeRetriesIsOneAttempt(t *testing.T) {
	next := &scriptedProcessor{errs: []error{errors.New("503")}}
	p, _ := newRetrying(next, -1, time.Second)
	if _, err := p.Submit(context.Background(), newBatch(t)); err == nil || next.calls != 1 {
		t.Errorf("Submit() = %v after %d calls, want an error after 1", err, next.calls)
	}
}
