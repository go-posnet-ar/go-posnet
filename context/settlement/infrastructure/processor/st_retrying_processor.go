package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
)

// Espera entre reintentos: se duplica en cada uno, hasta retryMaxWait.
const (
	retryFirstWait = 500 * time.Millisecond
	retryMaxWait   = 5 * time.Second
)

// RetryingProcessor envuelve un procesador con un plazo por intento y reintentos con espera
// creciente. Implementa SETTLEMENT_SUBMIT_TIMEOUT y SETTLEMENT_SUBMIT_RETRIES.
//
// Reintentar es seguro porque el puerto exige que Submit sea idempotente por batch.ID(): un
// intento que se cortó después de que el procesador lo aceptara devuelve la confirmación anterior.
//
// No reintenta ErrProcessorUnavailable ni ErrSubmissionRejected, que darían lo mismo, ni un
// contexto del llamador cancelado o vencido.
type RetryingProcessor struct {
	next    service.SettlementProcessor
	retries int
	timeout time.Duration
	wait    func(ctx context.Context, d time.Duration) error // reemplazable en tests
}

// NewRetryingProcessor envuelve next. retries < 0 cuenta como 0; timeout <= 0 no pone plazo.
func NewRetryingProcessor(next service.SettlementProcessor, retries int, timeout time.Duration) *RetryingProcessor {
	if retries < 0 {
		retries = 0
	}
	return &RetryingProcessor{next: next, retries: retries, timeout: timeout, wait: sleepCtx}
}

// Submit envía el lote, con hasta retries reintentos si falla.
func (p *RetryingProcessor) Submit(ctx context.Context, batch *aggregate.SettlementBatch) (string, error) {
	wait := retryFirstWait
	var err error
	for attempt := 0; attempt <= p.retries; attempt++ {
		if attempt > 0 {
			slog.WarnContext(ctx, "retrying batch submission to the processor",
				slog.String("batch_id", batch.ID()),
				slog.Int("attempt", attempt+1),
				slog.Int("max_attempts", p.retries+1),
				slog.String("error", err.Error()),
			)
			if werr := p.wait(ctx, wait); werr != nil {
				return "", fmt.Errorf("%w (retry interrupted: %v)", err, werr)
			}
			wait = min(wait*2, retryMaxWait)
		}
		var confirmationID string
		confirmationID, err = p.submitOnce(ctx, batch)
		if err == nil {
			return confirmationID, nil
		}
		if !retryable(ctx, err) {
			return "", err
		}
	}
	return "", fmt.Errorf("submit failed after %d attempts: %w", p.retries+1, err)
}

func (p *RetryingProcessor) submitOnce(ctx context.Context, batch *aggregate.SettlementBatch) (string, error) {
	if p.timeout <= 0 {
		return p.next.Submit(ctx, batch)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return p.next.Submit(attemptCtx, batch)
}

// retryable indica si otro intento puede dar otro resultado.
func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	return !errors.Is(err, service.ErrProcessorUnavailable) && !errors.Is(err, service.ErrSubmissionRejected)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
