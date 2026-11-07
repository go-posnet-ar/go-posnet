package http

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// fakeSessionService implementa port.SessionService.
type fakeSessionService struct {
	createSessionResult *port.SessionCreatedResult
	createSessionErr    error
	createSessionCalls  []port.CreateSessionCommand

	processPaymentErr   error
	processPaymentCalls []port.ProcessPaymentCommand

	requestReversalErr   error
	requestReversalCalls []port.RequestReversalCommand

	cancelSessionErr   error
	cancelSessionCalls []port.CancelSessionCommand

	requestBatchCloseErr   error
	requestBatchCloseCalls []port.RequestBatchCloseCommand
}

func (f *fakeSessionService) CreateSession(_ context.Context, cmd port.CreateSessionCommand) (*port.SessionCreatedResult, error) {
	f.createSessionCalls = append(f.createSessionCalls, cmd)
	return f.createSessionResult, f.createSessionErr
}

func (f *fakeSessionService) ProcessPayment(_ context.Context, cmd port.ProcessPaymentCommand) error {
	f.processPaymentCalls = append(f.processPaymentCalls, cmd)
	return f.processPaymentErr
}

func (f *fakeSessionService) RequestReversal(_ context.Context, cmd port.RequestReversalCommand) error {
	f.requestReversalCalls = append(f.requestReversalCalls, cmd)
	return f.requestReversalErr
}

func (f *fakeSessionService) CancelSession(_ context.Context, cmd port.CancelSessionCommand) error {
	f.cancelSessionCalls = append(f.cancelSessionCalls, cmd)
	return f.cancelSessionErr
}

func (f *fakeSessionService) RequestBatchClose(_ context.Context, cmd port.RequestBatchCloseCommand) error {
	f.requestBatchCloseCalls = append(f.requestBatchCloseCalls, cmd)
	return f.requestBatchCloseErr
}

// ─── fakeSessionRepo ───────────────────────────────────────────────────────────

// fakeSessionRepo implementa repository.PaymentSessionRepository — usada para
// construir un *query.SessionQueryHandler real en los tests de handleGetSession.
type fakeSessionRepo struct {
	findResult *aggregate.PaymentSession
	findErr    error
}

func (f *fakeSessionRepo) Save(context.Context, *aggregate.PaymentSession) error { return nil }
func (f *fakeSessionRepo) SaveTx(context.Context, pgx.Tx, *aggregate.PaymentSession) error {
	return nil
}

func (f *fakeSessionRepo) FindByID(context.Context, domain.TransactionID) (*aggregate.PaymentSession, error) {
	return f.findResult, f.findErr
}

func (f *fakeSessionRepo) FindActiveByTerminal(context.Context, domain.TerminalID) (*aggregate.PaymentSession, error) {
	return nil, nil
}

func (f *fakeSessionRepo) ExpireAwaiting(context.Context, int) ([]repository.ExpiredSession, error) {
	return nil, nil
}

// ─── fakePool ────────────────────────────────────────────────────────────────

// fakePool implementa pgutil.PgxPool. Solo Ping() se ejercita en este
// paquete — ningún handler HTTP del BC Terminal Gateway abre transacciones.
type fakePool struct {
	pingErr error
}

func (f *fakePool) Ping(context.Context) error { return f.pingErr }

func (f *fakePool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	return nil, errors.New("fakePool: BeginTx not implemented")
}
