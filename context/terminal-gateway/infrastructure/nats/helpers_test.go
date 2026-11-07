package nats

import (
	"context"

	"github.com/jackc/pgx/v5"
	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
)

const idempotencySchema = "terminal_gateway"

// ─── fakeJetStream ───────────────────────────────────────────────────────────

// fakeJetStream implementa natsclient.JetStreamContext embebiendo la interfaz
// (nil) y sobreescribiendo solo PublishMsg y QueueSubscribe.
type fakeJetStream struct {
	natsclient.JetStreamContext

	publishErr error
	published  []*natsclient.Msg
	ackSeq     uint64

	subscribeErr   error
	subscribeCalls []queueSubscribeCall
}

type queueSubscribeCall struct {
	subject string
	durable string
}

func (f *fakeJetStream) PublishMsg(m *natsclient.Msg, _ ...natsclient.PubOpt) (*natsclient.PubAck, error) {
	f.published = append(f.published, m)
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	return &natsclient.PubAck{Sequence: f.ackSeq}, nil
}

func (f *fakeJetStream) QueueSubscribe(subj, queue string, _ natsclient.MsgHandler, _ ...natsclient.SubOpt) (*natsclient.Subscription, error) {
	f.subscribeCalls = append(f.subscribeCalls, queueSubscribeCall{subject: subj, durable: queue})
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	return nil, nil
}

// ─── fakeSessionRepo (para construir un *command.SessionHandler real) ─────────

type fakeSessionRepo struct {
	saveErr       error
	saveCallCount int
	savedSessions []*aggregate.PaymentSession

	findResult *aggregate.PaymentSession
	findErr    error
}

func (f *fakeSessionRepo) Save(_ context.Context, s *aggregate.PaymentSession) error {
	f.saveCallCount++
	f.savedSessions = append(f.savedSessions, s)
	return f.saveErr
}

func (f *fakeSessionRepo) SaveTx(_ context.Context, _ pgx.Tx, s *aggregate.PaymentSession) error {
	f.saveCallCount++
	f.savedSessions = append(f.savedSessions, s)
	return f.saveErr
}

func (f *fakeSessionRepo) lastSaved() *aggregate.PaymentSession {
	if len(f.savedSessions) == 0 {
		return nil
	}
	return f.savedSessions[len(f.savedSessions)-1]
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

// ─── fakeNotifier ──────────────────────────────────────────────────────────────

type fakeNotifier struct {
	notifyResultErr   error
	notifyResultCalls int
}

func (f *fakeNotifier) NotifyResult(context.Context, *aggregate.PaymentSession) error {
	f.notifyResultCalls++
	return f.notifyResultErr
}

func (f *fakeNotifier) NotifySessionCreated(context.Context, *aggregate.PaymentSession) error {
	return nil
}

func (f *fakeNotifier) NotifySessionExpired(context.Context, domain.TerminalID, domain.TransactionID) error {
	return nil
}
