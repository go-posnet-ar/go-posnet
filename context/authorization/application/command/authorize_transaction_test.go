package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func TestAuthorizeTransaction_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*port.AuthorizeTransactionCommand)
		wantErr string
	}{
		{"invalid transaction id", func(c *port.AuthorizeTransactionCommand) { c.TransactionID = "not-a-uuid" }, "invalid transaction_id"},
		{"invalid terminal id", func(c *port.AuthorizeTransactionCommand) { c.TerminalID = "not-a-uuid" }, "invalid terminal_id"},
		{"invalid merchant id", func(c *port.AuthorizeTransactionCommand) { c.MerchantID = "not-a-uuid" }, "invalid merchant_id"},
		{"invalid currency", func(c *port.AuthorizeTransactionCommand) { c.Currency = "XXX" }, "invalid currency"},
		{"invalid amount", func(c *port.AuthorizeTransactionCommand) { c.AmountCents = 0 }, "invalid amount"},
		{"invalid stan", func(c *port.AuthorizeTransactionCommand) { c.STAN = 0 }, "invalid stan"},
		{"invalid card network", func(c *port.AuthorizeTransactionCommand) { c.CardNetwork = "BOGUS" }, "invalid card_network"},
		{"invalid pan", func(c *port.AuthorizeTransactionCommand) { c.CardLast4 = "12" }, "invalid pan"},
		{"invalid entry mode", func(c *port.AuthorizeTransactionCommand) { c.EntryMode = "SWIPE" }, "invalid entry_mode"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := validAuthorizeCmd(t)
			tc.mutate(&cmd)

			// Con un transaction_id válido, el rechazo se registra y se encola en una transacción.
			pool := newMockPool(t)
			if tc.name != "invalid transaction id" {
				expectTx(pool)
			}
			h := command.NewAuthorizationHandler(
				&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
				natsutil.NewIdempotencyStore(nil, idempotencySchema),
				pool,
			)

			err := h.AuthorizeTransaction(context.Background(), cmd)
			if err == nil {
				t.Fatalf("AuthorizeTransaction() error = nil, want error containing %q", tc.wantErr)
			}
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("error type = %T, want *pkgerrors.ValidationError", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// txOutcome es lo que hace la transacción del rechazo de una transacción inválida.
type txOutcome int

const (
	noTx       txOutcome = iota // no llega a abrirla
	txCommit                    // registra y encola
	txRollback                  // falla el registro o el encolado
)

func newInvalidDataHandler(t *testing.T, repo *fakeRepo, publisher *fakePublisher, outcome txOutcome) *command.AuthorizationHandler {
	t.Helper()
	pool := newMockPool(t)
	switch outcome {
	case txCommit:
		expectTx(pool)
	case txRollback:
		pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		pool.ExpectRollback()
	}
	return command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)
}

func TestAuthorizeTransaction_InvalidDataPublishesRejection(t *testing.T) {
	cmd := validAuthorizeCmd(t)
	cmd.CardNetwork = "BOGUS"
	repo := &fakeRepo{}
	publisher := &fakePublisher{}

	err := newInvalidDataHandler(t, repo, publisher, txCommit).AuthorizeTransaction(context.Background(), cmd)

	var ve *pkgerrors.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v, want a ValidationError, para que el mensaje se descarte", err)
	}
	if len(publisher.invalidRejections) != 1 {
		t.Fatalf("rechazos publicados = %d, want 1", len(publisher.invalidRejections))
	}
	got := publisher.invalidRejections[0]
	if got.TransactionID.String() != cmd.TransactionID {
		t.Errorf("TransactionID = %q, want %q", got.TransactionID, cmd.TransactionID)
	}
	if got.Rejection.Code() != valueobject.CodeInvalidData || got.Rejection.Source() != valueobject.SourceValidation {
		t.Errorf("Rejection = %v, want VALIDATION(%s)", got.Rejection, valueobject.CodeInvalidData)
	}
	if got.Rejection.IsRetryable() || got.Rejection.RequiresCardCapture() {
		t.Errorf("Rejection = %v: no debe ser reintentable ni ordenar la retención", got.Rejection)
	}
	if got.CardNetwork != "" {
		t.Errorf("CardNetwork = %q, want vacío: no pasó la validación", got.CardNetwork)
	}
	if got.CardLast4 != cmd.CardLast4 || got.AmountCents != cmd.AmountCents || got.Currency != cmd.Currency ||
		got.TerminalID != cmd.TerminalID || got.MerchantID != cmd.MerchantID || got.EntryMode != cmd.EntryMode {
		t.Errorf("rechazo = %+v: debe conservar los campos válidos de %+v", got, cmd)
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0", len(repo.savedTxs))
	}
	if publisher.fraudCheckCalls != 0 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 0", publisher.fraudCheckCalls)
	}
	if len(repo.invalidSaved) != 1 || repo.invalidSaved[0].ValidationError != err.Error() {
		t.Errorf("registros = %+v, want uno con el motivo %q", repo.invalidSaved, err.Error())
	}
}

// Si no se puede registrar, la sesión igual tiene que enterarse del rechazo.
// El registro y el rechazo van en la misma transacción: si falla el registro, no se encola el
// rechazo, y el mensaje se reintenta entero.
func TestAuthorizeTransaction_InvalidDataRecordFailureIsRetried(t *testing.T) {
	cmd := validAuthorizeCmd(t)
	cmd.CardNetwork = "BOGUS"
	publisher := &fakePublisher{}

	err := newInvalidDataHandler(t, &fakeRepo{invalidErr: errors.New("db down")}, publisher, txRollback).
		AuthorizeTransaction(context.Background(), cmd)

	var ve *pkgerrors.ValidationError
	if err == nil || errors.As(err, &ve) {
		t.Errorf("error = %v, want un error que no sea de validación, para reintentar", err)
	}
	if len(publisher.invalidRejections) != 0 {
		t.Errorf("rechazos encolados = %d, want 0", len(publisher.invalidRejections))
	}
}

// card_last4 lo envía el cliente sin validar: un PAN completo no puede salir por el evento
// ni quedar en el error que se loguea.
func TestAuthorizeTransaction_InvalidCardDataIsNotForwarded(t *testing.T) {
	const pan = "4111111111111111"
	tests := []struct {
		name   string
		mutate func(*port.AuthorizeTransactionCommand)
	}{
		{"en card_last4", func(c *port.AuthorizeTransactionCommand) { c.CardLast4 = pan }},
		{"en card_network", func(c *port.AuthorizeTransactionCommand) { c.CardNetwork = pan }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := validAuthorizeCmd(t)
			tc.mutate(&cmd)
			repo := &fakeRepo{}
			publisher := &fakePublisher{}

			err := newInvalidDataHandler(t, repo, publisher, txCommit).AuthorizeTransaction(context.Background(), cmd)
			if err == nil {
				t.Fatal("AuthorizeTransaction() error = nil, want a validation error")
			}
			if strings.Contains(err.Error(), pan) {
				t.Errorf("error = %q: no debe incluir el dato de tarjeta", err.Error())
			}
			if len(publisher.invalidRejections) != 1 || len(repo.invalidSaved) != 1 {
				t.Fatalf("publicados = %d, registrados = %d, want 1 y 1", len(publisher.invalidRejections), len(repo.invalidSaved))
			}
			for _, r := range []valueobject.InvalidTransactionRejection{publisher.invalidRejections[0], repo.invalidSaved[0]} {
				if strings.Contains(fmt.Sprintf("%+v", r), pan) {
					t.Errorf("rechazo = %+v: no debe incluir el dato de tarjeta", r)
				}
			}
		})
	}
}

func TestAuthorizeTransaction_InvalidDataNotPublished(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*port.AuthorizeTransactionCommand)
		repo   *fakeRepo
	}{
		{"sin un transaction_id válido no hay a quién avisar", func(c *port.AuthorizeTransactionCommand) { c.TransactionID = "not-a-uuid" }, &fakeRepo{}},
		{"una transacción existente no se rechaza", func(c *port.AuthorizeTransactionCommand) { c.CardNetwork = "BOGUS" }, &fakeRepo{alreadyExists: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := validAuthorizeCmd(t)
			tc.mutate(&cmd)
			publisher := &fakePublisher{}

			err := newInvalidDataHandler(t, tc.repo, publisher, noTx).AuthorizeTransaction(context.Background(), cmd)

			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("error = %v, want a ValidationError", err)
			}
			if len(publisher.invalidRejections) != 0 {
				t.Errorf("rechazos publicados = %d, want 0", len(publisher.invalidRejections))
			}
		})
	}
}

// Un error transitorio no es de validación: el mensaje se reintenta, en lugar de descartarse
// sin haber avisado del rechazo.
func TestAuthorizeTransaction_InvalidDataTransientErrorsAreRetried(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name      string
		repo      *fakeRepo
		publisher *fakePublisher
		outcome   txOutcome
	}{
		{"falla la consulta", &fakeRepo{existsErr: boom}, &fakePublisher{}, noTx},
		{"falla el encolado", &fakeRepo{}, &fakePublisher{invalidErr: boom}, txRollback},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := validAuthorizeCmd(t)
			cmd.CardNetwork = "BOGUS"

			err := newInvalidDataHandler(t, tc.repo, tc.publisher, tc.outcome).AuthorizeTransaction(context.Background(), cmd)

			var ve *pkgerrors.ValidationError
			if !errors.Is(err, boom) || errors.As(err, &ve) {
				t.Errorf("error = %v, want %v y que no sea de validación", err, boom)
			}
		})
	}
}

func TestPurgeInvalidRejections(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	repo := &fakeRepo{purgeDeleted: 3}

	deleted, err := newInvalidDataHandler(t, repo, &fakePublisher{}, noTx).PurgeInvalidRejections(context.Background(), now, 24*time.Hour)
	if err != nil || deleted != 3 {
		t.Fatalf("PurgeInvalidRejections() = %d, %v, want 3, nil", deleted, err)
	}
	if want := now.Add(-24 * time.Hour); !repo.purgeCutoff.Equal(want) {
		t.Errorf("cutoff = %v, want %v", repo.purgeCutoff, want)
	}
}

func TestPurgeInvalidRejections_Error(t *testing.T) {
	boom := errors.New("boom")
	h := newInvalidDataHandler(t, &fakeRepo{purgeErr: boom}, &fakePublisher{}, noTx)

	if _, err := h.PurgeInvalidRejections(context.Background(), time.Now(), time.Hour); !errors.Is(err, boom) {
		t.Errorf("error = %v, want %v", err, boom)
	}
}

func TestAuthorizeTransaction_Success(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t)); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}

	if len(repo.savedTxs) != 1 {
		t.Fatalf("saved txs = %d, want 1", len(repo.savedTxs))
	}
	if repo.savedTxs[0].State() != valueobject.StateFraudChecking {
		t.Errorf("saved tx state = %v, want %v", repo.savedTxs[0].State(), valueobject.StateFraudChecking)
	}
	if publisher.fraudCheckCalls != 1 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 1", publisher.fraudCheckCalls)
	}
}

func TestAuthorizeTransaction_DuplicateEventIsSkipped(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 0) // 0 filas afectadas → event_id ya existía

	repo := &fakeRepo{}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t)); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}
	if len(repo.savedTxs) != 0 {
		t.Errorf("saved txs = %d, want 0 (duplicate must not be persisted)", len(repo.savedTxs))
	}
	if publisher.fraudCheckCalls != 0 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 0", publisher.fraudCheckCalls)
	}
}

// Un alta repetida con otro event_id no reinicia la transacción ni pide otro fraud check: si
// no, el score nuevo volvería a llamar al adquirente. El evento queda marcado.
func TestAuthorizeTransaction_ExistingTransactionIsNotRestarted(t *testing.T) {
	pool := newMockPool(t)
	expectClaimed(pool, 1)

	repo := &fakeRepo{alreadyExists: true}
	publisher := &fakePublisher{}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	if err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t)); err != nil {
		t.Fatalf("AuthorizeTransaction() error = %v", err)
	}
	if publisher.fraudCheckCalls != 0 {
		t.Errorf("PublishFraudCheckRequested calls = %d, want 0", publisher.fraudCheckCalls)
	}
}

func TestAuthorizeTransaction_SaveErrorRollsBack(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback() // fn devuelve error → no hay Commit, solo el Rollback diferido

	repo := &fakeRepo{saveErr: errors.New("db unreachable")}
	h := command.NewAuthorizationHandler(
		repo, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t))
	if err == nil {
		t.Fatal("AuthorizeTransaction() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "save transaction") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "save transaction")
	}
}

// El pedido de fraude se encola en la transacción del alta: si falla, no queda ni la transacción
// ni el evento marcado, y la reentrega lo reintenta. Antes quedaba trabada en FRAUD_CHECKING.
func TestAuthorizeTransaction_FraudCheckEnqueueErrorRollsBack(t *testing.T) {
	pool := newMockPool(t)
	expectClaimedThenRollback(pool)

	publisher := &fakePublisher{fraudCheckErr: errors.New("outbox insert failed")}
	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, publisher,
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t))
	if err == nil || !strings.Contains(err.Error(), "enqueue fraud check") {
		t.Fatalf("AuthorizeTransaction() error = %v, want el error del encolado, para reintentar", err)
	}
}

func TestAuthorizeTransaction_BeginTxError(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted}).WillReturnError(errors.New("connection refused"))

	h := command.NewAuthorizationHandler(
		&fakeRepo{}, &fakeAcquirer{}, &fakePublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool,
	)

	err := h.AuthorizeTransaction(context.Background(), validAuthorizeCmd(t))
	if err == nil {
		t.Fatal("AuthorizeTransaction() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "begin transaction") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "begin transaction")
	}
}
