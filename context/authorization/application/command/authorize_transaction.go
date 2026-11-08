// Package command contiene los command handlers del BC Authorization.
// Implementan los puertos de entrada definidos en application/port.
package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/repository"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

/*const (
	fraudCheckTimeout = 500 * time.Millisecond // Timeout para esperar el score de fraude
)*/

// AuthorizationHandler implementa port.AuthorizationService.
// Orquesta la Saga de autorización completa.
type AuthorizationHandler struct {
	repo         repository.TransactionRepository
	acquirer     service.AcquirerGateway
	publisher    service.EventPublisher
	idempotency  *natsutil.IdempotencyStore
	pool         pgutil.PgxPool
	metrics      *Metrics                         // opcional; nil = sin instrumentación (tests)
	blockedCards repository.BlockedCardRepository // opcional; nil = blocklist deshabilitada

	// Llamadas al adquirente fuera del handler de NATS. Sin slots, corren en el handler (tests).
	acquirerTimeout time.Duration
	acquirerSlots   chan struct{}
	acquirerCalls   sync.WaitGroup
}

// duplicateTransactionMsg se registra cuando llega un alta de un transaction_id que ya existe,
// con otro event_id. Se ignora: el resultado sigue siendo el de la transacción original.
const duplicateTransactionMsg = "transaction already exists — ignoring the duplicate"

// defaultAcquirerTimeout limita las llamadas si no se configura otro valor.
const defaultAcquirerTimeout = 30 * time.Second

// ConfigureAcquirerCalls hace que las llamadas al adquirente no ocupen el handler de NATS, con
// el límite de simultáneas y el timeout de cada una. Un adquirente lento frena solo sus
// llamadas: el mensaje se confirma enseguida, porque la transacción ya está guardada y, si el
// proceso cae, la retoma el conciliador.
func (h *AuthorizationHandler) ConfigureAcquirerCalls(maxConcurrent int, timeout time.Duration) {
	h.acquirerSlots = make(chan struct{}, maxConcurrent)
	h.acquirerTimeout = timeout
}

// WaitAcquirerCalls espera las llamadas en curso, hasta que venza ctx. Lo que no termine a
// tiempo queda para el conciliador.
func (h *AuthorizationHandler) WaitAcquirerCalls(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		h.acquirerCalls.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// runAcquirerCall ejecuta fn en un worker. Si están todos ocupados, espera: esa demora frena
// el consumo de eventos, en lugar de acumular llamadas sin límite.
func (h *AuthorizationHandler) runAcquirerCall(ctx context.Context, log *slog.Logger, fn func(context.Context) error) error {
	if h.acquirerSlots == nil {
		return fn(ctx)
	}
	h.acquirerSlots <- struct{}{}
	h.acquirerCalls.Add(1)
	go func() {
		defer h.acquirerCalls.Done()
		defer func() { <-h.acquirerSlots }()
		// El handler ya terminó: conserva la traza, pero no su cancelación.
		if err := fn(context.WithoutCancel(ctx)); err != nil {
			observability.RecordError(ctx, err)
			log.Error("acquirer call failed — the reconciler will retry", slog.String("error", err.Error()))
		}
	}()
	return nil
}

// withAcquirerTimeout limita una llamada al adquirente.
func (h *AuthorizationHandler) withAcquirerTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := h.acquirerTimeout
	if timeout <= 0 {
		timeout = defaultAcquirerTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

// SetBlockedCards inyecta la blocklist de tarjetas. Se llama desde el wiring
// (cmd/authorization). Si no se inyecta, la blocklist queda deshabilitada y
// el wiring lo advierte: sigue el mismo patrón que SetMetrics para no romper
// las firmas que construyen el handler en los tests.
func (h *AuthorizationHandler) SetBlockedCards(r repository.BlockedCardRepository) {
	h.blockedCards = r
}

// SetMetrics inyecta los instrumentos de negocio. Se llama desde el wiring
// (cmd/authorization) tras InitMeter. Mantener la firma de NewAuthorizationHandler
// intacta evita tocar los tests que construyen el handler.
func (h *AuthorizationHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

// NewAuthorizationHandler construye el handler con todas sus dependencias.
func NewAuthorizationHandler(
	repo repository.TransactionRepository,
	acquirer service.AcquirerGateway,
	publisher service.EventPublisher,
	idempotency *natsutil.IdempotencyStore,
	pool pgutil.PgxPool,
) *AuthorizationHandler {
	return &AuthorizationHandler{
		repo:        repo,
		acquirer:    acquirer,
		publisher:   publisher,
		idempotency: idempotency,
		pool:        pool,
	}
}

// AuthorizeTransaction es el punto de entrada de la Saga de autorización.
// Implementa port.AuthorizationService.
//
// Pasos:
//  1. Parsear y validar el comando. Si es inválido, avisar del rechazo y descartarlo
//  2. Crear el aggregate Transaction y consultar la blocklist
//  3. Marcar el evento y dar de alta la transacción, en una sola transacción de base. Un
//     event_id ya procesado o un transaction_id que ya existe no continúan
//  4. Solicitar fraud check → publicar FraudCheckRequested
//  5. El score llega vía ApplyFraudScore (evento NATS separado)
func (h *AuthorizationHandler) AuthorizeTransaction(ctx context.Context, cmd port.AuthorizeTransactionCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.AuthorizeTransaction")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("transaction_id", cmd.TransactionID),
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("event_id", cmd.EventID),
	)

	tx, err := newTransactionFromCommand(cmd)
	if err != nil {
		return h.rejectInvalid(ctx, cmd, err, log)
	}

	// ── Blocklist: la tarjeta fue reportada como robada/perdida ──────────────
	// Se corta acá, antes del fraud check y del adquirente: la tarjeta ya está
	// bloqueada, no hay nada que consultar. Si la consulta a la blocklist
	// falla se deja seguir la Saga en vez de rechazar: un error de la base no
	// debe convertirse en un rechazo con orden de retener el plástico.
	blocked, err := h.isCardBlocked(ctx, tx.CardToken())
	if err != nil {
		log.Error("blocklist lookup failed — continuing without the check",
			slog.String("error", err.Error()))
	}
	if blocked {
		return h.rejectBlockedCard(ctx, tx, cmd.EventID, log)
	}

	if err := tx.StartFraudCheck(); err != nil {
		return fmt.Errorf("AuthorizeTransaction: start fraud check: %w", err)
	}

	// ── Paso 4: Persistir + marcar idempotencia en una sola transacción ──────
	var created bool
	err = pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, dbTx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			log.Info("event already processed — skipping")
			return nil
		}
		created, err = h.repo.InsertTx(ctx, dbTx, tx)
		if err != nil {
			return fmt.Errorf("save transaction: %w", err)
		}
		if !created {
			log.Warn(duplicateTransactionMsg)
			return nil
		}
		// ── Paso 5: FraudCheckRequested, en el outbox de la misma transacción ──
		if err := h.publisher.PublishFraudCheckRequested(ctx, dbTx, tx); err != nil {
			return fmt.Errorf("enqueue fraud check: %w", err)
		}
		return nil
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("AuthorizeTransaction: persist: %w", err)
	}
	if !created {
		return nil
	}
	h.metrics.RecordReceived(ctx)

	log.Info("transaction received and fraud check requested",
		slog.String("state", tx.State().String()),
		slog.Int64("amount_cents", cmd.AmountCents),
	)
	return nil
}

// newTransactionFromCommand valida el comando y crea el aggregate. Todos sus errores son de
// validación. El mensaje se loguea y se guarda, así que no incluye los datos de tarjeta que
// envía el cliente: card_last4 podría traer un PAN completo, y card_network es texto libre.
func newTransactionFromCommand(cmd port.AuthorizeTransactionCommand) (*aggregate.Transaction, error) {
	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid transaction_id: " + err.Error())
	}
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid terminal_id: " + err.Error())
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid merchant_id: " + err.Error())
	}
	currency, err := domain.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid currency: " + err.Error())
	}
	amount, err := domain.NewMoney(cmd.AmountCents, currency)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid amount: " + err.Error())
	}
	stan, err := domain.NewSTAN(cmd.STAN)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid stan: " + err.Error())
	}
	network, err := domain.ParseCardNetwork(cmd.CardNetwork)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid card_network: unknown value")
	}
	pan, err := domain.NewPAN(cmd.CardLast4, network)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid pan: card_last4 must be exactly 4 digits")
	}
	entryMode, err := valueobject.ParseEntryMode(cmd.EntryMode)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid entry_mode: " + err.Error())
	}
	cardToken, err := domain.ParseOptionalCardToken(cmd.CardToken)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid card_token: " + err.Error())
	}

	tx, err := aggregate.NewTransaction(
		txID, terminalID, merchantID,
		amount, stan, pan, entryMode,
		cardToken, cmd.EMVDataBase64, cmd.ISO8583Raw,
	)
	if err != nil {
		return nil, pkgerrors.NewValidationError(err.Error())
	}
	return tx, nil
}

// rejectInvalid avisa del rechazo de una transacción con datos inválidos, para que su sesión
// no quede en PROCESSING. Sus datos no cumplen las invariantes del aggregate, así que no entra
// en transactions: se registra aparte, para que una consulta no la confunda con un ID que nunca
// llegó.
//
// Devuelve cause para que el mensaje se descarte, salvo un error transitorio, que se reintenta
// antes de descartarlo. Sin un transaction_id válido no hay a quién avisar, y si esa
// transacción ya existe, su resultado sigue siendo el de la original.
func (h *AuthorizationHandler) rejectInvalid(
	ctx context.Context,
	cmd port.AuthorizeTransactionCommand,
	cause error,
	log *slog.Logger,
) error {
	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return cause
	}
	exists, err := h.repo.ExistsByID(ctx, txID)
	if err != nil {
		return fmt.Errorf("rejectInvalid: check existing transaction: %w", err)
	}
	if exists {
		log.Warn(duplicateTransactionMsg, slog.String("reason", "invalid data"))
		return cause
	}

	rejection := newInvalidRejection(cmd, txID, cause)
	// El registro y el rechazo van juntos: si algo falla, el mensaje se reintenta entero.
	if err := pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		if err := h.repo.SaveInvalidRejectionTx(ctx, dbTx, rejection); err != nil {
			return err
		}
		return h.publisher.PublishInvalidRejected(ctx, dbTx, rejection)
	}); err != nil {
		return fmt.Errorf("rejectInvalid: record and enqueue: %w", err)
	}
	h.metrics.RecordReceived(ctx)
	h.metrics.RecordRejected(ctx, rejection.Rejection.Code(), string(rejection.Rejection.Source()))
	log.Warn("transaction rejected — invalid data", slog.String("error", cause.Error()))
	return cause
}

// PurgeInvalidRejections borra el registro de las transacciones inválidas más antiguas que
// retention, y devuelve cuántos borró. Una consulta posterior por esos IDs responde NotFound.
func (h *AuthorizationHandler) PurgeInvalidRejections(ctx context.Context, now time.Time, retention time.Duration) (int64, error) {
	deleted, err := h.repo.DeleteInvalidRejectionsBefore(ctx, now.Add(-retention))
	if err != nil {
		return 0, fmt.Errorf("PurgeInvalidRejections: %w", err)
	}
	return deleted, nil
}

// newInvalidRejection arma el rechazo solo con los campos del comando que son válidos.
func newInvalidRejection(cmd port.AuthorizeTransactionCommand, txID domain.TransactionID, cause error) valueobject.InvalidTransactionRejection {
	r := valueobject.InvalidTransactionRejection{
		TransactionID:   txID,
		Rejection:       valueobject.NewRejectionFromValidation(valueobject.CodeInvalidData),
		ValidationError: cause.Error(),
		RejectedAt:      time.Now().UTC(),
	}
	if id, err := domain.ParseTerminalID(cmd.TerminalID); err == nil {
		r.TerminalID = id.String()
	}
	if id, err := domain.ParseMerchantID(cmd.MerchantID); err == nil {
		r.MerchantID = id.String()
	}
	if currency, err := domain.ParseCurrency(cmd.Currency); err == nil {
		r.Currency = currency.String()
		if amount, err := domain.NewMoney(cmd.AmountCents, currency); err == nil {
			r.AmountCents = amount.Cents()
		}
	}
	if network, err := domain.ParseCardNetwork(cmd.CardNetwork); err == nil {
		r.CardNetwork = string(network)
	}
	if pan, err := domain.NewPAN(cmd.CardLast4, domain.NetworkUnknown); err == nil {
		r.CardLast4 = pan.Last4()
	}
	if mode, err := valueobject.ParseEntryMode(cmd.EntryMode); err == nil {
		r.EntryMode = mode.String()
	}
	return r
}

// ApplyFraudScore procesa el resultado del motor antifraude y continúa la Saga.
// Es llamado por el subscriber NATS al recibir FraudScoreCalculated.
func (h *AuthorizationHandler) ApplyFraudScore(ctx context.Context, cmd port.ApplyFraudScoreCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ApplyFraudScore")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("transaction_id", cmd.TransactionID),
		slog.String("event_id", cmd.EventID),
		slog.Int("fraud_score", cmd.Score),
		slog.String("fraud_decision", cmd.Decision),
	)

	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id: " + err.Error())
	}
	fraudDecision, err := valueobject.NewFraudDecision(cmd.Score, cmd.Decision, cmd.RulesHit)
	if err != nil {
		return pkgerrors.NewValidationError("invalid fraud decision: " + err.Error())
	}

	// El evento se marca junto con el nuevo estado: si algo falla, no queda ninguno de los dos
	// y la reentrega reprocesa. La condición sobre el estado garantiza que, aunque lleguen
	// dos scores de la misma transacción, solo uno llegue al adquirente.
	var (
		tx       *aggregate.Transaction
		advanced bool
	)
	err = pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		claimed, err := h.idempotency.TryMarkAsProcessed(ctx, dbTx, cmd.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			log.Info("fraud score event already processed — skipping")
			return nil
		}
		if tx, err = h.repo.FindByIDForUpdateTx(ctx, dbTx, txID); err != nil {
			return fmt.Errorf("find transaction: %w", err)
		}
		if tx.State() != valueobject.StateFraudChecking {
			log.Warn("fraud score ignored — transaction is no longer waiting for it", slog.String("state", tx.State().String()))
			return nil
		}
		if err := tx.ApplyFraudDecision(fraudDecision); err != nil {
			return fmt.Errorf("apply decision: %w", err)
		}
		saved, err := h.repo.SaveIfStateTx(ctx, dbTx, tx, valueobject.StateFraudChecking)
		if err != nil {
			return fmt.Errorf("persist decision: %w", err)
		}
		if !saved {
			log.Warn("fraud score ignored — another score moved the transaction first")
			return nil
		}
		if tx.State() == valueobject.StateRejected {
			if err := h.publisher.PublishRejected(ctx, dbTx, tx); err != nil {
				return fmt.Errorf("enqueue rejection: %w", err)
			}
		}
		advanced = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("ApplyFraudScore: claim event: %w", err)
	}
	if !advanced {
		return nil
	}

	// Si el fraud rechazó, el rechazo ya quedó en el outbox
	if tx.State() == valueobject.StateRejected {
		h.recordRejection(ctx, tx)
		return nil
	}

	// Fraud aprobó → llamar al adquirente
	return h.runAcquirerCall(ctx, log, func(ctx context.Context) error {
		return h.callAcquirer(ctx, tx, log)
	})
}

// callAcquirer envía la transacción al host adquirente y procesa la respuesta. La
// transacción ya está guardada en PROCESSING y el evento, marcado: una reentrega no vuelve a
// llamar al adquirente. Sin una respuesta guardada, ReconcileUnresolved revierte la
// operación, porque no se sabe si el emisor la aprobó.
func (h *AuthorizationHandler) callAcquirer(
	ctx context.Context,
	tx *aggregate.Transaction,
	log *slog.Logger,
) error {
	ctx, span := observability.StartSpan(ctx, "command.callAcquirer")
	defer span.End()

	acquirerStart := time.Now()
	callCtx, cancel := h.withAcquirerTimeout(ctx)
	defer cancel()
	response, err := h.acquirer.Authorize(callCtx, tx)
	if errors.Is(err, service.ErrNotSent) {
		// Seguro no salió: no hay nada que revertir, y se responde como un rechazo.
		h.metrics.RecordAcquirerError(ctx)
		log.Warn("authorization not sent to the acquirer — rejecting", slog.String("error", err.Error()))
		response, err = notSentResponse(err), nil
	}
	if err != nil {
		h.metrics.RecordAcquirerDuration(ctx, time.Since(acquirerStart), "error")
		h.metrics.RecordAcquirerError(ctx)
		log.Warn("acquirer error — marking transaction as indeterminate",
			slog.String("error", err.Error()))
		if markErr := tx.MarkIndeterminate(); markErr != nil {
			return fmt.Errorf("callAcquirer: mark indeterminate: %w", markErr)
		}
		// Si no se puede guardar, sigue en PROCESSING y el conciliador la toma igual.
		if _, saveErr := h.repo.SaveIfState(ctx, tx, valueobject.StateProcessing); saveErr != nil {
			observability.RecordError(ctx, saveErr)
			log.Error("could not save INDETERMINATE — the reconciler will take the transaction from PROCESSING",
				slog.String("error", saveErr.Error()))
		}
		return nil
	}
	if response.IsApproved() {
		h.metrics.RecordAcquirerDuration(ctx, time.Since(acquirerStart), "approved")
	} else {
		h.metrics.RecordAcquirerDuration(ctx, time.Since(acquirerStart), "declined")
	}

	if response.IsApproved() {
		authCode, err := domain.NewAuthCode(response.AuthCode)
		if err != nil {
			return fmt.Errorf("callAcquirer: invalid auth code %q: %w", response.AuthCode, err)
		}
		if err := tx.Approve(authCode); err != nil {
			return fmt.Errorf("callAcquirer: approve: %w", err)
		}
	} else {
		rc, err := response.ToRejectionCode()
		if err != nil {
			rc = valueobject.NewRejectionFromValidation("UNKNOWN_RESPONSE")
		}
		if err := tx.Reject(rc); err != nil {
			return fmt.Errorf("callAcquirer: reject: %w", err)
		}
		// El emisor ordenó retener la tarjeta (ISO 04/41/43): además de
		// rechazar esta transacción, la tarjeta entra a la blocklist para que
		// el próximo intento se corte sin llegar al adquirente.
		h.blockCard(ctx, tx, rc, log)
	}

	saved, err := h.saveAndEnqueue(ctx, tx, valueobject.StateProcessing, func(dbTx pgx.Tx) error {
		if tx.State() == valueobject.StateApproved {
			return h.publisher.PublishApproved(ctx, dbTx, tx)
		}
		return h.publisher.PublishRejected(ctx, dbTx, tx)
	})
	if err != nil {
		// Sigue en PROCESSING: el conciliador la revierte, como con cualquier respuesta perdida.
		observability.RecordError(ctx, err)
		return fmt.Errorf("callAcquirer: persist result: %w", err)
	}
	if !saved {
		// La respuesta llegó después de que el conciliador tomara la transacción. No se
		// informa: el resultado para el cliente es el del conciliador, que la revierte.
		log.Error("acquirer response arrived after the reconciler took the transaction — ignoring it",
			slog.String("response_state", tx.State().String()))
		return nil
	}

	// El resultado ya quedó en el outbox, con el nuevo estado.
	if tx.State() == valueobject.StateApproved {
		h.metrics.RecordApproved(ctx)
		h.metrics.RecordSagaDuration(ctx, tx.ReceivedAt(), "approved")
		log.Info("transaction approved", slog.String("auth_code", tx.AuthCode().String()))
	} else {
		rc := tx.RejectionCode()
		h.recordRejection(ctx, tx)
		log.Info("transaction rejected",
			slog.String("rejection_code", rc.Code()),
			slog.String("rejection_source", string(rc.Source())),
		)
	}

	return nil
}

// notSentResponse traduce un envío imposible a un código ISO 8583: tarjeta desconocida, o un
// emisor al que no se puede llegar, que se puede reintentar.
func notSentResponse(err error) service.AcquirerResponse {
	code := valueobject.ISO_ISSUER_UNAVAILABLE
	if errors.Is(err, service.ErrCardDataNotFound) {
		code = valueobject.ISO_CARD_NOT_FOUND
	}
	return service.AcquirerResponse{ResponseCode: code, RespondedAt: time.Now().UTC()}
}

// isCardBlocked consulta la blocklist. Con la blocklist deshabilitada o sin
// token la tarjeta nunca está bloqueada — no se puede inferir la identidad de
// una tarjeta a partir de los últimos 4 dígitos.
func (h *AuthorizationHandler) isCardBlocked(ctx context.Context, token domain.CardToken) (bool, error) {
	if h.blockedCards == nil || token.IsZero() {
		return false, nil
	}
	return h.blockedCards.IsBlocked(ctx, token)
}

// blockCard agrega la tarjeta a la blocklist cuando el rechazo trae orden de
// retención. No propaga el error: el rechazo de esta transacción ya es
// correcto y no debe revertirse porque la escritura en la blocklist falle —
// se loguea en ERROR para que quede visible que el bloqueo no persistió.
func (h *AuthorizationHandler) blockCard(
	ctx context.Context,
	tx *aggregate.Transaction,
	rc valueobject.RejectionCode,
	log *slog.Logger,
) {
	if !rc.RequiresCardCapture() {
		return
	}
	if tx.CardToken().IsZero() {
		log.Warn("issuer ordered card capture but no card token was provided — card cannot be blocked",
			slog.String("rejection_code", rc.Code()))
		return
	}
	if h.blockedCards == nil {
		log.Warn("issuer ordered card capture but the blocklist is disabled — card not blocked",
			slog.String("rejection_code", rc.Code()))
		return
	}
	if err := h.blockedCards.Block(ctx, tx.CardToken(), rc.Code(), tx.ID()); err != nil {
		observability.RecordError(ctx, err)
		log.Error("failed to add card to blocklist",
			slog.String("rejection_code", rc.Code()),
			slog.String("error", err.Error()))
		return
	}
	log.Warn("card added to blocklist by issuer capture order",
		slog.String("rejection_code", rc.Code()),
		slog.String("card_token", tx.CardToken().String()))
}

// rejectBlockedCard rechaza una transacción cuya tarjeta ya está bloqueada,
// reclamando el evento en la misma transacción de base que el rechazo para
// que un redelivery de NATS no vuelva a publicarlo.
func (h *AuthorizationHandler) rejectBlockedCard(
	ctx context.Context,
	tx *aggregate.Transaction,
	eventID string,
	log *slog.Logger,
) error {
	rc := valueobject.NewRejectionFromBlocklist()
	if err := tx.Reject(rc); err != nil {
		return fmt.Errorf("rejectBlockedCard: reject: %w", err)
	}

	var created bool
	if err := pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, dbTx, eventID)
		if err != nil {
			return err
		}
		if !inserted {
			log.Info("event already processed — skipping")
			return nil
		}
		created, err = h.repo.InsertTx(ctx, dbTx, tx)
		if err != nil {
			return err
		}
		if !created {
			log.Warn(duplicateTransactionMsg)
			return nil
		}
		return h.publisher.PublishRejected(ctx, dbTx, tx)
	}); err != nil {
		observability.RecordError(ctx, err)
		return fmt.Errorf("rejectBlockedCard: persist: %w", err)
	}
	if !created {
		return nil
	}

	h.metrics.RecordReceived(ctx)
	log.Warn("transaction rejected — card is on the blocklist",
		slog.String("rejection_code", rc.Code()),
		slog.String("card_token", tx.CardToken().String()))
	h.recordRejection(ctx, tx)
	return nil
}

// recordRejection registra las métricas de un rechazo. El evento ya quedó en el outbox, en la
// transacción que guardó el rechazo.
func (h *AuthorizationHandler) recordRejection(ctx context.Context, tx *aggregate.Transaction) {
	rc := tx.RejectionCode()
	h.metrics.RecordRejected(ctx, rc.Code(), string(rc.Source()))
	h.metrics.RecordSagaDuration(ctx, tx.ReceivedAt(), "rejected")
}

// saveAndEnqueue guarda tx, si en la base sigue en expected, y encola su evento con enqueue, en
// una sola transacción: el evento se publica si y solo si el cambio se confirma. Devuelve false,
// sin encolar nada, si otro proceso la movió antes.
func (h *AuthorizationHandler) saveAndEnqueue(
	ctx context.Context,
	tx *aggregate.Transaction,
	expected valueobject.TransactionState,
	enqueue func(dbTx pgx.Tx) error,
) (bool, error) {
	var saved bool
	err := pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		ok, err := h.repo.SaveIfStateTx(ctx, dbTx, tx, expected)
		if err != nil {
			return fmt.Errorf("save: %w", err)
		}
		if !ok {
			return nil
		}
		if err := enqueue(dbTx); err != nil {
			return fmt.Errorf("enqueue event: %w", err)
		}
		saved = true
		return nil
	})
	return saved, err
}

// ProcessReversal procesa un pedido de anulación. El estado se valida antes de llamar al
// adquirente, y el pedido queda guardado junto con el evento: si la reversa falla o el proceso
// cae, el conciliador la reintenta.
//
//   - APPROVED: se revierte.
//   - FRAUD_CHECKING o PROCESSING: la venta sigue en curso. Se guarda el pedido, y el
//     conciliador la revierte si termina aprobada.
//   - INDETERMINATE: el conciliador ya la revierte. No hace falta nada más.
//   - REJECTED o REVERSED: no hay nada que revertir.
func (h *AuthorizationHandler) ProcessReversal(ctx context.Context, cmd port.ProcessReversalCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ProcessReversal")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("original_tx_id", cmd.OriginalTransactionID),
		slog.String("event_id", cmd.EventID),
	)

	txID, err := domain.ParseTransactionID(cmd.OriginalTransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid original_transaction_id")
	}

	var (
		tx      *aggregate.Transaction
		reverse bool
	)
	err = pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		claimed, err := h.idempotency.TryMarkAsProcessed(ctx, dbTx, cmd.EventID)
		if err != nil {
			return err
		}
		if !claimed {
			log.Info("reversal event already processed — skipping")
			return nil
		}
		if tx, err = h.repo.FindByIDForUpdateTx(ctx, dbTx, txID); err != nil {
			return fmt.Errorf("find transaction: %w", err)
		}
		switch tx.State() {
		case valueobject.StateApproved:
			reverse = true
		case valueobject.StateFraudChecking, valueobject.StateProcessing:
			log.Warn("reversal requested while the sale is in progress — it will be reversed if approved",
				slog.String("state", tx.State().String()))
		case valueobject.StateIndeterminate:
			log.Info("reversal requested — the reconciler is already reversing this transaction")
			return nil
		default:
			h.metrics.RecordReversalIgnored(ctx, tx.State().String())
			log.Warn("reversal ignored — nothing to reverse", slog.String("state", tx.State().String()))
			return nil
		}
		return h.repo.MarkReversalRequestedTx(ctx, dbTx, txID)
	})
	if err != nil {
		return fmt.Errorf("ProcessReversal: claim event: %w", err)
	}
	if !reverse {
		return nil
	}

	return h.runAcquirerCall(ctx, log, func(ctx context.Context) error {
		_, err := h.completeReversal(ctx, tx, log)
		if err != nil {
			// Queda en APPROVED con el pedido guardado: el conciliador la reintenta.
			log.Warn("reversal failed — the reconciler will retry", slog.String("error", err.Error()))
		}
		return nil
	})
}

// completeReversal revierte en el adquirente una transacción aprobada e informa el resultado.
// Devuelve false si otro proceso la completó antes.
func (h *AuthorizationHandler) completeReversal(ctx context.Context, tx *aggregate.Transaction, log *slog.Logger) (bool, error) {
	callCtx, cancel := h.withAcquirerTimeout(ctx)
	defer cancel()
	if err := h.acquirer.Reverse(callCtx, tx); err != nil {
		h.metrics.RecordAcquirerError(ctx)
		return false, fmt.Errorf("acquirer reversal: %w", err)
	}
	if err := tx.Reverse(); err != nil {
		return false, fmt.Errorf("reverse aggregate: %w", err)
	}
	// Si esto falla, se repite la reversa, que no tiene efecto.
	saved, err := h.saveAndEnqueue(ctx, tx, valueobject.StateApproved, func(dbTx pgx.Tx) error {
		return h.publisher.PublishReversalCompleted(ctx, dbTx, tx.ID(), tx)
	})
	if err != nil {
		return false, fmt.Errorf("persist: %w", err)
	}
	if !saved {
		return false, nil
	}
	log.Info("reversal completed successfully")
	return true, nil
}
