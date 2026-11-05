// Package command contiene los command handlers del BC Terminal Gateway.
package command

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/service"
	valueobject "github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

// SessionHandler implementa port.SessionService y port.AuthResultService.
// Orquesta el ciclo de vida de las sesiones de pago del terminal.
type SessionHandler struct {
	sessionRepo  repository.PaymentSessionRepository
	terminalRepo repository.TerminalRepository
	notifier     service.TerminalNotifier
	publisher    service.EventPublisher
	idempotency  *natsutil.IdempotencyStore
	outbox       *outbox.Store
	pool         pgutil.PgxPool
	metrics      *Metrics // opcional; nil = sin instrumentación (tests)
}

// SetMetrics inyecta los instrumentos de negocio (desde el wiring, tras InitMeter).
func (h *SessionHandler) SetMetrics(m *Metrics) {
	h.metrics = m
}

// NewSessionHandler construye el handler con todas sus dependencias.
func NewSessionHandler(
	sessionRepo repository.PaymentSessionRepository,
	terminalRepo repository.TerminalRepository,
	notifier service.TerminalNotifier,
	publisher service.EventPublisher,
	idempotency *natsutil.IdempotencyStore,
	outboxStore *outbox.Store,
	pool pgutil.PgxPool,
) *SessionHandler {
	return &SessionHandler{
		sessionRepo:  sessionRepo,
		terminalRepo: terminalRepo,
		notifier:     notifier,
		publisher:    publisher,
		idempotency:  idempotency,
		outbox:       outboxStore,
		pool:         pool,
	}
}

// CreateSession inicia una nueva sesión de pago.
// Crea el aggregate, lo persiste y notifica al terminal (QR o NFC activo).
func (h *SessionHandler) CreateSession(ctx context.Context, cmd port.CreateSessionCommand) (*port.SessionCreatedResult, error) {
	ctx, span := observability.StartSpan(ctx, "command.CreateSession")
	defer span.End()

	log := observability.FromContext(ctx).With(
		slog.String("terminal_id", cmd.TerminalID),
		slog.String("channel", cmd.PaymentChannel),
	)

	// Parsear Value Objects
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid terminal_id")
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid merchant_id")
	}
	currency, err := domain.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid currency")
	}
	amount, err := domain.NewMoney(cmd.AmountCents, currency)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid amount")
	}
	stan, err := domain.NewSTAN(cmd.STAN)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid stan")
	}
	channel, err := valueobject.ParsePaymentChannel(cmd.PaymentChannel)
	if err != nil {
		return nil, pkgerrors.NewValidationError("invalid payment channel")
	}

	// Verificar que el terminal esté activo
	terminal, err := h.terminalRepo.FindByID(ctx, terminalID)
	if err != nil {
		return nil, fmt.Errorf("CreateSession: find terminal: %w", err)
	}
	if !terminal.IsActive() {
		return nil, pkgerrors.NewValidationError("terminal is not active")
	}

	// Crear la sesión
	session, err := aggregate.NewPaymentSession(terminalID, merchantID, amount, stan, channel)
	if err != nil {
		return nil, fmt.Errorf("CreateSession: create session: %w", err)
	}

	// Persistir
	if err := h.sessionRepo.Save(ctx, session); err != nil {
		observability.RecordError(ctx, err)
		return nil, fmt.Errorf("CreateSession: save session: %w", err)
	}

	h.metrics.RecordCreated(ctx)

	// Notificar al terminal (QR en pantalla o NFC activo)
	if err := h.notifier.NotifySessionCreated(ctx, session); err != nil {
		log.Error("failed to notify terminal of session creation",
			slog.String("transaction_id", session.ID().String()),
			slog.String("error", err.Error()),
		)
		// No es fatal — el terminal puede reintentar vía reconexión WebSocket
	}

	log.Info("payment session created",
		slog.String("transaction_id", session.ID().String()),
		slog.Int64("amount_cents", cmd.AmountCents),
		slog.String("expires_at", session.ExpiresAt().Format(time.RFC3339)),
	)

	return &port.SessionCreatedResult{
		TransactionID: session.ID().String(),
		ExpiresAt:     session.ExpiresAt().Format(time.RFC3339),
		TTLSeconds:    int(session.TTLRemaining().Seconds()),
		Channel:       session.Channel().String(),
		Amount:        session.Amount(),
	}, nil
}

// ProcessPayment procesa el mensaje ISO 8583 e inicia la Saga de autorización.
// Usa el patrón Transactional Outbox: el Save de la sesión y la entrada en el outbox
// se realizan en la misma transacción Postgres, eliminando el dual-write.
// El Relay publica el evento a NATS de forma asíncrona con reintentos automáticos.
//
// Fix: cmd.CardLast4 y cmd.CardNetwork se pasan explícitamente al publisher.
// En el flujo QR estos valores vienen del payload del cliente (pay.html).
// En un flujo ISO 8583 real se parsearían del mensaje raw por el adaptador.
func (h *SessionHandler) ProcessPayment(ctx context.Context, cmd port.ProcessPaymentCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ProcessPayment")
	defer span.End()

	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id")
	}

	session, err := h.sessionRepo.FindByID(ctx, txID)
	if err != nil {
		return fmt.Errorf("ProcessPayment: find session: %w", err)
	}

	if err := session.StartProcessing(cmd.ISO8583Raw, cmd.EMVDataBase64); err != nil {
		return fmt.Errorf("ProcessPayment: start processing: %w", err)
	}

	// Pasar CardLast4/CardNetwork del comando al publisher.
	// Sin esto el payload llega a Authorization con card_network=""
	// y ParseCardNetwork falla con ValidationError → msg.Term() silencioso.
	subject, eventID, payload, err := h.publisher.BuildTransactionReceived(
		ctx,
		session,
		cmd.ISO8583Raw,
		cmd.EMVDataBase64,
		cmd.CardLast4,
		cmd.CardNetwork,
		cmd.CardToken,
	)
	if err != nil {
		return fmt.Errorf("ProcessPayment: build event: %w", err)
	}

	// Atómico: Save de la sesión + inserción en outbox en la misma TX.
	// Si el pod muere tras el commit, el Relay recupera la entrada del outbox
	// y publica a NATS en el próximo ciclo. JetStream deduplica por event_id.
	return pgutil.WithReadCommitted(ctx, h.pool, func(dbTx pgx.Tx) error {
		if err := h.sessionRepo.SaveTx(ctx, dbTx, session); err != nil {
			return fmt.Errorf("save session: %w", err)
		}
		return h.outbox.InsertTx(ctx, dbTx, subject, eventID, payload)
	})
}

// ApplyApproval recibe el resultado aprobado desde NATS y notifica al terminal.
func (h *SessionHandler) ApplyApproval(ctx context.Context, cmd port.ApplyApprovalCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ApplyApproval")
	defer span.End()

	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id")
	}

	session, err := h.sessionRepo.FindByID(ctx, txID)
	if err != nil {
		return fmt.Errorf("ApplyApproval: find session: %w", err)
	}

	if err := session.Approve(cmd.AuthCode); err != nil {
		return fmt.Errorf("ApplyApproval: approve session: %w", err)
	}

	processed := false
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		processed = true
		return h.sessionRepo.Save(ctx, session)
	})
	if err != nil {
		return fmt.Errorf("ApplyApproval: persist: %w", err)
	}

	if processed {
		h.metrics.RecordApproved(ctx, session.CreatedAt())
		if err := h.notifier.NotifyResult(ctx, session); err != nil {
			observability.FromContext(ctx).Error("failed to notify terminal of approval",
				slog.String("transaction_id", cmd.TransactionID),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

// ApplyRejection recibe el resultado rechazado desde NATS y notifica al terminal.
func (h *SessionHandler) ApplyRejection(ctx context.Context, cmd port.ApplyRejectionCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.ApplyRejection")
	defer span.End()

	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id")
	}

	session, err := h.sessionRepo.FindByID(ctx, txID)
	if err != nil {
		return fmt.Errorf("ApplyRejection: find session: %w", err)
	}

	if err := session.Reject(cmd.RejectionCode, cmd.RejectionReason); err != nil {
		return fmt.Errorf("ApplyRejection: reject session: %w", err)
	}

	processed := false
	err = pgutil.WithReadCommitted(ctx, h.pool, func(tx pgx.Tx) error {
		inserted, err := h.idempotency.TryMarkAsProcessed(ctx, tx, cmd.EventID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		processed = true
		return h.sessionRepo.Save(ctx, session)
	})
	if err != nil {
		return fmt.Errorf("ApplyRejection: persist: %w", err)
	}

	if processed {
		if cmd.CaptureCard {
			// El emisor ordenó retener la tarjeta. Se loguea en WARN aparte del
			// rechazo porque es una acción física sobre el plástico del portador
			// y debe quedar en la traza de auditoría aunque la notificación al
			// terminal falle después.
			observability.FromContext(ctx).Warn("issuer ordered card capture — terminal must retain the card",
				slog.String("transaction_id", cmd.TransactionID),
				slog.String("terminal_id", cmd.TerminalID),
				slog.String("rejection_code", cmd.RejectionCode),
				slog.String("rejection_reason", cmd.RejectionReason),
			)
		}
		if err := h.notifier.NotifyResult(ctx, session); err != nil {
			observability.FromContext(ctx).Error("failed to notify terminal of rejection",
				slog.String("transaction_id", cmd.TransactionID),
				slog.Bool("capture_card", cmd.CaptureCard),
				slog.String("error", err.Error()),
			)
		}
	}

	return nil
}

// CancelSession cancela una sesión activa por acción del cajero.
func (h *SessionHandler) CancelSession(ctx context.Context, cmd port.CancelSessionCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.CancelSession")
	defer span.End()

	txID, err := domain.ParseTransactionID(cmd.TransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid transaction_id")
	}
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}

	session, err := h.sessionRepo.FindByID(ctx, txID)
	if err != nil {
		return fmt.Errorf("CancelSession: find session: %w", err)
	}
	if !session.TerminalID().Equals(terminalID) {
		return pkgerrors.NewValidationError("terminal is not authorized to cancel this session")
	}

	if err := session.Cancel(); err != nil {
		return fmt.Errorf("CancelSession: cancel: %w", err)
	}

	return h.sessionRepo.Save(ctx, session)
}

// RequestBatchClose publica el evento de cierre de lote a NATS.
func (h *SessionHandler) RequestBatchClose(ctx context.Context, cmd port.RequestBatchCloseCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.RequestBatchClose")
	defer span.End()

	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}
	merchantID, err := domain.ParseMerchantID(cmd.MerchantID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid merchant_id")
	}

	// Settlement busca el lote por moneda y descarta un pedido sin una válida: se
	// rechaza acá para que el terminal reciba el error en lugar de un 202 sin efecto.
	if _, err := domain.ParseCurrency(cmd.Currency); err != nil {
		return pkgerrors.NewValidationError("invalid currency")
	}
	if cmd.BatchDate != "" {
		if _, err := time.Parse("2006-01-02", cmd.BatchDate); err != nil {
			return pkgerrors.NewValidationError("invalid batch_date")
		}
	}

	return h.publisher.PublishBatchCloseRequested(ctx,
		terminalID, merchantID, cmd.BatchDate,
		cmd.TerminalCount, cmd.TerminalAmount, cmd.Currency,
	)
}

// RequestReversal publica la solicitud de anulación a NATS.
func (h *SessionHandler) RequestReversal(ctx context.Context, cmd port.RequestReversalCommand) error {
	ctx, span := observability.StartSpan(ctx, "command.RequestReversal")
	defer span.End()

	origTxID, err := domain.ParseTransactionID(cmd.OriginalTransactionID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid original_transaction_id")
	}
	terminalID, err := domain.ParseTerminalID(cmd.TerminalID)
	if err != nil {
		return pkgerrors.NewValidationError("invalid terminal_id")
	}

	// Cargar la sesión original para tener el MerchantID y monto
	session, err := h.sessionRepo.FindByID(ctx, origTxID)
	if err != nil {
		return fmt.Errorf("RequestReversal: find original session: %w", err)
	}
	if !session.TerminalID().Equals(terminalID) {
		return pkgerrors.NewValidationError("terminal is not authorized to reverse this transaction")
	}

	return h.publisher.PublishReversalRequested(ctx, origTxID, session)
}
