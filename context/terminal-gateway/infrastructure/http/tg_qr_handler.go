package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/port"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// QRHandler expone los endpoints JSON del flujo de pago con QR.
// No contiene HTML — el frontend vive en un repo separado (posnet-frontend).
type QRHandler struct {
	sessionService port.SessionService
	queryHandler   *query.SessionQueryHandler
	auth           *TerminalAuth // nil = sin autenticación, solo en desarrollo
}

func NewQRHandler(
	sessionService port.SessionService,
	queryHandler *query.SessionQueryHandler,
) *QRHandler {
	return &QRHandler{
		sessionService: sessionService,
		queryHandler:   queryHandler,
	}
}

// RegisterQRRoutes registra los endpoints en el mux existente.
func (h *QRHandler) RegisterQRRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/sessions/create", h.auth.requireTerminal(h.handleCreateQRSession))
	// /pay y /status los usa el teléfono del cliente, que no tiene el token del terminal.
	mux.HandleFunc("POST /api/sessions/{id}/pay", h.handleSimulatePay)
	mux.HandleFunc("GET  /api/sessions/{id}/status", h.handleSessionStatus)
}

// POST /api/sessions/create
func (h *QRHandler) handleCreateQRSession(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.CreateQRSession")
	defer span.End()

	var req struct {
		TerminalID  string `json:"terminal_id"`
		MerchantID  string `json:"merchant_id"`
		AmountCents int64  `json:"amount_cents"`
		Currency    string `json:"currency"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_BODY", "invalid request body"))
		return
	}

	if id, ok := authenticatedTerminal(ctx); ok {
		// El comercio, si falta, lo completa CreateSession con el del terminal.
		if req.TerminalID != "" && req.TerminalID != id.String() {
			writeForbidden(w)
			return
		}
		req.TerminalID = id.String()
	} else {
		// Sin tokens, solo en desarrollo: el terminal de prueba de 01-init.sql.
		if req.TerminalID == "" {
			req.TerminalID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
		}
		if req.MerchantID == "" {
			req.MerchantID = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
		}
	}
	if req.Currency == "" {
		req.Currency = "ARS"
	}
	if req.AmountCents <= 0 {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_AMOUNT", "amount_cents must be positive"))
		return
	}

	stan := int(time.Now().UnixNano()%999998 + 1)

	cmd := port.CreateSessionCommand{
		TerminalID:     req.TerminalID,
		MerchantID:     req.MerchantID,
		AmountCents:    req.AmountCents,
		Currency:       req.Currency,
		STAN:           stan,
		PaymentChannel: "QR",
	}

	result, err := h.sessionService.CreateSession(ctx, cmd)
	if err != nil {
		var invalid *pkgerrors.ValidationError
		if errors.As(err, &invalid) {
			writeJSON(w, http.StatusBadRequest, errResp("INVALID_REQUEST", invalid.Error()))
			return
		}
		writeInternalError(ctx, w, err)
		return
	}

	localIP := getLocalIP()
	qrContent := fmt.Sprintf("http://%s:%s/pay/%s", localIP, frontendPort, result.TransactionID)

	writeJSON(w, http.StatusCreated, map[string]any{
		"transaction_id": result.TransactionID,
		"expires_at":     result.ExpiresAt,
		"ttl_seconds":    result.TTLSeconds,
		"amount_cents":   req.AmountCents,
		"currency":       req.Currency,
		"qr_content":     qrContent,
	})
}

// POST /api/sessions/{id}/pay
//
// Fix: en lugar de publicar TransactionReceived directamente a NATS,
// delega a sessionService.ProcessPayment() que:
//  1. Llama session.StartProcessing() → transiciona AWAITING_PAYMENT → PROCESSING
//  2. Persiste la sesión en PROCESSING (SaveTx)
//  3. Escribe el evento en el outbox atómicamente (misma TX de Postgres)
//  4. El Relay publica a NATS desde el outbox
//
// Sin este fix la sesión queda en AWAITING_PAYMENT y cuando Authorization
// responde con AuthApproved, la transición AWAITING_PAYMENT → APPROVED
// falla porque no está permitida en la state machine.
func (h *QRHandler) handleSimulatePay(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.SimulatePay")
	defer span.End()

	txID := r.PathValue("id")
	if _, err := domain.ParseTransactionID(txID); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "invalid transaction id"))
		return
	}

	var req struct {
		CardLast4   string `json:"card_last4"`
		CardNetwork string `json:"card_network"`
		EntryMode   string `json:"entry_mode"`
		CardToken   string `json:"card_token"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	// Se valida acá porque, una vez publicado el evento, el cliente ya recibió el 202.
	if _, err := domain.ParseOptionalCardToken(req.CardToken); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_CARD_TOKEN", "card_token is not valid"))
		return
	}
	if req.CardLast4 == "" {
		req.CardLast4 = "0000"
	}
	if req.CardNetwork == "" {
		req.CardNetwork = "VISA"
	}
	if req.EntryMode == "" {
		req.EntryMode = "QR"
	}
	// Antes de empezar el pago: la sesión sigue disponible para reintentar, y un PAN enviado por
	// error en estos campos no llega al evento, que NATS conserva sin vencimiento. Las respuestas
	// no repiten el valor recibido.
	network, err := domain.ParseCardNetwork(req.CardNetwork)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_CARD_NETWORK", "card_network is not valid"))
		return
	}
	if _, err := domain.NewPAN(req.CardLast4, network); err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_CARD_LAST4", "card_last4 must be exactly 4 digits"))
		return
	}

	// Delegar a ProcessPayment:
	//   StartProcessing() → AWAITING_PAYMENT → PROCESSING
	//   SaveTx() + outbox.InsertTx() atómico en Postgres
	//   Relay publica TransactionReceived a NATS
	cmd := port.ProcessPaymentCommand{
		EventID:       domain.NewTransactionID().String(),
		TransactionID: txID,
		ISO8583Raw:    []byte{},
		EMVDataBase64: "",
		CardLast4:     req.CardLast4,
		CardNetwork:   req.CardNetwork,
		CardToken:     req.CardToken,
	}

	if err := h.sessionService.ProcessPayment(ctx, cmd); err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "session not found or expired"))
			return
		}
		writeInternalError(ctx, w, err)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":         "processing",
		"transaction_id": txID,
	})
}

// GET /api/sessions/{id}/status
func (h *QRHandler) handleSessionStatus(w http.ResponseWriter, r *http.Request) {
	ctx, span := observability.StartSpan(r.Context(), "http.SessionStatus")
	defer span.End()

	txID := r.PathValue("id")
	parsedID, err := domain.ParseTransactionID(txID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errResp("INVALID_ID", "invalid transaction id"))
		return
	}

	session, err := h.queryHandler.GetSessionStatus(ctx, parsedID)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			writeJSON(w, http.StatusNotFound, errResp("NOT_FOUND", "session not found"))
			return
		}
		observability.RecordError(ctx, err)
		writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"transaction_id":   session.TransactionID,
		"state":            session.State,
		"amount_cents":     session.AmountCents,
		"currency":         session.Currency,
		"auth_code":        session.AuthCode,
		"rejection_code":   session.RejectionCode,
		"rejection_reason": session.RejectionReason,
		"ttl_seconds":      session.TTLSeconds,
		"expires_at":       session.ExpiresAt,
	})
}

// frontendPort es el puerto del frontend de pago, al que apunta qr_content.
const frontendPort = "5173"

// writeInternalError registra el error y responde un 500 sin su detalle, que puede exponer datos
// internos: SQL, direcciones o nombres de tablas.
func writeInternalError(ctx context.Context, w http.ResponseWriter, err error) {
	observability.RecordError(ctx, err)
	slog.ErrorContext(ctx, "request failed", slog.String("error", err.Error()))
	writeJSON(w, http.StatusInternalServerError, errResp("INTERNAL", "internal error"))
}

func getLocalIP() string {
	if host := os.Getenv("POSNET_HOST"); host != "" {
		return host
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "localhost"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
