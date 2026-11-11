// Package port define los comandos, resultados y puertos de la capa de aplicación del BC Settlement.
package port

// ─── Commands ─────────────────────────────────────────────────────────────────

// RegisterApprovalCommand contiene los datos del evento AuthorizationApproved.
type RegisterApprovalCommand struct {
	EventID       string
	TransactionID string
	TerminalID    string
	MerchantID    string
	AmountCents   int64
	Currency      string
	AuthorizedAt  string // RFC3339 UTC — define la fecha del batch
}

// RegisterReversalCommand contiene los datos del evento ReversalCompleted.
type RegisterReversalCommand struct {
	EventID               string
	OriginalTransactionID string
	TerminalID            string
	MerchantID            string
	AmountCents           int64
	Currency              string
	CompletedAt           string // RFC3339 UTC
}

// ProcessBatchCloseCommand contiene los datos del evento BatchCloseRequested.
type ProcessBatchCloseCommand struct {
	EventID        string
	TerminalID     string
	MerchantID     string
	BatchDate      string // "2025-06-04"; vacío = día de negocio de RequestedAt
	RequestedAt    string // RFC3339 UTC
	TerminalCount  int    // Total reportado por el terminal
	TerminalAmount int64  // Monto total reportado por el terminal
	Currency       string
}

// ForceCloseCommand fuerza el cierre de un batch desde operaciones.
type ForceCloseCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
	Reason     string // Opcional, salvo con SETTLEMENT_REQUIRE_REASON=true

	// TerminalCount y TerminalAmount son los totales del terminal, si el operador los conoce: van
	// juntos. Con ellos se concilia como en el cierre del terminal; sin ellos, se cierra con los
	// del backend.
	TerminalCount  *int
	TerminalAmount *int64
}

// ResubmitBatchCommand reintenta el envío al procesador de un batch CLOSED.
type ResubmitBatchCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
	Reason     string // Opcional, salvo con SETTLEMENT_REQUIRE_REASON=true
}

// ReopenBatchCommand devuelve un batch en disputa a OPEN.
type ReopenBatchCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
	Reason     string // Para auditoría
}

// ResolveDisputeCommand cierra un batch en disputa con los totales del backend.
type ResolveDisputeCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
	Reason     string // Para auditoría
}

// SettleBatchCommand confirma la liquidación de un batch SUBMITTED.
type SettleBatchCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
	Reason     string // Opcional, salvo con SETTLEMENT_REQUIRE_REASON=true
}

// ResolveReversalReviewCommand cierra la revisión de una reversa.
type ResolveReversalReviewCommand struct {
	TransactionID string
	OperatorID    string // Para auditoría
	Resolution    string // Qué se hizo y por qué; ticket si lo hay
	Action        string // ACCEPT, USE_PURCHASE_AMOUNT o EXCLUDE
}

// SetMerchantTimezoneCommand define la zona del día de negocio de un comercio.
type SetMerchantTimezoneCommand struct {
	MerchantID string
	Timezone   string // Zona IANA
	OperatorID string // Para auditoría
}

// ListBatchesCommand filtra batches por comercio y fecha.
type ListBatchesCommand struct {
	MerchantID string
	Date       string // "2025-06-04"
}

// ─── Results ─────────────────────────────────────────────────────────────────

// BatchResult es el resultado de las queries de batch.
type BatchResult struct {
	ID          string
	TerminalID  string
	MerchantID  string
	BatchDate   string
	State       string
	Currency    string
	TotalCount  int
	TotalAmount int64 // Incluye los ajustes
	// Correcciones de lotes ya enviados que liquida este lote, con signo.
	AdjustmentCount  int
	AdjustmentAmount int64
	Discrepancies    int
	// ReconciledWith es contra qué se concilió: TERMINAL, BACKEND (sin totales del terminal) o
	// NONE (retenido sin conciliar). Vacío en un lote OPEN.
	ReconciledWith string
	ClosedAt       string
	SubmittedAt    string
	SettledAt      string
}

// AuditEntryResult es un registro de la auditoría de un lote.
type AuditEntryResult struct {
	ID                    string `json:"id"`
	Action                string `json:"action"`
	OperatorID            string `json:"operator_id"`
	Reason                string `json:"reason,omitempty"`
	FromState             string `json:"from_state"`
	ToState               string `json:"to_state"`
	PreviousDiscrepancies int    `json:"previous_discrepancies"`
	TotalAmount           *int64 `json:"total_amount"`
	ConfirmationID        string `json:"confirmation_id,omitempty"`
	CreatedAt             string `json:"created_at"`
}

// ReversalReviewResult es una revisión abierta de una reversa.
type ReversalReviewResult struct {
	TransactionID      string  `json:"transaction_id"`
	BatchID            string  `json:"batch_id"`
	BatchState         string  `json:"batch_state"`
	Reason             string  `json:"reason"`
	MerchantID         string  `json:"merchant_id"`
	ReversalAmount     int64   `json:"reversal_amount"`
	Currency           string  `json:"currency"`
	PurchaseMerchantID *string `json:"purchase_merchant_id"`
	PurchaseAmount     *int64  `json:"purchase_amount"`
	PurchaseCurrency   *string `json:"purchase_currency"`
	CreatedAt          string  `json:"created_at"`
}

// MerchantTimezoneResult es la zona de negocio vigente de un comercio.
type MerchantTimezoneResult struct {
	MerchantID string `json:"merchant_id"`
	Timezone   string `json:"timezone"`
	Source     string `json:"source"` // "merchant" o "default"
}
