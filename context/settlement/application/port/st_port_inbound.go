// Package port define los puertos de entrada del BC Settlement.
package port

// BatchService es el puerto de entrada principal.
// El adaptador NATS lo llama al consumir eventos de Authorization y Terminal Gateway.
type BatchService interface {
	// RegisterApproval agrega una transacción aprobada al batch del terminal.
	// Consumido desde el evento AuthorizationApproved.
	RegisterApproval(ctx interface{}, cmd RegisterApprovalCommand) error

	// RegisterReversal descuenta una anulación del batch del terminal.
	// Consumido desde el evento ReversalCompleted.
	RegisterReversal(ctx interface{}, cmd RegisterReversalCommand) error

	// ProcessBatchClose procesa el cierre de lote solicitado por el terminal.
	// Consumido desde el evento BatchCloseRequested.
	ProcessBatchClose(ctx interface{}, cmd ProcessBatchCloseCommand) error
}

// AdminService es el puerto de entrada para operaciones manuales.
// Expuesto vía HTTP para soporte y operaciones.
type AdminService interface {
	// GetBatch retorna el estado de un batch por su ID.
	GetBatch(ctx interface{}, batchID string) (*BatchResult, error)

	// ListBatchesByMerchant lista los batches de un comercio en una fecha.
	ListBatchesByMerchant(ctx interface{}, cmd ListBatchesCommand) ([]*BatchResult, error)

	// ForceClose fuerza el cierre de un batch (uso en operaciones de soporte).
	ForceClose(ctx interface{}, cmd ForceCloseCommand) error

	// ResubmitBatch reintenta el envío al procesador externo de un batch
	// atascado en CLOSED (ej: el envío original falló por una falla transitoria
	// del procesador). Uso en operaciones de soporte.
	ResubmitBatch(ctx interface{}, cmd ResubmitBatchCommand) error

	// ReopenBatch devuelve un batch en disputa a OPEN para corregirlo y volver a cerrarlo.
	ReopenBatch(ctx interface{}, cmd ReopenBatchCommand) error

	// ResolveDispute cierra un batch en disputa aceptando los totales del backend.
	ResolveDispute(ctx interface{}, cmd ResolveDisputeCommand) error

	// SettleBatch confirma la liquidación de un batch SUBMITTED.
	SettleBatch(ctx interface{}, cmd SettleBatchCommand) error
}

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
}

// ResubmitBatchCommand reintenta el envío al procesador de un batch CLOSED.
type ResubmitBatchCommand struct {
	BatchID    string
	OperatorID string // Para auditoría
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
}

// ListBatchesCommand filtra batches por comercio y fecha.
type ListBatchesCommand struct {
	MerchantID string
	Date       string // "2025-06-04"
}

// ─── Results ─────────────────────────────────────────────────────────────────

// BatchResult es el resultado de las queries de batch.
type BatchResult struct {
	ID            string
	TerminalID    string
	MerchantID    string
	BatchDate     string
	State         string
	Currency      string
	TotalCount    int
	TotalAmount   int64
	Discrepancies int
	ClosedAt      string
	SubmittedAt   string
	SettledAt     string
}
