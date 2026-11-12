// Package port define los puertos de entrada del BC Notification.
package port

// NotificationService es el puerto de entrada principal.
// El adaptador NATS lo llama al consumir eventos de Authorization y Settlement.
type NotificationService interface {
	// NotifyApproval crea y despacha la notificación de transacción aprobada.
	// Consumido desde AuthorizationApproved.
	NotifyApproval(ctx interface{}, cmd NotifyApprovalCommand) error

	// NotifyRejection crea y despacha la notificación de transacción rechazada.
	// Consumido desde AuthorizationRejected.
	NotifyRejection(ctx interface{}, cmd NotifyRejectionCommand) error

	// NotifyBatchClosed crea y despacha la notificación de cierre de lote.
	// Consumido desde BatchClosed.
	NotifyBatchClosed(ctx interface{}, cmd NotifyBatchClosedCommand) error

	// RetryFailed reintenta el envío de una notificación en estado RETRYING.
	// Llamado por el job periódico de reintentos.
	RetryFailed(ctx interface{}, notificationID string) error
}

// AdminService es el puerto de entrada para operaciones.
type AdminService interface {
	// GetNotification retorna el estado de una notificación por ID.
	GetNotification(ctx interface{}, id string) (*NotificationResult, error)

	// GetByTransactionID retorna todas las notificaciones de una transacción.
	GetByTransactionID(ctx interface{}, transactionID string) ([]*NotificationResult, error)

	// ListDead retorna notificaciones en estado DEAD para revisión manual.
	ListDead(ctx interface{}, limit int) ([]*NotificationResult, error)

	// ForceRetry fuerza el reintento manual de una notificación DEAD.
	ForceRetry(ctx interface{}, notificationID string) error
}

// ─── Commands ─────────────────────────────────────────────────────────────────

// NotifyApprovalCommand contiene los datos del evento AuthorizationApproved.
type NotifyApprovalCommand struct {
	EventID       string
	TransactionID string
	TerminalID    string
	MerchantID    string
	AuthCode      string
	AmountCents   int64
	Currency      string
	CardLast4     string
	CardNetwork   string
	EntryMode     string
	AuthorizedAt  string // RFC3339 UTC
}

// NotifyRejectionCommand contiene los datos del evento AuthorizationRejected.
type NotifyRejectionCommand struct {
	EventID         string
	TransactionID   string
	TerminalID      string
	MerchantID      string
	RejectionCode   string
	RejectionReason string
	IsRetryable     bool
	AmountCents     int64
	Currency        string
	CardLast4       string
	CardNetwork     string
	EntryMode       string
	RejectedAt      string // RFC3339 UTC
}

// NotifyBatchClosedCommand contiene los datos del evento BatchClosed.
type NotifyBatchClosedCommand struct {
	EventID       string
	BatchID       string
	TerminalID    string
	MerchantID    string
	BatchDate     string
	TotalCount    int
	TotalAmount   int64
	Currency      string
	Discrepancies int
	ClosedAt      string // RFC3339 UTC

	AdjustmentCount  int
	AdjustmentAmount int64
	ReconciledWith   string
	Trigger          string // events.BatchClose*: un cierre o una revisión
	Revision         bool   // Corrige un cierre anterior del mismo lote
}

// NotifySettlementCompletedCommand contiene los datos del evento SettlementCompleted.
type NotifySettlementCompletedCommand struct {
	EventID          string
	MerchantID       string
	SettlementDate   string // YYYY-MM-DD
	Currency         string
	Revision         int
	TotalBatches     int
	TotalAmount      int64
	AdjustmentAmount int64
	NetAmount        int64
	MDRPercent       float64
	DeltaBatches     int
	DeltaAmount      int64
	DeltaNetAmount   int64
	CompletedAt      string // RFC3339 UTC
}

// ─── Results ─────────────────────────────────────────────────────────────────

// NotificationResult es el resultado de las queries de notificación.
type NotificationResult struct {
	ID            string
	TransactionID string
	MerchantID    string
	Channel       string
	State         string
	AttemptCount  int
	MaxAttempts   int
	CreatedAt     string
	DispatchedAt  string
	NextRetryAt   string
	Attempts      []AttemptResult
}

// AttemptResult describe un intento individual de entrega.
type AttemptResult struct {
	AttemptNumber int
	Success       bool
	HTTPStatus    int
	ErrorMessage  string
	AttemptedAt   string
}
