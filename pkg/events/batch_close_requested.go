package events

// BatchCloseRequestedPayload es el payload del evento posnet.transaction.batch-close.v1
//
// Publicado por: Terminal Gateway
// Consumido por: Settlement
//
// Se emite cuando el terminal solicita el cierre de lote al final del día.
// Incluye el resumen que el terminal tiene localmente para que Settlement
// pueda compararlo contra su propio registro y detectar discrepancias.
type BatchCloseRequestedPayload struct {
	TerminalID     string `json:"terminal_id"`
	MerchantID     string `json:"merchant_id"`
	BatchDate      string `json:"batch_date"`      // "2025-06-04"; vacía = día de negocio de RequestedAt
	TerminalCount  int    `json:"terminal_count"`  // Cantidad de tx reportadas por el terminal
	TerminalAmount int64  `json:"terminal_amount"` // Total en centavos reportado por el terminal
	Currency       string `json:"currency"`
	RequestedAt    string `json:"requested_at"` // RFC3339 UTC
}
