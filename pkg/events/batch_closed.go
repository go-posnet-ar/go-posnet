package events

// BatchClosedPayload es el payload del evento posnet.settlement.batch-closed.v1
//
// Publicado por: Settlement
// Consumido por: Notification
//
// Se emite cuando Settlement termina de procesar el cierre de lote
// de un terminal. Incluye los totales finales y la cantidad de
// discrepancias detectadas entre el resumen del terminal y el backend.
type BatchClosedPayload struct {
	BatchID       string `json:"batch_id"`
	TerminalID    string `json:"terminal_id"`
	MerchantID    string `json:"merchant_id"`
	BatchDate     string `json:"batch_date"`   // "2025-06-04"
	TotalCount    int    `json:"total_count"`  // Cantidad de transacciones en el lote
	TotalAmount   int64  `json:"total_amount"` // Suma total en centavos, con los ajustes
	Currency      string `json:"currency"`
	Discrepancies int    `json:"discrepancies"` // Gaps detectados entre terminal y backend
	ClosedAt      string `json:"closed_at"`     // RFC3339 UTC

	// Ajustes: correcciones de lotes ya enviados que se liquidan con este. No cuentan en
	// TotalCount; su monto, con signo, ya está en TotalAmount. Positivo devuelve al comercio.
	AdjustmentCount  int   `json:"adjustment_count,omitempty"`
	AdjustmentAmount int64 `json:"adjustment_amount,omitempty"`

	// ReconciledWith es contra qué se concilió el cierre: TERMINAL (los totales del terminal),
	// BACKEND (cerrado sin ellos, por el job o un operador: no se comparó con nada) o NONE
	// (cerrado sin ellos y retenido en disputa para un operador).
	ReconciledWith string `json:"reconciled_with,omitempty"`
}
