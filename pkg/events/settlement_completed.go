package events

// SettlementCompletedPayload es el payload del evento posnet.settlement.completed.v1
//
// Publicado por: Settlement
// Consumido por: Notification
//
// Se emite cuando la liquidación diaria fue procesada exitosamente
// por el procesador externo (Visa/MC) para un comercio.
// NetAmount = TotalAmount - (TotalAmount * MDRPercent / 100)
type SettlementCompletedPayload struct {
	MerchantID     string  `json:"merchant_id"`
	SettlementDate string  `json:"settlement_date"` // "2025-06-04"
	TotalBatches   int     `json:"total_batches"`   // Cantidad de lotes liquidados
	TotalAmount    int64   `json:"total_amount"`    // Monto bruto en centavos, con los ajustes
	Currency       string  `json:"currency"`
	NetAmount      int64   `json:"net_amount"`   // Monto neto después de MDR (en centavos)
	MDRPercent     float64 `json:"mdr_percent"`  // Comisión aplicada (ej: 2.5)
	CompletedAt    string  `json:"completed_at"` // RFC3339 UTC

	// AdjustmentAmount es la suma, con signo, de los ajustes que ya están en TotalAmount:
	// correcciones de lotes de días anteriores. Positivo devuelve al comercio.
	AdjustmentAmount int64 `json:"adjustment_amount,omitempty"`
}
