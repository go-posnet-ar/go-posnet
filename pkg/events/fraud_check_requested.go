package events

// FraudCheckRequestedPayload es el payload del evento posnet.fraud.check-requested.v1
//
// Publicado por: Authorization
// Consumido por: Fraud Detection
//
// Authorization publica este evento antes de llamar al adquirente.
// Fraud Detection responde con FraudScoreCalculatedPayload.
// Si Fraud Detection no responde dentro de FRAUD_CHECK_TIMEOUT (5 s por
// defecto), Authorization rechaza la transacción con FRAUD_TIMEOUT.
type FraudCheckRequestedPayload struct {
	TransactionID string `json:"transaction_id"`
	TerminalID    string `json:"terminal_id"`
	MerchantID    string `json:"merchant_id"`
	AmountCents   int64  `json:"amount_cents"`
	Currency      string `json:"currency"`
	CardNetwork   string `json:"card_network"`
	EntryMode     string `json:"entry_mode"`  // CHIP | CONTACTLESS | MAGSTRIPE
	OccurredAt    string `json:"occurred_at"` // RFC3339 UTC — timestamp de la transacción original
}
