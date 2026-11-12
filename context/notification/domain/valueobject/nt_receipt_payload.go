package valueobject

import "fmt"

// ReceiptPayload contiene los datos estructurados del comprobante de la transacción.
// Es inmutable una vez construido — se serializa a JSON para enviarlo al terminal
// y al webhook del comercio.
type ReceiptPayload struct {
	TransactionID   string
	MerchantName    string
	MerchantAddress string
	TerminalCode    string
	Result          string // "APPROVED" | "REJECTED" | "REVERSED" | "BATCH_CLOSED" | "BATCH_REVISED" | "SETTLEMENT_COMPLETED"
	AuthCode        string // Solo si Result == "APPROVED"
	RejectionCode   string // Solo si Result == "REJECTED"
	RejectionReason string // Solo si Result == "REJECTED"
	AmountCents     int64
	Currency        string
	CardLast4       string
	CardNetwork     string
	EntryMode       string
	TransactionAt   string // RFC3339 UTC

	// Batch es el detalle de un cierre de lote (BATCH_CLOSED o BATCH_REVISED). Nil en los demás.
	Batch *BatchReceipt `json:",omitempty"`
	// Settlement es el detalle de una liquidación diaria (SETTLEMENT_COMPLETED). Nil en los demás.
	Settlement *SettlementReceipt `json:",omitempty"`
}

// Resultados de los comprobantes que no son de una transacción.
const (
	ResultBatchClosed  = "BATCH_CLOSED"
	ResultBatchRevised = "BATCH_REVISED" // Un lote en disputa volvió a CLOSED con otros totales
	// ResultSettlementCompleted es el resumen de liquidación diaria de un comercio, en una moneda.
	ResultSettlementCompleted = "SETTLEMENT_COMPLETED"
)

// BatchReceipt detalla un cierre de lote. AmountCents del comprobante es el total, con los ajustes.
type BatchReceipt struct {
	BatchDate        string // YYYY-MM-DD
	TerminalID       string
	TotalCount       int // Compras y reversas; los ajustes no cuentan
	Discrepancies    int // 0 si coincide con el terminal
	AdjustmentCount  int
	AdjustmentAmount int64 // Con signo: positivo devuelve al comercio
	ReconciledWith   string
	Trigger          string // Por qué se emitió: ver events.BatchClose*
}

// SettlementReceipt detalla una revisión del resumen de liquidación diaria. AmountCents del
// comprobante es el neto acumulado del día.
type SettlementReceipt struct {
	SettlementDate   string // YYYY-MM-DD
	Revision         int    // 1 en el primer resumen; uno más por cada lote liquidado después
	TotalBatches     int
	GrossAmount      int64 // Acumulado, con los ajustes
	AdjustmentAmount int64
	NetAmount        int64
	MDRPercent       float64
	DeltaBatches     int // Lo que agrega esta revisión
	DeltaAmount      int64
	DeltaNetAmount   int64
}

// NewReceiptPayload crea un ReceiptPayload validando los campos requeridos.
func NewReceiptPayload(
	transactionID, merchantName, terminalCode string,
	result string,
	amountCents int64,
	currency, cardLast4, cardNetwork, entryMode, transactionAt string,
) (ReceiptPayload, error) {
	if transactionID == "" {
		return ReceiptPayload{}, fmt.Errorf("receipt_payload: transaction_id cannot be empty")
	}
	if result != "APPROVED" && result != "REJECTED" && result != "REVERSED" {
		return ReceiptPayload{}, fmt.Errorf("receipt_payload: invalid result %q", result)
	}
	if amountCents <= 0 {
		return ReceiptPayload{}, fmt.Errorf("receipt_payload: amount_cents must be positive")
	}
	return ReceiptPayload{
		TransactionID: transactionID,
		MerchantName:  merchantName,
		TerminalCode:  terminalCode,
		Result:        result,
		AmountCents:   amountCents,
		Currency:      currency,
		CardLast4:     cardLast4,
		CardNetwork:   cardNetwork,
		EntryMode:     entryMode,
		TransactionAt: transactionAt,
	}, nil
}

// IsApproved indica si el comprobante es de una transacción aprobada.
func (r ReceiptPayload) IsApproved() bool { return r.Result == "APPROVED" }
