package events

// BatchClosedPayload es el payload del evento posnet.settlement.batch-closed.v1
//
// Publicado por: Settlement
// Consumido por: Notification
//
// Se emite cuando Settlement cierra un lote, y otra vez cada vez que un lote en disputa vuelve a
// CLOSED con sus totales recalculados: Trigger dice por qué. Incluye los totales y la cantidad de
// discrepancias detectadas entre el resumen del terminal y el backend. Para un mismo lote, el más
// reciente es el de ClosedAt mayor.
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

	// Trigger es lo que emitió el evento: un cierre (BatchCloseTerminal, BatchCloseAuto,
	// BatchCloseForce) o una revisión de un lote en disputa que volvió a CLOSED
	// (BatchCloseResolveDispute, BatchCloseRelease). Vacío en los eventos anteriores a este campo,
	// que siempre eran cierres.
	Trigger string `json:"trigger,omitempty"`
}

// Valores de BatchClosedPayload.Trigger.
const (
	BatchCloseTerminal = "terminal_close" // El terminal pidió el cierre
	BatchCloseAuto     = "auto_close"     // El cierre automático, pasado el día
	BatchCloseForce    = "force_close"    // Un operador, con force-close
	// BatchCloseResolveDispute: un operador aceptó los totales del backend de un lote en disputa.
	BatchCloseResolveDispute = "resolve_dispute"
	// BatchCloseRelease: un lote en disputa volvió a conciliar solo, al cerrarse su última
	// revisión de reversa o al llegar una transacción tardía.
	BatchCloseRelease = "release"
)

// IsRevision indica que el evento corrige un cierre anterior del mismo lote.
func (p BatchClosedPayload) IsRevision() bool {
	return p.Trigger == BatchCloseResolveDispute || p.Trigger == BatchCloseRelease
}
