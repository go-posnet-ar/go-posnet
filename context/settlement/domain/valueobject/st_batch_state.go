// Package valueobject contiene los Value Objects del BC Settlement.
package valueobject

import "fmt"

// BatchState representa el estado del ciclo de vida de un lote de liquidación.
// Es la máquina de estados central del aggregate SettlementBatch.
type BatchState string

const (
	BatchStateOpen         BatchState = "OPEN"          // Lote abierto — acepta transacciones
	BatchStatePendingClose BatchState = "PENDING_CLOSE" // Cierre solicitado — en proceso
	BatchStateClosed       BatchState = "CLOSED"        // Cerrado — totales calculados
	BatchStateSubmitted    BatchState = "SUBMITTED"     // Enviado al procesador externo
	BatchStateSettled      BatchState = "SETTLED"       // Liquidado — fondos transferidos
	BatchStateDisputed     BatchState = "DISPUTED"      // En disputa — discrepancias detectadas
	// BatchStateVoid es un lote cerrado sin transacciones: no hay nada que enviar ni liquidar.
	// Queda así cuando una revisión excluye su única reversa, o lo crea una reversa de otro día.
	BatchStateVoid BatchState = "VOID"
)

// IsTerminal indica si el estado es final. DISPUTED no lo es: un operador puede
// reabrir el lote o aceptar los totales del backend (ver AdminHandler).
func (s BatchState) IsTerminal() bool {
	return s == BatchStateSettled || s == BatchStateVoid
}

// CanTransitionTo valida si la transición al nuevo estado es válida.
func (s BatchState) CanTransitionTo(next BatchState) bool {
	allowed := map[BatchState][]BatchState{
		BatchStateOpen:         {BatchStatePendingClose},
		BatchStatePendingClose: {BatchStateClosed, BatchStateDisputed},
		BatchStateClosed:       {BatchStateSubmitted, BatchStateDisputed, BatchStateVoid}, // Close() detecta discrepancias y marca disputa de inmediato; Void() si está vacío
		BatchStateSubmitted:    {BatchStateSettled, BatchStateDisputed},
		BatchStateDisputed:     {BatchStateSubmitted, BatchStateOpen, BatchStateClosed}, // Reopen / ResolveDispute
	}
	for _, a := range allowed[s] {
		if a == next {
			return true
		}
	}
	return false
}

func (s BatchState) String() string { return string(s) }

func ParseBatchState(s string) (BatchState, error) {
	switch BatchState(s) {
	case BatchStateOpen, BatchStatePendingClose, BatchStateClosed,
		BatchStateSubmitted, BatchStateSettled, BatchStateDisputed, BatchStateVoid:
		return BatchState(s), nil
	}
	return "", fmt.Errorf("unknown batch state %q", s)
}

// ─── BatchTransactionType ─────────────────────────────────────────────────────

// BatchTransactionType clasifica el tipo de movimiento dentro del lote.
type BatchTransactionType string

const (
	BatchTxPurchase BatchTransactionType = "PURCHASE" // Compra aprobada
	BatchTxReversal BatchTransactionType = "REVERSAL" // Anulación procesada
	// BatchTxAdjustment corrige un lote ya enviado, en el lote abierto siguiente del terminal.
	// Su monto tiene signo: positivo devuelve al comercio, negativo le descuenta. El terminal no
	// lo conoce, así que no entra en la conciliación.
	BatchTxAdjustment BatchTransactionType = "ADJUSTMENT"
)

func ParseBatchTransactionType(s string) (BatchTransactionType, error) {
	switch BatchTransactionType(s) {
	case BatchTxPurchase, BatchTxReversal, BatchTxAdjustment:
		return BatchTransactionType(s), nil
	}
	return "", fmt.Errorf("unknown batch transaction type %q", s)
}

func (t BatchTransactionType) String() string { return string(t) }
