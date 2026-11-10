// Package aggregate contiene los Aggregates del BC Settlement.
package aggregate

import (
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/entity"
	"github.com/juantevez/go-posnet/context/settlement/domain/event"
	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// SettlementBatch es el Aggregate Root del BC Settlement.
// Representa el lote de transacciones de un terminal en un día.
// Agrupa las transacciones aprobadas y gestiona el ciclo de liquidación.
type SettlementBatch struct {
	id         string
	terminalID domain.TerminalID
	merchantID domain.MerchantID
	batchDate  time.Time // Solo la fecha — truncada a día
	state      valueobject.BatchState
	currency   string

	// Transacciones incluidas en el lote
	transactions []*entity.BatchTransaction

	// Resumen calculado al cierre (nil mientras OPEN)
	summary *valueobject.BatchSummary

	// Timestamps
	createdAt   time.Time
	closedAt    *time.Time
	submittedAt *time.Time
	settledAt   *time.Time

	// Discrepancias detectadas en la conciliación
	discrepancies int

	// reconciledWith dice contra qué se concilió el cierre, para poder repetirlo al cerrar una
	// revisión de reversa. terminalCount y terminalAmount son los totales del terminal, solo con
	// ReconciledWithTerminal.
	reconciledWith string
	terminalCount  *int
	terminalAmount *int64

	// Versión persistida, para el bloqueo optimista
	version int

	// Eventos pendientes
	domainEvents []event.DomainEvent
}

// NewSettlementBatch crea un lote nuevo en estado OPEN.
func NewSettlementBatch(
	terminalID domain.TerminalID,
	merchantID domain.MerchantID,
	batchDate time.Time,
	currency string,
) (*SettlementBatch, error) {
	if terminalID.IsZero() {
		return nil, fmt.Errorf("settlement_batch: terminal_id cannot be zero")
	}
	if merchantID.IsZero() {
		return nil, fmt.Errorf("settlement_batch: merchant_id cannot be zero")
	}
	if currency == "" {
		return nil, fmt.Errorf("settlement_batch: currency cannot be empty")
	}

	id := domain.NewTransactionID().String()
	b := &SettlementBatch{
		id:         id,
		terminalID: terminalID,
		merchantID: merchantID,
		batchDate:  batchDate.Truncate(24 * time.Hour),
		state:      valueobject.BatchStateOpen,
		currency:   currency,
		createdAt:  time.Now().UTC(),
	}

	b.record(event.NewBatchOpened(id, terminalID, merchantID, batchDate))
	return b, nil
}

// ─── Mutaciones ───────────────────────────────────────────────────────────────

// AddTransaction agrega una transacción aprobada al lote.
// Solo acepta transacciones mientras el lote está en estado OPEN.
func (b *SettlementBatch) AddTransaction(
	transactionID domain.TransactionID,
	amountCents int64,
	txType valueobject.BatchTransactionType,
) error {
	if b.state != valueobject.BatchStateOpen {
		return fmt.Errorf("settlement_batch %s: cannot add transaction in state %s", b.id, b.state)
	}

	tx, err := entity.NewBatchTransaction(b.id, transactionID, amountCents, b.currency, txType)
	if err != nil {
		return fmt.Errorf("settlement_batch: create batch transaction: %w", err)
	}

	b.transactions = append(b.transactions, tx)
	return nil
}

// RemoveTransaction descuenta una reversión del lote con su monto real. La compra
// original puede estar en este lote o en uno anterior: la reversa se registra en el
// lote abierto al momento de completarse.
func (b *SettlementBatch) RemoveTransaction(transactionID domain.TransactionID, amountCents int64) error {
	if b.state != valueobject.BatchStateOpen {
		return fmt.Errorf("settlement_batch %s: cannot remove transaction in state %s", b.id, b.state)
	}

	tx, err := entity.NewBatchTransaction(b.id, transactionID, amountCents, b.currency, valueobject.BatchTxReversal)
	if err != nil {
		return err
	}
	b.transactions = append(b.transactions, tx)
	return nil
}

// AddAdjustment agrega la corrección de un lote ya enviado, con signo. Solo en un lote OPEN.
func (b *SettlementBatch) AddAdjustment(transactionID domain.TransactionID, amountCents int64) error {
	return b.AddTransaction(transactionID, amountCents, valueobject.BatchTxAdjustment)
}

// RequestClose inicia el proceso de cierre — transiciona a PENDING_CLOSE.
func (b *SettlementBatch) RequestClose() error {
	if err := b.transition(valueobject.BatchStatePendingClose); err != nil {
		return err
	}
	b.record(event.NewBatchCloseRequested(b.id, b.terminalID, b.merchantID))
	return nil
}

// Close cierra el lote calculando los totales y registrando discrepancias.
// terminalCount y terminalAmount son los totales reportados por el terminal
// para comparar contra los del backend.
func (b *SettlementBatch) Close(terminalCount int, terminalAmount int64) error {
	if err := b.transition(valueobject.BatchStateClosed); err != nil {
		return err
	}

	summary, backendTotalCount, backendTotalAmount, err := b.calculateSummary()
	if err != nil {
		return err
	}

	// Detectar discrepancias. countDiff mide el desajuste de cantidad; un
	// desajuste de monto por sí solo también debe contar como discrepancia
	// aunque countDiff sea 0, o quedaría enmascarado para el caller que
	// solo chequea Discrepancies() > 0.
	b.discrepancies = discrepancyCount(terminalCount, terminalAmount, backendTotalCount, backendTotalAmount)

	b.summary = &summary
	now := time.Now().UTC()
	b.closedAt = &now
	b.reconciledWith = ReconciledWithTerminal
	b.terminalCount, b.terminalAmount = &terminalCount, &terminalAmount

	b.record(event.NewBatchClosed(b.id, b.terminalID, b.merchantID, summary, b.discrepancies))
	return nil
}

// CloseWithBackendTotals cierra un lote OPEN sin totales del terminal, usando los
// del backend. No hay conciliación posible, así que no registra discrepancias.
func (b *SettlementBatch) CloseWithBackendTotals() error {
	if err := b.RequestClose(); err != nil {
		return err
	}
	_, count, amount, err := b.calculateSummary()
	if err != nil {
		return err
	}
	if err := b.Close(count, amount); err != nil {
		return err
	}
	b.reconciledWith, b.terminalCount, b.terminalAmount = ReconciledWithBackend, nil, nil
	return nil
}

// Reopen devuelve un lote en disputa a OPEN para que acepte transacciones y
// pueda volver a cerrarse. Descarta los totales del cierre anterior.
func (b *SettlementBatch) Reopen() error {
	if err := b.transition(valueobject.BatchStateOpen); err != nil {
		return err
	}
	b.summary = nil
	b.closedAt = nil
	b.discrepancies = 0
	b.reconciledWith, b.terminalCount, b.terminalAmount = "", nil, nil
	return nil
}

// ResolveDispute cierra un lote en disputa aceptando los totales del backend,
// recalculados a partir de las transacciones actuales del lote. Es una decisión
// del operador: los totales del terminal dejan de compararse.
func (b *SettlementBatch) ResolveDispute() error {
	summary, _, _, err := b.calculateSummary()
	if err != nil {
		return err
	}
	if err := b.transition(valueobject.BatchStateClosed); err != nil {
		return err
	}
	b.summary = &summary
	b.discrepancies = 0
	now := time.Now().UTC()
	b.closedAt = &now
	// El operador aceptó los totales del backend: son la referencia desde ahora.
	b.reconciledWith = ReconciledWithBackend
	return nil
}

// Contra qué se concilió un cierre. Un lote cerrado antes de que se guardara no tiene ninguno.
const (
	ReconciledWithTerminal = "TERMINAL" // Los totales que informó el terminal
	ReconciledWithBackend  = "BACKEND"  // Sin terminal: el job, force-close o resolve-dispute
)

// Release vuelve a conciliar un lote DISPUTED con sus transacciones actuales. Se usa cuando se
// cierra su última revisión de reversa, que pudo cambiar los totales.
//
//   - Conciliado contra el terminal: compara con los totales que informó. Si coinciden, pasa a
//     CLOSED; si no, sigue DISPUTED con las discrepancias recalculadas.
//   - Conciliado contra el backend: no hay con qué comparar, y pasa a CLOSED.
//   - Sin referencia, de antes de que se guardara: no hace nada. Lo resuelve un operador.
//
// changed indica que el lote cambió y hay que guardarlo.
func (b *SettlementBatch) Release() (released, changed bool, err error) {
	if b.state != valueobject.BatchStateDisputed || b.reconciledWith == "" {
		return false, false, nil
	}
	summary, count, amount, err := b.calculateSummary()
	if err != nil {
		return false, false, err
	}
	discrepancies := 0
	if b.reconciledWith == ReconciledWithTerminal && b.terminalCount != nil && b.terminalAmount != nil {
		discrepancies = discrepancyCount(*b.terminalCount, *b.terminalAmount, count, amount)
	}
	if discrepancies > 0 {
		changed = discrepancies != b.discrepancies || b.summary == nil || *b.summary != summary
		b.summary, b.discrepancies = &summary, discrepancies
		return false, changed, nil
	}
	if err := b.transition(valueobject.BatchStateClosed); err != nil {
		return false, false, err
	}
	b.summary, b.discrepancies = &summary, 0
	now := time.Now().UTC()
	b.closedAt = &now
	return true, true, nil
}

// AddLateSale agrega a un lote DISPUTED una venta que llegó después de su cierre, si el terminal
// la había contado: el lote se concilió contra el terminal, la venta se autorizó antes del cierre
// y al backend le faltan transacciones. Recalcula el resumen y las discrepancias, sin cambiar de
// estado: si ahora coincide, el lote se libera con Release. Devuelve false, sin cambiar nada, si
// la venta no corresponde a este lote; entonces abre uno nuevo, como cualquier venta posterior.
func (b *SettlementBatch) AddLateSale(transactionID domain.TransactionID, amountCents int64, authorizedAt time.Time) (bool, error) {
	if b.state != valueobject.BatchStateDisputed || b.reconciledWith != ReconciledWithTerminal ||
		b.terminalCount == nil || b.terminalAmount == nil || b.closedAt == nil || authorizedAt.After(*b.closedAt) {
		return false, nil
	}
	_, count, _, err := b.calculateSummary()
	if err != nil {
		return false, err
	}
	if count >= *b.terminalCount {
		return false, nil // El terminal no contó más ventas de las que hay: esta no es suya
	}
	for _, t := range b.transactions {
		if t.TransactionID() == transactionID && t.Type() == valueobject.BatchTxPurchase {
			return false, nil
		}
	}

	tx, err := entity.NewBatchTransaction(b.id, transactionID, amountCents, b.currency, valueobject.BatchTxPurchase)
	if err != nil {
		return false, fmt.Errorf("settlement_batch: create batch transaction: %w", err)
	}
	b.transactions = append(b.transactions, tx)
	summary, count, amount, err := b.calculateSummary()
	if err != nil {
		return false, err
	}
	b.summary = &summary
	b.discrepancies = discrepancyCount(*b.terminalCount, *b.terminalAmount, count, amount)
	return true, nil
}

// discrepancyCount compara los totales del terminal con los del backend: 0 si coinciden; si no,
// la diferencia de cantidad, o 1 si solo difiere el monto. Un desajuste de monto tiene que
// contar, o quedaría enmascarado para quien solo mira Discrepancies() > 0.
func discrepancyCount(terminalCount int, terminalAmount int64, count int, amount int64) int {
	if terminalCount == count && terminalAmount == amount {
		return 0
	}
	return max(abs(terminalCount-count), 1)
}

// BackendTotals devuelve la cantidad y el monto neto de las transacciones actuales del lote,
// sin los ajustes: es lo que se compara con el terminal.
func (b *SettlementBatch) BackendTotals() (int, int64, error) {
	_, count, amount, err := b.calculateSummary()
	return count, amount, err
}

// calculateSummary calcula los totales del backend a partir de las transacciones. Devuelve el
// resumen, con los ajustes en el monto total, y la cantidad y el monto comparables con el
// terminal, sin los ajustes.
func (b *SettlementBatch) calculateSummary() (valueobject.BatchSummary, int, int64, error) {
	var purchaseCount, reversalCount, adjustmentCount int
	var purchaseAmount, reversalAmount, adjustmentAmount int64

	for _, tx := range b.transactions {
		switch tx.Type() {
		case valueobject.BatchTxPurchase, valueobject.BatchTxOffline:
			purchaseCount++
			purchaseAmount += tx.AmountCents()
		case valueobject.BatchTxReversal:
			reversalCount++
			reversalAmount += tx.AmountCents()
		case valueobject.BatchTxAdjustment:
			adjustmentCount++
			adjustmentAmount += tx.AmountCents()
		}
	}

	totalCount := purchaseCount + reversalCount
	terminalAmount := purchaseAmount - reversalAmount
	totalAmount := terminalAmount + adjustmentAmount

	cur, _ := domain.ParseCurrency(b.currency)
	totalMoney, _ := domain.NewMoney(totalAmount, cur)
	purchaseMoney, _ := domain.NewMoney(purchaseAmount, cur)
	var reversalMoney domain.Money
	if reversalAmount > 0 {
		reversalMoney, _ = domain.NewMoney(reversalAmount, cur)
	}

	summary, err := valueobject.NewBatchSummary(
		totalCount, totalMoney,
		purchaseCount, purchaseMoney,
		reversalCount, reversalMoney,
	)
	if err != nil {
		return valueobject.BatchSummary{}, 0, 0, fmt.Errorf("settlement_batch: calculate summary: %w", err)
	}
	return summary.WithAdjustments(adjustmentCount, adjustmentAmount), totalCount, terminalAmount, nil
}

// Submit marca el lote como enviado al procesador externo.
func (b *SettlementBatch) Submit() error {
	if err := b.transition(valueobject.BatchStateSubmitted); err != nil {
		return err
	}
	now := time.Now().UTC()
	b.submittedAt = &now
	return nil
}

// MarkSettled marca el lote como liquidado exitosamente.
func (b *SettlementBatch) MarkSettled() error {
	if b.summary == nil && b.state.CanTransitionTo(valueobject.BatchStateSettled) {
		return fmt.Errorf("settlement_batch %s: cannot settle without a summary", b.id)
	}
	if err := b.transition(valueobject.BatchStateSettled); err != nil {
		return err
	}
	now := time.Now().UTC()
	b.settledAt = &now
	b.record(event.NewBatchSettled(b.id, b.terminalID, b.merchantID, *b.summary))
	return nil
}

// MarkDisputed marca el lote con discrepancias que requieren intervención manual.
func (b *SettlementBatch) MarkDisputed(reason string) error {
	if err := b.transition(valueobject.BatchStateDisputed); err != nil {
		return err
	}
	b.record(event.NewBatchDisputed(b.id, b.terminalID, reason))
	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func (b *SettlementBatch) transition(next valueobject.BatchState) error {
	if !b.state.CanTransitionTo(next) {
		return fmt.Errorf("settlement_batch %s: invalid transition %s → %s", b.id, b.state, next)
	}
	b.state = next
	return nil
}

func (b *SettlementBatch) record(e event.DomainEvent) {
	b.domainEvents = append(b.domainEvents, e)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// MarkPersisted registra que el repositorio guardó esta versión.
func (b *SettlementBatch) MarkPersisted() {
	b.version++
}

// ─── Getters ──────────────────────────────────────────────────────────────────

func (b *SettlementBatch) ID() string                               { return b.id }
func (b *SettlementBatch) TerminalID() domain.TerminalID            { return b.terminalID }
func (b *SettlementBatch) MerchantID() domain.MerchantID            { return b.merchantID }
func (b *SettlementBatch) BatchDate() time.Time                     { return b.batchDate }
func (b *SettlementBatch) State() valueobject.BatchState            { return b.state }
func (b *SettlementBatch) Currency() string                         { return b.currency }
func (b *SettlementBatch) Transactions() []*entity.BatchTransaction { return b.transactions }
func (b *SettlementBatch) Summary() *valueobject.BatchSummary       { return b.summary }
func (b *SettlementBatch) Discrepancies() int                       { return b.discrepancies }
func (b *SettlementBatch) ReconciledWith() string                   { return b.reconciledWith }
func (b *SettlementBatch) TerminalTotals() (*int, *int64)           { return b.terminalCount, b.terminalAmount }
func (b *SettlementBatch) CreatedAt() time.Time                     { return b.createdAt }
func (b *SettlementBatch) ClosedAt() *time.Time                     { return b.closedAt }
func (b *SettlementBatch) SubmittedAt() *time.Time                  { return b.submittedAt }
func (b *SettlementBatch) SettledAt() *time.Time                    { return b.settledAt }
func (b *SettlementBatch) Version() int                             { return b.version }
func (b *SettlementBatch) DomainEvents() []event.DomainEvent        { return b.domainEvents }
func (b *SettlementBatch) ClearDomainEvents()                       { b.domainEvents = nil }
