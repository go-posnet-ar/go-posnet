package valueobject

import (
	"fmt"

	"github.com/juantevez/go-posnet/pkg/domain"
)

// BatchSummary es el resumen financiero de un lote.
// Se calcula al cierre y es inmutable una vez calculado.
type BatchSummary struct {
	totalCount     int
	totalAmount    domain.Money
	purchaseCount  int
	purchaseAmount domain.Money
	reversalCount  int
	reversalAmount domain.Money
	// Ajustes de lotes ya enviados. totalAmount los incluye; totalCount no, porque el terminal
	// no los conoce.
	adjustmentCount  int
	adjustmentAmount int64
}

// NewBatchSummary crea un BatchSummary validando consistencia de los totales.
func NewBatchSummary(
	totalCount int,
	totalAmount domain.Money,
	purchaseCount int,
	purchaseAmount domain.Money,
	reversalCount int,
	reversalAmount domain.Money,
) (BatchSummary, error) {
	if totalCount < 0 {
		return BatchSummary{}, fmt.Errorf("batch_summary: total_count cannot be negative")
	}
	if purchaseCount+reversalCount != totalCount {
		return BatchSummary{}, fmt.Errorf(
			"batch_summary: purchase_count(%d) + reversal_count(%d) != total_count(%d)",
			purchaseCount, reversalCount, totalCount,
		)
	}
	return BatchSummary{
		totalCount:     totalCount,
		totalAmount:    totalAmount,
		purchaseCount:  purchaseCount,
		purchaseAmount: purchaseAmount,
		reversalCount:  reversalCount,
		reversalAmount: reversalAmount,
	}, nil
}

func (s BatchSummary) TotalCount() int              { return s.totalCount }
func (s BatchSummary) TotalAmount() domain.Money    { return s.totalAmount }
func (s BatchSummary) PurchaseCount() int           { return s.purchaseCount }
func (s BatchSummary) PurchaseAmount() domain.Money { return s.purchaseAmount }
func (s BatchSummary) ReversalCount() int           { return s.reversalCount }
func (s BatchSummary) ReversalAmount() domain.Money { return s.reversalAmount }
func (s BatchSummary) IsZero() bool                 { return s.totalCount == 0 && s.adjustmentCount == 0 }
func (s BatchSummary) AdjustmentCount() int         { return s.adjustmentCount }

// AdjustmentAmount es la suma de los ajustes, con signo, en centavos.
func (s BatchSummary) AdjustmentAmount() int64 { return s.adjustmentAmount }

// WithAdjustments devuelve el resumen con sus ajustes. totalAmount ya debe incluirlos.
func (s BatchSummary) WithAdjustments(count int, amount int64) BatchSummary {
	s.adjustmentCount, s.adjustmentAmount = count, amount
	return s
}
