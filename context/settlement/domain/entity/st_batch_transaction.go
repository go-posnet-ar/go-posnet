// Package entity contiene las entidades del BC Settlement.
package entity

import (
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// BatchTransaction representa una transacción incluida dentro de un lote.
// No es un Aggregate Root — pertenece al SettlementBatch.
// Tiene identidad propia dentro del lote.
type BatchTransaction struct {
	id            string
	batchID       string
	transactionID domain.TransactionID
	amountCents   int64
	currency      string
	txType        valueobject.BatchTransactionType
	includedAt    time.Time
}

// NewBatchTransaction crea una BatchTransaction validando sus invariantes.
func NewBatchTransaction(
	batchID string,
	transactionID domain.TransactionID,
	amountCents int64,
	currency string,
	txType valueobject.BatchTransactionType,
) (*BatchTransaction, error) {
	if batchID == "" {
		return nil, fmt.Errorf("batch_transaction: batch_id cannot be empty")
	}
	if transactionID.IsZero() {
		return nil, fmt.Errorf("batch_transaction: transaction_id cannot be zero")
	}
	// Un ajuste tiene signo; el resto de los movimientos, no.
	if txType == valueobject.BatchTxAdjustment {
		if amountCents == 0 {
			return nil, fmt.Errorf("batch_transaction: an adjustment cannot be zero")
		}
	} else if amountCents <= 0 {
		return nil, fmt.Errorf("batch_transaction: amount_cents must be positive")
	}
	return &BatchTransaction{
		id:            domain.NewTransactionID().String(), // UUID propio del BatchTransaction
		batchID:       batchID,
		transactionID: transactionID,
		amountCents:   amountCents,
		currency:      currency,
		txType:        txType,
		includedAt:    time.Now().UTC(),
	}, nil
}

// ReconstituteBatchTransaction reconstruye desde Postgres.
func ReconstituteBatchTransaction(
	id, batchID string,
	transactionID domain.TransactionID,
	amountCents int64,
	currency string,
	txType valueobject.BatchTransactionType,
	includedAt time.Time,
) *BatchTransaction {
	return &BatchTransaction{
		id:            id,
		batchID:       batchID,
		transactionID: transactionID,
		amountCents:   amountCents,
		currency:      currency,
		txType:        txType,
		includedAt:    includedAt,
	}
}

func (t *BatchTransaction) ID() string                             { return t.id }
func (t *BatchTransaction) BatchID() string                        { return t.batchID }
func (t *BatchTransaction) TransactionID() domain.TransactionID    { return t.transactionID }
func (t *BatchTransaction) AmountCents() int64                     { return t.amountCents }
func (t *BatchTransaction) Currency() string                       { return t.currency }
func (t *BatchTransaction) Type() valueobject.BatchTransactionType { return t.txType }
func (t *BatchTransaction) IncludedAt() time.Time                  { return t.includedAt }
