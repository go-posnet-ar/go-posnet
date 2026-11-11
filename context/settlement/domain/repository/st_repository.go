// Package repository define los puertos de salida del BC Settlement.
package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ErrStaleBatch indica que el batch cambió desde que se cargó. Nada se guardó: hay
// que volver a cargarlo y reintentar.
var ErrStaleBatch = errors.New("settlement batch was modified concurrently")

// SettlementBatchRepository persiste y recupera SettlementBatches.
type SettlementBatchRepository interface {
	// Save persiste un batch nuevo o actualiza uno existente (UPSERT).
	// Retorna ErrStaleBatch si otro proceso lo modificó desde que se cargó.
	Save(ctx context.Context, batch *aggregate.SettlementBatch) error

	// SaveTx hace lo mismo dentro de una transacción existente, para guardar el batch
	// junto con su evento en el outbox. Si la transacción no confirma, hay que descartar
	// el batch y volver a cargarlo: su versión en memoria ya avanzó.
	SaveTx(ctx context.Context, tx pgx.Tx, batch *aggregate.SettlementBatch) error

	// MerchantDayBatchesTx devuelve los batches con transacciones de un comercio en una
	// fecha. Espera a que terminen las demás llamadas por ese comercio y fecha, y mantiene
	// la exclusión hasta el fin de tx: quien llama ve lo que confirmaron las anteriores.
	MerchantDayBatchesTx(ctx context.Context, tx pgx.Tx, merchantID domain.MerchantID, date time.Time) ([]MerchantDayBatch, error)

	// FindByID recupera un batch por su ID.
	FindByID(ctx context.Context, id string) (*aggregate.SettlementBatch, error)

	// FindOpenByTerminal recupera el batch OPEN del terminal en una fecha y moneda.
	// Retorna nil si no existe — se creará uno nuevo al recibir la primera transacción.
	FindOpenByTerminal(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency string) (*aggregate.SettlementBatch, error)

	// FindOrCreateOpenTx recupera el batch OPEN del terminal o lo crea si no existe, dentro de tx:
	// si tx se revierte, el lote creado también.
	// Garantiza que hay un solo batch OPEN por terminal, día y moneda. El lote viene sin sus
	// transacciones, para agregarle una: no puede calcular totales ni cerrarse.
	FindOrCreateOpenTx(ctx context.Context, tx pgx.Tx, terminalID domain.TerminalID, merchantID domain.MerchantID, date time.Time, currency string) (*aggregate.SettlementBatch, error)

	// ListByMerchantDate lista todos los batches de un comercio en una fecha.
	// Usado para generar el reporte de liquidación diaria.
	ListByMerchantDate(ctx context.Context, merchantID domain.MerchantID, date time.Time) ([]*aggregate.SettlementBatch, error)

	// FindStaleOpenIDs lista los batches OPEN de días de negocio anteriores al de now, en
	// la zona de cada comercio o, si no tiene una, en defaultTimezone.
	// Usado por el job de cierre automático.
	FindStaleOpenIDs(ctx context.Context, now time.Time, defaultTimezone string) ([]string, error)

	// ClosedTotalsForTerminalDay suma los totales de los lotes ya cerrados del terminal en
	// esa fecha y moneda, sin contar excludeBatchID. Los lotes en DISPUTED no cuentan: sus
	// totales no están conciliados.
	ClosedTotalsForTerminalDay(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency, excludeBatchID string) (count int, amountCents int64, err error)
}

// MerchantDayBatch resume un batch para decidir si se completó la liquidación diaria.
type MerchantDayBatch struct {
	Currency         string
	Settled          bool
	TotalAmount      int64 // Con los ajustes
	AdjustmentAmount int64
}

// DailyCompletion es una revisión de settlement.completed ya emitida: lo que se le informó al
// comercio para ese día y moneda.
type DailyCompletion struct {
	MerchantID       domain.MerchantID
	Date             time.Time
	Currency         string
	Revision         int
	TotalBatches     int
	TotalAmount      int64
	AdjustmentAmount int64
	NetAmount        *int64 // Nil en las filas que completó 000016: el MDR de entonces no se guardaba
	EventID          string // Vacío en las filas que completó 000016
}

// SettlementCompletions guarda las revisiones de settlement.completed emitidas, para numerar la
// siguiente y calcular su diferencia. Se usa dentro del advisory lock de MerchantDayBatchesTx.
type SettlementCompletions interface {
	// LastCompletionTx devuelve la última revisión del comercio, día y moneda, o nil.
	LastCompletionTx(ctx context.Context, tx pgx.Tx, merchantID domain.MerchantID, date time.Time, currency string) (*DailyCompletion, error)
	// InsertCompletionTx guarda una revisión nueva.
	InsertCompletionTx(ctx context.Context, tx pgx.Tx, c DailyCompletion) error
}

// Acciones de operador que quedan en la auditoría.
const (
	AuditActionForceClose      = "FORCE_CLOSE"
	AuditActionResubmit        = "RESUBMIT"
	AuditActionResubmitBlocked = "RESUBMIT_BLOCKED"
	AuditActionReopen          = "REOPEN"
	AuditActionResolveDispute  = "RESOLVE_DISPUTE"
	AuditActionSettle          = "SETTLE"
	AuditActionRecoverReversal = "RECOVER_REVERSAL"
	AuditActionRedate          = "REDATE"        // El cambio de zona del comercio movió el lote a otra fecha
	AuditActionAutoRelease     = "AUTO_RELEASE"  // Se cerró la última revisión y el lote concilia: pasa a CLOSED
	AuditActionAdjustment      = "ADJUSTMENT"    // El lote recibió la corrección de un lote ya enviado
	AuditActionLateSale        = "LATE_SALE"     // Llegó tarde una venta que el terminal había contado al cerrar
	AuditActionLateReversal    = "LATE_REVERSAL" // Llegó tarde una reversa que el terminal había contado al cerrar
	AuditActionVoid            = "VOID"          // Lote CLOSED sin transacciones, anulado por la migración 000014

	AuditActionReviewAccept            = "REVIEW_ACCEPT"
	AuditActionReviewUsePurchaseAmount = "REVIEW_USE_PURCHASE_AMOUNT"
	AuditActionReviewExclude           = "REVIEW_EXCLUDE"
)

// BatchAuditEntry registra un cambio de estado hecho por un operador. OperatorID lo
// declara quien llama: no prueba su identidad.
type BatchAuditEntry struct {
	BatchID               string
	Action                string
	OperatorID            string
	Reason                string // Vacío si la operación no lo pide
	FromState             string
	ToState               string
	PreviousDiscrepancies int
	TotalAmount           *int64 // Nil si el batch no tiene totales después del cambio
	ConfirmationID        string // Vacío si no hubo envío al procesador
}

// BatchAuditLog guarda la auditoría de operador. Se escribe en la misma transacción que
// el cambio: sin registro, el cambio no se confirma.
type BatchAuditLog interface {
	InsertAuditTx(ctx context.Context, tx pgx.Tx, entry BatchAuditEntry) error
}

// StoredAuditEntry es un registro de auditoría guardado.
type StoredAuditEntry struct {
	BatchAuditEntry
	ID        string
	CreatedAt time.Time
}

// BatchAuditReader lee la auditoría de un lote. Es un puerto aparte porque solo lo usa la API.
type BatchAuditReader interface {
	// ListBatchAudit devuelve los registros del lote, del más viejo al más nuevo, hasta limit.
	ListBatchAudit(ctx context.Context, batchID string, limit int) ([]StoredAuditEntry, error)
}

// BatchReopens informa si un operador reabrió un lote: es lo que permite que un terminal lo
// vuelva a cerrar aunque sea de un día anterior a ayer.
type BatchReopens interface {
	WasReopened(ctx context.Context, batchID string) (bool, error)
}

// TerminalDayBatches busca los lotes de un terminal en un día, para las ventas, reversas y cierres
// que llegan tarde.
type TerminalDayBatches interface {
	// FindLateBatchTx devuelve el lote DISPUTED del terminal, fecha y moneda al que le falta una
	// venta o reversa ocurrida en at: concilió contra el terminal, cerró después de esa hora y el
	// terminal contó más transacciones de las que tiene. Vacío si no hay ninguno.
	FindLateBatchTx(ctx context.Context, tx pgx.Tx, terminalID domain.TerminalID, date time.Time, currency string, at time.Time) (string, error)

	// CountTerminalDayBatches cuenta los lotes del terminal en esa fecha y moneda, en cualquier estado.
	CountTerminalDayBatches(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency string) (int, error)
}

// PendingClose son los totales de un cierre del terminal que no encontró ningún lote del día.
type PendingClose struct {
	TerminalID     domain.TerminalID
	BatchDate      time.Time
	Currency       string
	TerminalCount  int
	TerminalAmount int64
	EventID        string
	RequestedAt    time.Time
}

// PendingTerminalCloses guarda los cierres sin lote, para que el cierre automático concilie
// contra ellos el lote que formen las ventas que lleguen después.
type PendingTerminalCloses interface {
	// SavePendingClose guarda los totales. Si ya hay unos del mismo día, quedan los del pedido
	// más nuevo.
	SavePendingClose(ctx context.Context, p PendingClose) error

	// FindPendingClose devuelve los totales guardados del terminal en esa fecha y moneda, o nil.
	FindPendingClose(ctx context.Context, terminalID domain.TerminalID, date time.Time, currency string) (*PendingClose, error)

	// DeletePendingCloseTx borra los totales al usarlos, o cuando un cierre más nuevo los reemplaza.
	DeletePendingCloseTx(ctx context.Context, tx pgx.Tx, terminalID domain.TerminalID, date time.Time, currency string) error
}

// SubmissionTotals son los totales con los que se envía un lote: los que el procesador recibe.
type SubmissionTotals struct {
	TotalCount       int
	TotalAmount      int64 // Centavos, con los ajustes
	AdjustmentCount  int
	AdjustmentAmount int64 // Centavos, con signo
}

// ErrSubmittedTotalsChanged indica que el lote ya se envió con otros totales. No se envía otra vez:
// el procesador podría tener la remesa anterior aceptada, y un segundo envío podría pagarse dos
// veces. El lote queda retenido hasta que un operador lo resuelva con el procesador.
var ErrSubmittedTotalsChanged = errors.New("batch totals changed since it was sent to the processor")

// SubmittedTotalsChangedError detalla ErrSubmittedTotalsChanged.
type SubmittedTotalsChangedError struct {
	Sent           SubmissionTotals
	Current        SubmissionTotals
	ConfirmationID string // Vacío si el envío anterior no tiene resultado conocido
}

func (e *SubmittedTotalsChangedError) Error() string {
	return fmt.Sprintf("%s: sent count=%d amount=%d adjustments=%d/%d, now count=%d amount=%d adjustments=%d/%d",
		ErrSubmittedTotalsChanged.Error(),
		e.Sent.TotalCount, e.Sent.TotalAmount, e.Sent.AdjustmentCount, e.Sent.AdjustmentAmount,
		e.Current.TotalCount, e.Current.TotalAmount, e.Current.AdjustmentCount, e.Current.AdjustmentAmount,
	)
}

func (e *SubmittedTotalsChangedError) Unwrap() error { return ErrSubmittedTotalsChanged }

// BatchSubmissions registra los envíos al procesador aparte del lote, para no enviar dos veces uno
// que el procesador ya aceptó: si falla guardar el lote en SUBMITTED, la confirmación queda.
type BatchSubmissions interface {
	// StartSubmission registra un intento de envío, confirmado antes de llamar al procesador.
	// Devuelve la confirmación de un envío anterior aceptado, o vacío si no hay ninguno.
	//
	// El primer intento guarda los totales. Si un intento anterior, con o sin confirmación, se hizo
	// con otros totales, no registra nada y devuelve *SubmittedTotalsChangedError: el procesador
	// puede tener aceptado el envío anterior.
	StartSubmission(ctx context.Context, batchID string, totals SubmissionTotals) (confirmationID string, err error)

	// ConfirmSubmission guarda la confirmación del procesador. No reemplaza una anterior.
	ConfirmSubmission(ctx context.Context, batchID, confirmationID string) error
}

// Motivos por los que una reversa queda en revisión.
const (
	ReversalReviewPurchaseNotFound = "PURCHASE_NOT_FOUND"
	ReversalReviewMerchantMismatch = "MERCHANT_MISMATCH"
	ReversalReviewCurrencyMismatch = "CURRENCY_MISMATCH"
	ReversalReviewAmountMismatch   = "AMOUNT_MISMATCH"
)

// Formas de cerrar una revisión.
const (
	// ReviewActionAccept deja la reversa como está: el monto del evento es correcto.
	ReviewActionAccept = "ACCEPT"
	// ReviewActionUsePurchaseAmount cambia el monto de la reversa por el de la compra.
	ReviewActionUsePurchaseAmount = "USE_PURCHASE_AMOUNT"
	// ReviewActionExclude quita la reversa del lote: no debe descontarse.
	ReviewActionExclude = "EXCLUDE"
)

// ReversalPurchase es la compra registrada a la que apunta una reversa.
type ReversalPurchase struct {
	MerchantID  string
	AmountCents int64
	Currency    string
}

// ReversalReview es una reversa que no coincide con su compra. La reversa se registra
// igual: la revisión la deja a la vista. Purchase es nil si la compra no está.
type ReversalReview struct {
	TransactionID string
	BatchID       string
	Reason        string
	MerchantID    string
	AmountCents   int64
	Currency      string
	Purchase      *ReversalPurchase
}

// StoredReversalReview es una revisión guardada, con el estado actual de su lote.
type StoredReversalReview struct {
	ReversalReview
	BatchState    string
	Discrepancies int
	Resolved      bool
	CreatedAt     time.Time
	// Action es la última acción de un operador, o RESTORE si el sistema restituyó una reversa
	// excluida. Con ReviewActionExclude, la reversa no está en ningún lote.
	Action string
}

// ReviewActionRestore registra que el sistema restituyó una reversa excluida al llegar su compra.
const ReviewActionRestore = "RESTORE"

// ReviewReconciliation es lo que pasó con la revisión de una transacción al llegar su compra.
type ReviewReconciliation struct {
	// Reason es el motivo si la revisión sigue o vuelve a quedar abierta; vacío si no hay.
	Reason string
	// Resolved indica que la compra coincide y la revisión se cerró. BatchID es el lote que
	// retenía, que puede liberarse.
	Resolved bool
	BatchID  string
	// Restored indica que la reversa estaba excluida y se restituyó en el lote de la compra.
	Restored bool
	// Reopened indica que la reversa estaba excluida, la compra no coincide, y la revisión se
	// reabrió en el lote de la compra.
	Reopened bool
}

// ReversalReviewLog valida las reversas contra sus compras y gestiona sus revisiones.
type ReversalReviewLog interface {
	// FindPurchaseTx devuelve la compra de la transacción, o nil si no está registrada.
	// Espera a que termine cualquier otra compra o reversa de la misma transacción: quien
	// llama ve lo que esa confirmó.
	FindPurchaseTx(ctx context.Context, tx pgx.Tx, transactionID string) (*ReversalPurchase, error)

	// InsertReversalReviewTx abre la revisión. Una transacción tiene una sola revisión.
	InsertReversalReviewTx(ctx context.Context, tx pgx.Tx, review ReversalReview) error

	// ReconcileReversalReviewTx compara la compra recién llegada con la reversa que la esperaba.
	//   - Revisión abierta: la cierra si coinciden o cambia el motivo si no.
	//   - Reversa excluida por un operador sin la compra: si coinciden, la restituye en
	//     purchaseBatchID, el lote de la compra, y cierra la revisión; si no, la reabre en ese
	//     lote, que queda retenido hasta que un operador decida.
	// Se serializa con FindPurchaseTx por transacción.
	ReconcileReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID, purchaseBatchID string, purchase ReversalPurchase) (ReviewReconciliation, error)

	// CountOpenReversalReviews cuenta las revisiones abiertas de las reversas de un lote.
	CountOpenReversalReviews(ctx context.Context, batchID string) (int, error)

	// FindReleasableHeldBatchIDs lista los lotes DISPUTED que solo retenían revisiones de reversa,
	// ya todas cerradas: sin discrepancias, con la referencia de conciliación guardada y con al
	// menos una revisión. Son los que ReleaseHeldBatch debería haber liberado.
	FindReleasableHeldBatchIDs(ctx context.Context) ([]string, error)

	// ListOpenReversalReviews lista las revisiones abiertas, de la más antigua a la más nueva.
	ListOpenReversalReviews(ctx context.Context, limit int) ([]StoredReversalReview, error)

	// LockReversalReviewTx bloquea el lote y la revisión hasta el fin de tx, y la devuelve.
	// Retorna nil si no existe.
	LockReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID string) (*StoredReversalReview, error)

	// UpdateReversalAmountTx cambia el monto de la reversa en su lote. Avanza la versión del
	// lote: quien lo haya cargado antes recibe ErrStaleBatch y no pisa el cambio.
	UpdateReversalAmountTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string, amountCents int64) error

	// DeleteReversalTx quita la reversa de su lote. Avanza la versión, igual que UpdateReversalAmountTx.
	DeleteReversalTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string) error

	// InsertReversalTx vuelve a agregar una reversa excluida a un lote. Avanza la versión.
	InsertReversalTx(ctx context.Context, tx pgx.Tx, batchID, transactionID string, amountCents int64, currency string) error

	// CloseReversalReviewTx cierra la revisión con el operador, la resolución y la acción indicados.
	// Sobre una revisión ya cerrada, la reemplaza: es la corrección de un lote ya enviado.
	CloseReversalReviewTx(ctx context.Context, tx pgx.Tx, transactionID, operatorID, resolution, action string) error
}

// MerchantTimezones guarda la zona del día de negocio de cada comercio.
type MerchantTimezones interface {
	// MerchantTimezone devuelve la zona del comercio, o vacío si no tiene una propia.
	MerchantTimezone(ctx context.Context, merchantID string) (string, error)

	// MerchantTimezoneTx devuelve la zona como MerchantTimezone, pero antes toma, en tx, un lock
	// compartido del comercio que SetMerchantTimezone espera. Una venta o reversa que lee la zona
	// así termina de guardarse en su lote antes de que el cambio mueva los lotes, o lee la zona nueva.
	MerchantTimezoneTx(ctx context.Context, tx pgx.Tx, merchantID string) (string, error)

	// SetMerchantTimezone guarda la zona y, en la misma transacción, mueve los lotes OPEN del
	// comercio fechados hoy en la zona anterior a hoy en la nueva, con su auditoría.
	// defaultTimezone es la zona anterior si el comercio no tenía una propia. Retorna
	// ErrUnknownTimezone si Postgres no la reconoce: las consultas por día de negocio fallarían
	// para todos los comercios. Retorna ErrOpenBatchConflict si un terminal ya tiene un lote
	// OPEN en la fecha nueva; en los dos casos no cambia nada.
	SetMerchantTimezone(ctx context.Context, merchantID, timezone, defaultTimezone, operatorID string) ([]RedatedBatch, error)
}

// RedatedBatch es un lote que el cambio de zona movió de fecha.
type RedatedBatch struct {
	BatchID  string
	FromDate string
	ToDate   string
}

// ErrUnknownTimezone indica que la base de datos no reconoce la zona horaria.
var ErrUnknownTimezone = errors.New("timezone is not known to the database")

// ErrOpenBatchConflict indica que mover un lote a su fecha nueva chocaría con otro lote OPEN
// del mismo terminal, fecha y moneda.
var ErrOpenBatchConflict = errors.New("an open batch already exists for the new business date")

// BatchAttentionCounts son los lotes que requieren atención de un operador.
type BatchAttentionCounts struct {
	Disputed          int64 // En DISPUTED
	ClosedUnsent      int64 // En CLOSED, con transacciones, desde antes del corte
	TotalsChanged     int64 // En CLOSED, enviados antes con otros totales: no se reenvían solos
	OpenOverdue       int64 // En OPEN, de un día de negocio anterior
	OutboxStuck       int64 // Eventos en el outbox desde antes del corte
	ReversalsInReview int64 // Revisiones de reversas abiertas desde antes del corte
	RecoveryPending   bool  // Falta registrar la recuperación de reversas anteriores
}

// MaintenanceReversalRecovery es la tarea de settlement-recover-reversals en maintenance_runs.
const MaintenanceReversalRecovery = "reversal-recovery"

// BatchAttentionCounter cuenta los lotes que requieren atención. Es un puerto aparte
// porque solo lo usan las métricas.
type BatchAttentionCounter interface {
	// El vencimiento de los lotes OPEN se calcula con el día de negocio de now, en la zona
	// de cada comercio o, si no tiene una, en defaultTimezone.
	CountBatchesNeedingAttention(ctx context.Context, now time.Time, defaultTimezone string, closedBefore, outboxBefore, reviewBefore time.Time) (BatchAttentionCounts, error)
}
