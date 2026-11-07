package valueobject

import (
	"fmt"

	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── RejectionCode ───────────────────────────────────────────────────────────

// RejectionCode encapsula el código de respuesta ISO 8583 del banco emisor
// junto con metadatos útiles para el sistema (retryable, descripción).
type RejectionCode struct {
	code   string
	source RejectionSource
}

// RejectionSource indica quién generó el rechazo.
type RejectionSource string

const (
	SourceAcquirer   RejectionSource = "ACQUIRER"   // Rechazo del banco emisor (código ISO 8583)
	SourceFraud      RejectionSource = "FRAUD"      // Motor antifraude interno
	SourceTimeout    RejectionSource = "TIMEOUT"    // Sin respuesta del adquirente
	SourceValidation RejectionSource = "VALIDATION" // Validación local del terminal/backend
	SourceBlocklist  RejectionSource = "BLOCKLIST"  // Tarjeta en la blocklist interna
)

// Códigos ISO 8583 comunes
const (
	ISO_APPROVED            = "00"
	ISO_CAPTURE_CARD        = domain.ISOCaptureCard
	ISO_DO_NOT_HONOR        = "05"
	ISO_INVALID_TRANSACTION = "12"
	ISO_INVALID_AMOUNT      = "13"
	ISO_CARD_NOT_FOUND      = "14"
	ISO_FORMAT_ERROR        = "30"
	ISO_LOST_CARD           = domain.ISOLostCard
	ISO_STOLEN_CARD         = domain.ISOStolenCard
	ISO_INSUFFICIENT_FUNDS  = "51"
	ISO_EXPIRED_CARD        = "54"
	ISO_INCORRECT_PIN       = "55"
	ISO_NOT_PERMITTED       = "57"
	ISO_SUSPECTED_FRAUD     = "59"
	ISO_EXCEEDS_LIMIT       = "61"
	ISO_RESTRICTED_CARD     = "62"
	ISO_SECURITY_VIOLATION  = "63"
	ISO_ISSUER_UNAVAILABLE  = "91"
	ISO_SYSTEM_MALFUNCTION  = "96"
)

// codeTimeout es el código de una transacción sin respuesta del adquirente, ya revertida.
const codeTimeout = "TIMEOUT"

// CodeInvalidData es el código de una transacción descartada por datos inválidos.
const CodeInvalidData = "INVALID_DATA"

// retryableCodes son códigos donde tiene sentido reintentar la transacción.
var retryableCodes = map[string]bool{
	ISO_ISSUER_UNAVAILABLE: true,
	ISO_SYSTEM_MALFUNCTION: true,
	// El conciliador ya revirtió la operación: reintentar no cobra dos veces.
	codeTimeout: true,
}

// isoDescriptions mapea código → descripción legible para logs y comprobantes.
var isoDescriptions = map[string]string{
	ISO_CAPTURE_CARD:        "Capture Card",
	ISO_DO_NOT_HONOR:        "Do Not Honor",
	ISO_INVALID_TRANSACTION: "Invalid Transaction",
	ISO_INVALID_AMOUNT:      "Invalid Amount",
	ISO_CARD_NOT_FOUND:      "Card Not Found",
	ISO_FORMAT_ERROR:        "Format Error",
	ISO_LOST_CARD:           "Lost Card - Pick Up",
	ISO_STOLEN_CARD:         "Stolen Card - Pick Up",
	ISO_INSUFFICIENT_FUNDS:  "Insufficient Funds",
	ISO_EXPIRED_CARD:        "Expired Card",
	ISO_INCORRECT_PIN:       "Incorrect PIN",
	ISO_NOT_PERMITTED:       "Transaction Not Permitted",
	ISO_SUSPECTED_FRAUD:     "Suspected Fraud",
	ISO_EXCEEDS_LIMIT:       "Exceeds Withdrawal Limit",
	ISO_RESTRICTED_CARD:     "Restricted Card",
	ISO_SECURITY_VIOLATION:  "Security Violation",
	ISO_ISSUER_UNAVAILABLE:  "Issuer Unavailable",
	ISO_SYSTEM_MALFUNCTION:  "System Malfunction",

	domain.CodeCardBlocked: "Card Blocked - Pick Up",
	codeTimeout:            "No Response From Acquirer - Reversed",
	CodeInvalidData:        "Invalid Transaction Data",
}

// NewRejectionFromISO crea un RejectionCode a partir de un código ISO 8583.
func NewRejectionFromISO(isoCode string) (RejectionCode, error) {
	if isoCode == "" {
		return RejectionCode{}, fmt.Errorf("rejection_code: iso code cannot be empty")
	}
	return RejectionCode{code: isoCode, source: SourceAcquirer}, nil
}

// NewRejectionFromFraud crea un RejectionCode para rechazos del motor antifraude.
func NewRejectionFromFraud() RejectionCode {
	return RejectionCode{code: "FRAUD_REJECTED", source: SourceFraud}
}

// NewRejectionFromTimeout crea un RejectionCode para timeouts del adquirente.
func NewRejectionFromTimeout() RejectionCode {
	return RejectionCode{code: codeTimeout, source: SourceTimeout}
}

// NewRejectionFromBlocklist crea un RejectionCode para tarjetas bloqueadas.
//
// Es una fuente propia y no VALIDATION porque arrastra autoridad del emisor:
// la tarjeta entró a la blocklist por un 41/43 previo, así que el rechazo
// vuelve a ordenar la retención del plástico.
func NewRejectionFromBlocklist() RejectionCode {
	return RejectionCode{code: domain.CodeCardBlocked, source: SourceBlocklist}
}

// NewRejectionFromValidation crea un RejectionCode para errores de validación local.
func NewRejectionFromValidation(reason string) RejectionCode {
	return RejectionCode{code: reason, source: SourceValidation}
}

// RestoreRejectionCode reconstruye un RejectionCode guardado, con su código y su fuente. Una
// fuente ausente o desconocida se lee como ACQUIRER.
func RestoreRejectionCode(code, source string) RejectionCode {
	switch s := RejectionSource(source); s {
	case SourceFraud, SourceTimeout, SourceValidation, SourceBlocklist:
		return RejectionCode{code: code, source: s}
	default:
		return RejectionCode{code: code, source: SourceAcquirer}
	}
}

func (r RejectionCode) Code() string            { return r.code }
func (r RejectionCode) Source() RejectionSource { return r.source }

// Description devuelve la descripción legible del código de rechazo.
func (r RejectionCode) Description() string {
	if desc, ok := isoDescriptions[r.code]; ok {
		return desc
	}
	return fmt.Sprintf("Rejection code: %s", r.code)
}

// RequiresCardCapture indica si el terminal debe retener la tarjeta
// ("pick-up card") en lugar de devolverla al portador.
//
// Solo un rechazo del adquirente puede ordenar la retención: los rechazos
// locales (antifraude, timeout, validación) no tienen autoridad del emisor
// para retener el plástico, aunque su código coincida por casualidad.
func (r RejectionCode) RequiresCardCapture() bool {
	switch r.source {
	case SourceAcquirer:
		return domain.RequiresCardCapture(r.code)
	case SourceBlocklist:
		// El bloqueo se originó en un 41/43 del emisor; esa orden sigue vigente.
		return true
	default:
		return false
	}
}

// IsRetryable indica si tiene sentido reintentar la transacción.
func (r RejectionCode) IsRetryable() bool {
	return retryableCodes[r.code]
}

func (r RejectionCode) String() string {
	return fmt.Sprintf("%s(%s)", r.source, r.code)
}

// ─── FraudDecision ───────────────────────────────────────────────────────────

// FraudDecision encapsula el resultado del análisis del motor antifraude.
type FraudDecision struct {
	Score    int      // 0–100
	Decision string   // APPROVE | REJECT | REVIEW
	RulesHit []string // IDs de reglas que activaron
}

const (
	FraudDecisionApprove = "APPROVE"
	FraudDecisionReject  = "REJECT"
	FraudDecisionReview  = "REVIEW"
)

// NewFraudDecision crea un FraudDecision validando el rango del score.
func NewFraudDecision(score int, decision string, rulesHit []string) (FraudDecision, error) {
	if score < 0 || score > 100 {
		return FraudDecision{}, fmt.Errorf("fraud_decision: score %d out of range [0,100]", score)
	}
	switch decision {
	case FraudDecisionApprove, FraudDecisionReject, FraudDecisionReview:
	default:
		return FraudDecision{}, fmt.Errorf("fraud_decision: unknown decision %q", decision)
	}
	return FraudDecision{Score: score, Decision: decision, RulesHit: rulesHit}, nil
}

// ShouldReject indica si la transacción debe ser rechazada.
func (f FraudDecision) ShouldReject() bool { return f.Decision == FraudDecisionReject }

// IsZero indica si el FraudDecision no fue inicializado (aún no evaluado).
func (f FraudDecision) IsZero() bool { return f.Decision == "" }
