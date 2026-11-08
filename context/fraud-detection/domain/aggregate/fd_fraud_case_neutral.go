package aggregate

import (
	"fmt"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// Reglas sintéticas: no están en fraud_rules, pero quedan en las evaluaciones del caso para que
// GET /fraud-cases muestre por qué el score no salió de las reglas.
const (
	// RuleBypass es el score neutral de un motor que falló.
	RuleBypass = "BYPASS"
	// RuleInvalidEvent rechaza un pedido que no se puede evaluar.
	RuleInvalidEvent = "INVALID_EVENT"
	// RuleNoActiveRules registra que no había reglas activas: el control de fraude está apagado.
	RuleNoActiveRules = "NO_ACTIVE_RULES"
)

// BuildNeutralScore aplica un score neutro al FraudCase cuando el motor
// de reglas falla completamente (error de infraestructura).
// Score 50 → decisión REVIEW: la transacción continúa pero queda marcada.
// reason documenta el motivo del bypass para auditoría.
func (fc *FraudCase) BuildNeutralScore(reason string) (valueobject.FraudScore, error) {
	neutralEval, err := valueobject.NewRuleEvaluation(
		RuleBypass, "Engine Bypass", true, 50,
		"rule engine failed: "+reason,
	)
	if err != nil {
		return valueobject.FraudScore{}, err
	}

	if err := fc.ApplyEvaluations([]valueobject.RuleEvaluation{neutralEval}); err != nil {
		return valueobject.FraudScore{}, err
	}

	return fc.score, nil
}

// ApplyNoActiveRules evalúa el caso sin reglas: score 0, APPROVE. Desactivar todas las reglas
// apaga el control de fraude; la evaluación NO_ACTIVE_RULES deja constancia en el caso.
func (fc *FraudCase) ApplyNoActiveRules() error {
	eval, err := valueobject.NewRuleEvaluation(
		RuleNoActiveRules, "No Active Rules", false, 0,
		"no active fraud rules — fraud check disabled",
	)
	if err != nil {
		return err
	}
	return fc.ApplyEvaluations([]valueobject.RuleEvaluation{eval})
}

// InvalidEventParams son los datos de un pedido que no se puede evaluar. Los campos inválidos
// llegan vacíos: el caso guarda solo lo que se pudo leer.
type InvalidEventParams struct {
	TransactionID domain.TransactionID
	TerminalID    domain.TerminalID // vacío si era inválido
	MerchantID    domain.MerchantID // vacío si era inválido
	AmountCents   int64
	Currency      string
	CardNetwork   string
	EntryMode     string
	OccurredAt    time.Time
	Reason        string // qué campos fallaron
}

// NewInvalidEventCase crea el caso de un pedido que no se puede evaluar, ya evaluado con score
// 100: REJECT. Sin respuesta, Authorization esperaría su timeout y rechazaría con un código de
// reintento; reintentar no lo arregla. Solo exige el transaction_id, que es a quién responder.
func NewInvalidEventCase(p InvalidEventParams) (*FraudCase, error) {
	if p.TransactionID.IsZero() {
		return nil, fmt.Errorf("fraud_case: transaction_id cannot be zero")
	}
	fc := &FraudCase{
		id:            domain.NewTransactionID().String(),
		transactionID: p.TransactionID,
		terminalID:    p.TerminalID,
		merchantID:    p.MerchantID,
		amountCents:   p.AmountCents,
		currency:      p.Currency,
		cardNetwork:   p.CardNetwork,
		entryMode:     p.EntryMode,
		occurredAt:    p.OccurredAt,
	}
	eval, err := valueobject.NewRuleEvaluation(
		RuleInvalidEvent, "Invalid Event", true, 100,
		"fraud check request cannot be evaluated: "+p.Reason,
	)
	if err != nil {
		return nil, err
	}
	if err := fc.ApplyEvaluations([]valueobject.RuleEvaluation{eval}); err != nil {
		return nil, err
	}
	return fc, nil
}
