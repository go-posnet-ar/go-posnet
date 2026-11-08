// Package port define los puertos de entrada del BC Fraud Detection.
package port

// FraudService es el puerto de entrada principal.
// El adaptador NATS lo llama al consumir FraudCheckRequested.
type FraudService interface {
	// EvaluateTransaction ejecuta el motor de reglas sobre una transacción
	// y publica FraudScoreCalculated a NATS con el resultado.
	EvaluateTransaction(ctx interface{}, cmd EvaluateTransactionCommand) error
}

// AdminService es el puerto de entrada para operaciones administrativas.
// Expuesto vía HTTP — permite gestionar reglas sin redespliegue.
type AdminService interface {
	// GetFraudCase retorna el resultado del análisis de una transacción.
	GetFraudCase(ctx interface{}, transactionID string) (*FraudCaseResult, error)

	// ListActiveRules retorna todas las reglas activas con sus parámetros actuales.
	ListActiveRules(ctx interface{}) ([]*RuleResult, error)

	// UpdateRuleThreshold actualiza el umbral de una regla sin redespliegue.
	// Los cambios toman efecto en el próximo ciclo de reload del cache.
	UpdateRuleThreshold(ctx interface{}, cmd UpdateRuleThresholdCommand) error
}

// ─── Commands ─────────────────────────────────────────────────────────────────

// EvaluateTransactionCommand contiene los datos del evento FraudCheckRequested.
type EvaluateTransactionCommand struct {
	EventID       string // UUID del evento NATS — clave de idempotencia
	CorrelationID string // correlation_id del envelope: el TransactionID, por si el payload trae uno ilegible
	TransactionID string
	TerminalID    string
	MerchantID    string
	AmountCents   int64
	Currency      string
	CardNetwork   string
	EntryMode     string // CHIP | CONTACTLESS | MAGSTRIPE
	OccurredAt    string // RFC3339 UTC
}

// UpdateRuleThresholdCommand actualiza el umbral de una regla existente.
type UpdateRuleThresholdCommand struct {
	RuleID         string
	NewThreshold   float64
	NewScoreWeight int
}

// ─── Results ──────────────────────────────────────────────────────────────────

// FraudCaseResult es el resultado de la query de un caso de fraude.
type FraudCaseResult struct {
	FraudCaseID   string
	TransactionID string
	Score         int
	Decision      string   // APPROVE | REJECT | REVIEW
	RulesHit      []string // IDs de reglas que activaron
	EvaluatedAt   string   // RFC3339 UTC
	Evaluations   []RuleEvaluationResult
}

// RuleEvaluationResult describe el resultado de una regla individual.
type RuleEvaluationResult struct {
	RuleID            string
	RuleName          string
	Activated         bool
	ScoreContribution int
	Reason            string
}

// RuleResult describe una regla activa del sistema.
type RuleResult struct {
	ID             string
	Name           string
	Description    string
	ScoreWeight    int
	ThresholdValue float64
	IsActive       bool
}
