package postgres_test

import (
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// anyArgs devuelve n comodines pgxmock.AnyArg(). pgxmock exige que la
// expectation declare exactamente la misma cantidad de argumentos que la
// llamada real (omitir WithArgs equivale a esperar cero argumentos), así que
// los tests que no verifican valores puntuales igual deben indicar cuántos
// argumentos esperar.
func anyArgs(n int) []any {
	args := make([]any, n)
	for i := range args {
		args[i] = pgxmock.AnyArg()
	}
	return args
}

// newMockPool crea un pool pgxmock y registra su cierre y la verificación de
// expectations al finalizar el test.
func newMockPool(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := pool.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet pgxmock expectations: %v", err)
		}
	})
	return pool
}

// ─── builders de aggregates ──────────────────────────────────────────────────

func mustMoney(t *testing.T, cents int64) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(cents, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney(%d) error = %v", cents, err)
	}
	return m
}

func mustSTAN(t *testing.T, v int) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(v)
	if err != nil {
		t.Fatalf("NewSTAN(%d) error = %v", v, err)
	}
	return s
}

func mustPAN(t *testing.T) domain.PAN {
	t.Helper()
	p, err := domain.NewPAN("1234", domain.NetworkVisa)
	if err != nil {
		t.Fatalf("NewPAN() error = %v", err)
	}
	return p
}

func newValidTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx, err := aggregate.NewTransaction(
		domain.NewTransactionID(),
		domain.NewTerminalID(),
		domain.NewMerchantID(),
		mustMoney(t, 5000),
		mustSTAN(t, 1),
		mustPAN(t),
		valueobject.EntryModeChip,
		domain.CardToken{},
		"emv-data-base64",
		[]byte{0xAA, 0xBB},
	)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	return tx
}

func newApprovedTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newValidTransaction(t)
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	fd, err := valueobject.NewFraudDecision(10, valueobject.FraudDecisionApprove, nil)
	if err != nil {
		t.Fatalf("NewFraudDecision() error = %v", err)
	}
	if err := tx.ApplyFraudDecision(fd); err != nil {
		t.Fatalf("ApplyFraudDecision() error = %v", err)
	}
	ac, err := domain.NewAuthCode("AB1234")
	if err != nil {
		t.Fatalf("NewAuthCode() error = %v", err)
	}
	if err := tx.Approve(ac); err != nil {
		t.Fatalf("Approve() error = %v", err)
	}
	return tx
}

func newRejectedTransaction(t *testing.T) *aggregate.Transaction {
	t.Helper()
	tx := newValidTransaction(t)
	rc, err := valueobject.NewRejectionFromISO(valueobject.ISO_DO_NOT_HONOR)
	if err != nil {
		t.Fatalf("NewRejectionFromISO() error = %v", err)
	}
	if err := tx.Reject(rc); err != nil {
		t.Fatalf("Reject() error = %v", err)
	}
	return tx
}

// ─── fixture de fila de Postgres ──────────────────────────────────────────────

// txColumns replica el orden exacto de columnas de los SELECT en repository.go.
var txColumns = []string{
	"id", "terminal_id", "merchant_id",
	"state", "amount_cents", "currency",
	"pan_last4", "card_network", "entry_mode",
	"stan", "auth_code", "rejection_code", "rejection_source",
	"fraud_score", "fraud_decision", "fraud_rules_hit",
	"emv_data_b64", "iso8583_raw",
	"created_at", "authorized_at", "rejected_at",
	"card_token", "reversal_requested_at",
}

// rowFixture arma los valores de una fila de pn_authorization.transactions.
// Los tipos de cada campo deben calzar exactamente con los destinos de Scan
// en txRow (repository.go): pgxmock no hace conversión de tipos como pgx
// real — un *string debe pasarse como *string, no como string, o el Scan
// falla con "destination kind not supported".
type rowFixture struct {
	id, terminalID, merchantID       string
	state                            string
	amountCents                      int64
	currency                         string
	panLast4, cardNetwork, entryMode string
	stan                             int
	authCode, rejectionCode          *string
	rejectionSource                  *string
	fraudScore                       *int
	fraudDecision                    *string
	fraudRulesHit                    []byte
	emvDataB64                       string
	iso8583Raw                       []byte
	createdAt                        time.Time
	authorizedAt, rejectedAt         *time.Time
	cardToken                        *string
	reversalRequestedAt              *time.Time
}

func newRowFixture(t *testing.T) rowFixture {
	t.Helper()
	return rowFixture{
		id:          domain.NewTransactionID().String(),
		terminalID:  domain.NewTerminalID().String(),
		merchantID:  domain.NewMerchantID().String(),
		state:       "RECEIVED",
		amountCents: 5000,
		currency:    "ARS",
		panLast4:    "1234",
		cardNetwork: "VISA",
		entryMode:   "CHIP",
		stan:        1,
		emvDataB64:  "emv==",
		iso8583Raw:  []byte{0x01, 0x02},
		createdAt:   time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}
}

func (f rowFixture) rows() *pgxmock.Rows {
	return pgxmock.NewRows(txColumns).AddRow(
		f.id, f.terminalID, f.merchantID,
		f.state, f.amountCents, f.currency,
		f.panLast4, f.cardNetwork, f.entryMode,
		f.stan, f.authCode, f.rejectionCode, f.rejectionSource,
		f.fraudScore, f.fraudDecision, f.fraudRulesHit,
		f.emvDataB64, f.iso8583Raw,
		f.createdAt, f.authorizedAt, f.rejectedAt,
		f.cardToken, f.reversalRequestedAt,
	)
}
