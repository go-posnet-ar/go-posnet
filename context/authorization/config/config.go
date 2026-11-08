// Package config centraliza la configuración del BC Authorization.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	GRPCPort int
	HTTPPort int
	Postgres PostgresConfig
	NATS     NATSConfig
	OTEL     OTELConfig
	Acquirer AcquirerConfig

	// InvalidTransactionsRetention es cuánto se conserva el registro de las transacciones
	// rechazadas por datos inválidos. 0 desactiva la purga.
	InvalidTransactionsRetention time.Duration

	// FraudCheckTimeout es cuánto se espera el score de Fraud Detection, desde que llega la
	// transacción. Después, se rechaza con FRAUD_TIMEOUT.
	FraudCheckTimeout time.Duration
}

// defaultFraudCheckTimeout deja margen para los dos saltos por NATS y los relays de outbox, que
// suman unos cientos de milisegundos, más la evaluación, sin hacer esperar al cliente de más.
const defaultFraudCheckTimeout = 5 * time.Second

// defaultInvalidTransactionsRetention son 90 días: ese registro sirve para soporte e incidentes
// de integración, no para analítica histórica.
const defaultInvalidTransactionsRetention = 90 * 24 * time.Hour

type PostgresConfig struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	MigrationsDir   string
}

type NATSConfig struct {
	URL           string
	NKeyPath      string
	TLSCertPath   string
	TLSKeyPath    string
	TLSCAPath     string
	MaxReconnect  int
	ReconnectWait time.Duration
}

type OTELConfig struct {
	ServiceName    string
	ServiceVersion string
	OTLPEndpoint   string
	Environment    string
}

type AcquirerConfig struct {
	Host string
	Port int
	// TLS campos opcionales — en dev pueden estar vacíos o ser "none"
	TLSCertPath    string
	TLSKeyPath     string
	TLSCAPath      string
	TimeoutSeconds int

	// Mode elige el adquirente: mock o iso8583.
	Mode string
	// InstitutionID es el DE-32 de los mensajes ISO 8583.
	InstitutionID string
	// MaxConcurrent limita las llamadas simultáneas al adquirente.
	MaxConcurrent int
	// MockScenarios hace que el mock responda según el monto. Solo para desarrollo.
	MockScenarios bool
	// BreakerFailures es cuántas llamadas seguidas sin respuesta abren el circuit breaker. 0 lo
	// desactiva.
	BreakerFailures int
	// BreakerCooldown es cuánto rechaza el breaker abierto antes de probar con una llamada.
	BreakerCooldown time.Duration

	// ReconcileEvery es cada cuánto corre el conciliador. 0 lo desactiva.
	ReconcileEvery time.Duration
	// StuckAfter es cuánto puede seguir una transacción en PROCESSING antes de que el
	// conciliador la tome. Debe superar TimeoutSeconds: si no, revertiría llamadas en curso.
	StuckAfter time.Duration
}

// Adquirentes disponibles.
const (
	AcquirerModeMock    = "mock"
	AcquirerModeISO8583 = "iso8583"
)

// TLSEnabled indica si hay configuración TLS real (no vacío ni "none").
func (a AcquirerConfig) TLSEnabled() bool {
	return a.TLSCertPath != "" && a.TLSCertPath != "none" &&
		a.TLSKeyPath != "" && a.TLSKeyPath != "none"
}

func Load() (*Config, error) {
	retention, err := loadInvalidTransactionsRetention()
	if err != nil {
		return nil, err
	}
	fraudTimeout, err := loadFraudCheckTimeout()
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		InvalidTransactionsRetention: retention,
		FraudCheckTimeout:            fraudTimeout,

		GRPCPort: envInt("GRPC_PORT", 9090),
		HTTPPort: envInt("HTTP_PORT", 8080),

		Postgres: PostgresConfig{
			DSN:             requireEnv("POSTGRES_DSN"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 20)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", 30*time.Minute),
			MaxConnIdleTime: envDuration("POSTGRES_MAX_CONN_IDLE", 5*time.Minute),
			MigrationsDir:   envStr("MIGRATIONS_DIR", "migrations/authorization"),
		},

		NATS: NATSConfig{
			URL:           requireEnv("NATS_URL"),
			NKeyPath:      envStr("NATS_NKEY_PATH", ""),
			TLSCertPath:   envStr("NATS_TLS_CERT", ""),
			TLSKeyPath:    envStr("NATS_TLS_KEY", ""),
			TLSCAPath:     envStr("NATS_TLS_CA", ""),
			MaxReconnect:  -1,
			ReconnectWait: 2 * time.Second,
		},

		OTEL: OTELConfig{
			ServiceName:    envStr("OTEL_SERVICE_NAME", "posnet-authorization"),
			ServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
			OTLPEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
			Environment:    envStr("ENVIRONMENT", "development"),
		},

		Acquirer: AcquirerConfig{
			Host: envStr("ACQUIRER_HOST", "localhost"),
			Port: envInt("ACQUIRER_PORT", 9100),
			// Opcionales — no hacen panic si están vacíos
			TLSCertPath:     envStr("ACQUIRER_TLS_CERT", ""),
			TLSKeyPath:      envStr("ACQUIRER_TLS_KEY", ""),
			TLSCAPath:       envStr("ACQUIRER_TLS_CA", ""),
			TimeoutSeconds:  envInt("ACQUIRER_TIMEOUT_SECONDS", 30),
			Mode:            envStr("ACQUIRER_MODE", AcquirerModeMock),
			InstitutionID:   envStr("ACQUIRER_INSTITUTION_ID", "000001"),
			MaxConcurrent:   envInt("ACQUIRER_MAX_CONCURRENT", 20),
			MockScenarios:   envStr("ACQUIRER_MOCK_SCENARIOS", "") == "true",
			ReconcileEvery:  envDuration("ACQUIRER_RECONCILE_EVERY", 30*time.Second),
			StuckAfter:      envDuration("ACQUIRER_STUCK_AFTER", 2*time.Minute),
			BreakerFailures: envInt("ACQUIRER_BREAKER_FAILURES", 5),
			BreakerCooldown: envDuration("ACQUIRER_BREAKER_COOLDOWN", 30*time.Second),
		},
	}

	if timeout := time.Duration(cfg.Acquirer.TimeoutSeconds) * time.Second; cfg.Acquirer.StuckAfter <= timeout {
		return nil, fmt.Errorf("config: ACQUIRER_STUCK_AFTER %s must be greater than ACQUIRER_TIMEOUT_SECONDS (%s): calls in progress would be reversed",
			cfg.Acquirer.StuckAfter, timeout)
	}
	switch cfg.Acquirer.Mode {
	case AcquirerModeMock:
	case AcquirerModeISO8583:
		// Hoy los datos de la tarjeta los resuelve un stub con tarjetas de prueba.
		if cfg.OTEL.Environment == "production" {
			return nil, fmt.Errorf("config: ACQUIRER_MODE=%s uses test cards and cannot be used with ENVIRONMENT=production", AcquirerModeISO8583)
		}
	default:
		return nil, fmt.Errorf("config: ACQUIRER_MODE %q must be %q or %q", cfg.Acquirer.Mode, AcquirerModeMock, AcquirerModeISO8583)
	}
	if cfg.Acquirer.TimeoutSeconds < 1 {
		return nil, fmt.Errorf("config: ACQUIRER_TIMEOUT_SECONDS %d must be at least 1", cfg.Acquirer.TimeoutSeconds)
	}
	if cfg.Acquirer.MaxConcurrent < 1 {
		return nil, fmt.Errorf("config: ACQUIRER_MAX_CONCURRENT %d must be at least 1", cfg.Acquirer.MaxConcurrent)
	}
	if cfg.Acquirer.MockScenarios && cfg.OTEL.Environment == "production" {
		return nil, fmt.Errorf("config: ACQUIRER_MOCK_SCENARIOS cannot be used with ENVIRONMENT=production")
	}
	if cfg.Acquirer.BreakerFailures < 0 {
		return nil, fmt.Errorf("config: ACQUIRER_BREAKER_FAILURES %d cannot be negative", cfg.Acquirer.BreakerFailures)
	}
	if cfg.Acquirer.BreakerFailures > 0 && cfg.Acquirer.BreakerCooldown <= 0 {
		return nil, fmt.Errorf("config: ACQUIRER_BREAKER_COOLDOWN %s must be positive", cfg.Acquirer.BreakerCooldown)
	}
	if cfg.Acquirer.ReconcileEvery < 0 {
		return nil, fmt.Errorf("config: ACQUIRER_RECONCILE_EVERY %s cannot be negative", cfg.Acquirer.ReconcileEvery)
	}

	return cfg, nil
}

// loadInvalidTransactionsRetention no ignora un valor inválido, como envDuration: "30d" no es
// una duración de Go, y conservar los datos más o menos tiempo del pedido tiene que notarse.
func loadInvalidTransactionsRetention() (time.Duration, error) {
	const key = "INVALID_TRANSACTIONS_RETENTION"
	v := os.Getenv(key)
	if v == "" {
		return defaultInvalidTransactionsRetention, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s %q is not a valid duration: use hours, like 720h for 30 days", key, v)
	}
	if d < 0 {
		return 0, fmt.Errorf("config: %s %s cannot be negative", key, d)
	}
	return d, nil
}

// loadFraudCheckTimeout no ignora un valor inválido: sin un timeout, una transacción sin score
// quedaría esperando para siempre.
func loadFraudCheckTimeout() (time.Duration, error) {
	const key = "FRAUD_CHECK_TIMEOUT"
	v := os.Getenv(key)
	if v == "" {
		return defaultFraudCheckTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s %q is not a valid duration, like 5s", key, v)
	}
	if d < time.Second {
		return 0, fmt.Errorf("config: %s %s must be at least 1s: the score crosses NATS twice", key, d)
	}
	return d, nil
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("config: required environment variable %q is not set", key))
	}
	return v
}

func envStr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func envDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}
