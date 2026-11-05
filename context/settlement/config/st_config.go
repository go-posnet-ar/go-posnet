// Package config centraliza la configuración del BC Settlement.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config contiene toda la configuración del BC Settlement.
type Config struct {
	GRPCPort int
	HTTPPort int

	Postgres   PostgresConfig
	NATS       NATSConfig
	OTEL       OTELConfig
	Settlement SettlementConfig
}

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

// SettlementConfig contiene parámetros operativos del proceso de liquidación.
type SettlementConfig struct {
	// MDRPercent es la comisión porcentual cobrada al comercio (Merchant Discount Rate).
	// Ej: 2.5 = 2.5%. Se aplica al calcular el NetAmount en SettlementCompleted.
	MDRPercent float64

	// BatchCloseHour es la hora UTC desde la que corre el cierre automático, que
	// alcanza a los lotes OPEN de días de negocio anteriores. Default: 3 = 00:00 en
	// Argentina. Debe ser posterior a la medianoche de BusinessTimezone.
	// Si es 0, el cierre es solo por pedido del terminal o de un operador.
	BatchCloseHour int

	// SubmitRetries es la cantidad de reintentos al enviar el archivo de remesa
	// al procesador externo antes de marcar el batch como DISPUTED.
	SubmitRetries int

	// SubmitTimeout es el timeout para la llamada al procesador externo.
	SubmitTimeout time.Duration

	// Processor selecciona el procesador externo: ProcessorNone o ProcessorMock.
	Processor string

	// BusinessTimezone es la zona IANA que define a qué día pertenece una transacción.
	BusinessTimezone string

	// AdminAPIKey es el token que exigen las operaciones de escritura de la API HTTP.
	// Vacío = esas operaciones quedan deshabilitadas.
	AdminAPIKey string
}

// minAdminAPIKeyLength rechaza tokens triviales al arrancar.
const minAdminAPIKeyLength = 32

const (
	// ProcessorNone no envía los lotes: quedan en CLOSED. Es el valor seguro por defecto.
	ProcessorNone = "none"
	// ProcessorMock simula el envío y los lotes pasan a SUBMITTED. Solo para desarrollo.
	ProcessorMock = "mock"
)

// Load carga la configuración desde variables de entorno.
func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort: envInt("GRPC_PORT", 9093),
		HTTPPort: envInt("HTTP_PORT", 8083),

		Postgres: PostgresConfig{
			DSN:             requireEnv("POSTGRES_DSN"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 15)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", 30*time.Minute),
			MaxConnIdleTime: envDuration("POSTGRES_MAX_CONN_IDLE", 5*time.Minute),
			MigrationsDir:   envStr("MIGRATIONS_DIR", "migrations/settlement"),
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
			ServiceName:    envStr("OTEL_SERVICE_NAME", "posnet-settlement"),
			ServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
			OTLPEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
			Environment:    envStr("ENVIRONMENT", "development"),
		},

		Settlement: SettlementConfig{
			MDRPercent:     envFloat("SETTLEMENT_MDR_PERCENT", 2.5),
			BatchCloseHour: envInt("SETTLEMENT_BATCH_CLOSE_HOUR", 3),
			SubmitRetries:  envInt("SETTLEMENT_SUBMIT_RETRIES", 3),
			SubmitTimeout:  envDuration("SETTLEMENT_SUBMIT_TIMEOUT", 30*time.Second),
			Processor:      envStr("SETTLEMENT_PROCESSOR", ProcessorNone),

			BusinessTimezone: envStr("SETTLEMENT_BUSINESS_TIMEZONE", "America/Argentina/Buenos_Aires"),
			AdminAPIKey:      envStr("SETTLEMENT_ADMIN_API_KEY", ""),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Settlement.MDRPercent < 0 || c.Settlement.MDRPercent > 10 {
		return fmt.Errorf("config: SETTLEMENT_MDR_PERCENT %.2f out of range [0, 10]",
			c.Settlement.MDRPercent)
	}
	if c.Settlement.BatchCloseHour < 0 || c.Settlement.BatchCloseHour > 23 {
		return fmt.Errorf("config: SETTLEMENT_BATCH_CLOSE_HOUR %d out of range [0, 23]",
			c.Settlement.BatchCloseHour)
	}
	switch c.Settlement.Processor {
	case "", ProcessorNone, ProcessorMock:
	default:
		return fmt.Errorf("config: SETTLEMENT_PROCESSOR %q must be %q or %q",
			c.Settlement.Processor, ProcessorNone, ProcessorMock)
	}
	if key := c.Settlement.AdminAPIKey; key != "" && len(key) < minAdminAPIKeyLength {
		return fmt.Errorf("config: SETTLEMENT_ADMIN_API_KEY must have at least %d characters", minAdminAPIKeyLength)
	}
	if _, err := time.LoadLocation(c.Settlement.BusinessTimezone); err != nil {
		return fmt.Errorf("config: SETTLEMENT_BUSINESS_TIMEZONE %q is invalid: %w", c.Settlement.BusinessTimezone, err)
	}
	return nil
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

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

func envFloat(key string, defaultVal float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
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
