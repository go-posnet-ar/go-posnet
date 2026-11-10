// Package config centraliza la configuración del BC Settlement.
package config

import (
	"fmt"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contiene toda la configuración del BC Settlement.
type Config struct {
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

	// BatchCloseHour solo activa o desactiva el cierre automático: con 0, el cierre es solo
	// por pedido del terminal o de un operador. Antes era la hora UTC desde la que corría; ya
	// no tiene efecto como hora, y BatchCloseHourDeprecated avisa si se lo configura.
	BatchCloseHour int

	// BatchCloseHourDeprecated indica que SETTLEMENT_BATCH_CLOSE_HOUR está configurada con un
	// valor distinto de 0, que ya no define una hora.
	BatchCloseHourDeprecated bool

	// BatchCloseGrace es cuánto después de la medianoche del comercio se cierra
	// automáticamente el lote del día anterior: el plazo para que el terminal lo cierre él.
	// El job revisa cada 15 minutos todo el día. La alerta de lotes vencidos usa el mismo plazo.
	BatchCloseGrace time.Duration

	// SubmitRetries es la cantidad de reintentos al enviar el archivo de remesa
	// al procesador externo antes de marcar el batch como DISPUTED.
	SubmitRetries int

	// SubmitTimeout es el timeout para la llamada al procesador externo.
	SubmitTimeout time.Duration

	// Processor selecciona el procesador externo: ProcessorNone o ProcessorMock.
	Processor string

	// BusinessTimezone es la zona IANA que define a qué día pertenece una transacción, para
	// los comercios sin una zona propia en settlement.merchant_timezones.
	BusinessTimezone string

	// CloseMaxAgeDays es la antigüedad máxima, en días, del batch_date que puede pedir un
	// terminal. Un lote más viejo lo cierra el job automático o un operador. 0 = sin límite.
	CloseMaxAgeDays int

	// AdminAPIKey es un token único, con el rol máximo, que no identifica a nadie. Queda por
	// compatibilidad: AdminTokensFile es lo que permite saber quién hizo cada operación.
	AdminAPIKey string

	// AdminTokensFile es el archivo con los tokens nombrados: nombre, rol, SHA-256 y
	// vencimiento. Se relee sin reiniciar. Sin esto ni AdminAPIKey, la API queda deshabilitada.
	AdminTokensFile string

	// TrustedProxies son los CIDR, separados por coma, cuyo X-Forwarded-For se acepta.
	TrustedProxies string

	// TLSCert y TLSKey habilitan HTTPS en el puerto HTTP. Van juntos.
	TLSCert string
	TLSKey  string

	// TLSExternal declara que un proxy termina TLS. Con ENVIRONMENT=production, hace falta
	// esto o TLSCert: si no, el servicio no arranca, porque los tokens viajarían sin cifrar.
	TLSExternal bool
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
			BatchCloseHourDeprecated: os.Getenv("SETTLEMENT_BATCH_CLOSE_HOUR") != "" &&
				os.Getenv("SETTLEMENT_BATCH_CLOSE_HOUR") != "0",
			SubmitRetries: envInt("SETTLEMENT_SUBMIT_RETRIES", 3),
			SubmitTimeout: envDuration("SETTLEMENT_SUBMIT_TIMEOUT", 30*time.Second),
			Processor:     envStr("SETTLEMENT_PROCESSOR", ProcessorNone),

			BusinessTimezone: envStr("SETTLEMENT_BUSINESS_TIMEZONE", "America/Argentina/Buenos_Aires"),
			CloseMaxAgeDays:  envInt("SETTLEMENT_CLOSE_MAX_AGE_DAYS", 7),
			AdminAPIKey:      envStr("SETTLEMENT_ADMIN_API_KEY", ""),
			AdminTokensFile:  envStr("SETTLEMENT_ADMIN_TOKENS_FILE", ""),
			TrustedProxies:   envStr("SETTLEMENT_TRUSTED_PROXIES", ""),
			TLSCert:          envStr("SETTLEMENT_TLS_CERT", ""),
			TLSKey:           envStr("SETTLEMENT_TLS_KEY", ""),
			TLSExternal:      envStr("SETTLEMENT_TLS_EXTERNAL", "") == "true",
		},
	}

	grace, err := strictDuration("SETTLEMENT_BATCH_CLOSE_GRACE", defaultBatchCloseGrace)
	if err != nil {
		return nil, err
	}
	cfg.Settlement.BatchCloseGrace = grace

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
	if g := c.Settlement.BatchCloseGrace; g < 0 || g > maxBatchCloseGrace {
		return fmt.Errorf("config: SETTLEMENT_BATCH_CLOSE_GRACE %s out of range [0, %s]", g, maxBatchCloseGrace)
	}
	if c.Settlement.CloseMaxAgeDays < 0 {
		return fmt.Errorf("config: SETTLEMENT_CLOSE_MAX_AGE_DAYS %d cannot be negative", c.Settlement.CloseMaxAgeDays)
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
	if (c.Settlement.TLSCert == "") != (c.Settlement.TLSKey == "") {
		return fmt.Errorf("config: SETTLEMENT_TLS_CERT and SETTLEMENT_TLS_KEY must be set together")
	}
	if c.OTEL.Environment == "production" && c.Settlement.TLSCert == "" && !c.Settlement.TLSExternal {
		return fmt.Errorf("config: ENVIRONMENT=production requires SETTLEMENT_TLS_CERT or SETTLEMENT_TLS_EXTERNAL=true: admin tokens would travel unencrypted")
	}
	if _, err := c.Settlement.TrustedProxyPrefixes(); err != nil {
		return err
	}
	if _, err := time.LoadLocation(c.Settlement.BusinessTimezone); err != nil {
		return fmt.Errorf("config: SETTLEMENT_BUSINESS_TIMEZONE %q is invalid: %w", c.Settlement.BusinessTimezone, err)
	}
	return nil
}

// TrustedProxyPrefixes interpreta SETTLEMENT_TRUSTED_PROXIES.
func (s SettlementConfig) TrustedProxyPrefixes() ([]netip.Prefix, error) {
	if strings.TrimSpace(s.TrustedProxies) == "" {
		return nil, nil
	}
	var prefixes []netip.Prefix
	for _, part := range strings.Split(s.TrustedProxies, ",") {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("config: SETTLEMENT_TRUSTED_PROXIES %q is not a CIDR: %w", part, err)
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixes, nil
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

// Plazo después de la medianoche del comercio para el cierre automático. Más de 12 horas
// dejaría el lote de ayer abierto durante buena parte del día siguiente.
const (
	defaultBatchCloseGrace = 30 * time.Minute
	maxBatchCloseGrace     = 12 * time.Hour
)

// strictDuration lee una duración y, a diferencia de envDuration, falla si el valor no se
// puede interpretar, en lugar de usar el default sin avisar.
func strictDuration(key string, defaultVal time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s %q is not a duration (e.g. 30m): %w", key, v, err)
	}
	return d, nil
}

func envDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}
