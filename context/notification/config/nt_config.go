// Package config centraliza la configuración del BC Notification.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config contiene toda la configuración del BC Notification.
type Config struct {
	GRPCPort int
	HTTPPort int

	Postgres   PostgresConfig
	NATS       NATSConfig
	OTEL       OTELConfig
	Webhook    WebhookConfig
	Retry      RetryConfig
	GRPCClient GRPCClientConfig
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

// WebhookConfig contiene los parámetros para el dispatcher de webhooks.
type WebhookConfig struct {
	// Timeout es el tiempo máximo de espera para la respuesta HTTP del comercio.
	Timeout time.Duration

	// DefaultEndpoint es el endpoint por defecto si el comercio no tiene uno configurado.
	// En producción cada comercio tiene su propio endpoint en la BD.
	DefaultEndpoint string
}

// RetryConfig contiene los parámetros del job de reintentos periódico.
type RetryConfig struct {
	// JobInterval es la frecuencia con que el job busca notificaciones pendientes de reintento.
	JobInterval time.Duration

	// BatchSize es la cantidad máxima de notificaciones a procesar por ciclo del job.
	BatchSize int
}

// GRPCClientConfig contiene la dirección del servidor gRPC de Terminal Gateway.
// Notification es el único BC que actúa como cliente gRPC.
type GRPCClientConfig struct {
	TerminalGatewayAddr string // ej: "terminal-gateway:9091"
	// TLSCAPath verifica a Terminal Gateway, y TLSCertPath y TLSKeyPath son el certificado de
	// Notification para mTLS. Fuera de desarrollo hacen falta los tres.
	TLSCertPath string
	TLSKeyPath  string
	TLSCAPath   string
	// TLSServerName reemplaza al host de TerminalGatewayAddr al verificar el certificado.
	TLSServerName string
}

// Load carga la configuración desde variables de entorno.
func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort: envInt("GRPC_PORT", 9094),
		HTTPPort: envInt("HTTP_PORT", 8084),

		Postgres: PostgresConfig{
			DSN:             requireEnv("POSTGRES_DSN"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 10)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", 30*time.Minute),
			MaxConnIdleTime: envDuration("POSTGRES_MAX_CONN_IDLE", 5*time.Minute),
			MigrationsDir:   envStr("MIGRATIONS_DIR", "migrations/notification"),
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
			ServiceName:    envStr("OTEL_SERVICE_NAME", "posnet-notification"),
			ServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
			OTLPEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
			Environment:    envStr("ENVIRONMENT", "development"),
		},

		Webhook: WebhookConfig{
			Timeout:         envDuration("WEBHOOK_TIMEOUT", 10*time.Second),
			DefaultEndpoint: envStr("WEBHOOK_DEFAULT_ENDPOINT", ""),
		},

		Retry: RetryConfig{
			JobInterval: envDuration("RETRY_JOB_INTERVAL", 1*time.Minute),
			BatchSize:   envInt("RETRY_BATCH_SIZE", 50),
		},

		GRPCClient: GRPCClientConfig{
			TerminalGatewayAddr: requireEnv("TERMINAL_GATEWAY_GRPC_ADDR"),
			TLSCertPath:         envStr("GRPC_CLIENT_TLS_CERT", ""),
			TLSKeyPath:          envStr("GRPC_CLIENT_TLS_KEY", ""),
			TLSCAPath:           envStr("GRPC_CLIENT_TLS_CA", ""),
			TLSServerName:       envStr("GRPC_CLIENT_TLS_SERVER_NAME", ""),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Webhook.Timeout > 30*time.Second {
		return fmt.Errorf("config: WEBHOOK_TIMEOUT %s too high — max 30s to avoid blocking the subscriber",
			c.Webhook.Timeout)
	}
	if c.Retry.BatchSize < 1 || c.Retry.BatchSize > 500 {
		return fmt.Errorf("config: RETRY_BATCH_SIZE %d out of range [1, 500]",
			c.Retry.BatchSize)
	}
	g := c.GRPCClient
	if (g.TLSCertPath == "") != (g.TLSKeyPath == "") {
		return fmt.Errorf("config: GRPC_CLIENT_TLS_CERT and GRPC_CLIENT_TLS_KEY must be set together")
	}
	if g.TLSCertPath != "" && g.TLSCAPath == "" {
		return fmt.Errorf("config: GRPC_CLIENT_TLS_CERT requires GRPC_CLIENT_TLS_CA to verify Terminal Gateway")
	}
	if c.OTEL.Environment != "development" && (g.TLSCAPath == "" || g.TLSCertPath == "") {
		return fmt.Errorf("config: ENVIRONMENT=%s requires GRPC_CLIENT_TLS_CA, GRPC_CLIENT_TLS_CERT and GRPC_CLIENT_TLS_KEY: receipts would travel unencrypted", c.OTEL.Environment)
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

func envDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}
