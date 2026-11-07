// Package config centraliza la configuración del BC Terminal Gateway.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCPort int
	HTTPPort int
	WSPort   int

	Postgres PostgresConfig
	NATS     NATSConfig
	OTEL     OTELConfig
	TLS      TLSConfig
	Session  SessionConfig

	// TerminalTokensFile es el archivo con los tokens de los terminales. Sin él, la API de
	// terminales queda abierta, y el servicio solo arranca con ENVIRONMENT=development.
	TerminalTokensFile string

	// CORSAllowedOrigins son los orígenes desde los que un navegador puede llamar a la API.
	// Vacío usa los del frontend de pago.
	CORSAllowedOrigins []string
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

// TLSConfig contiene los certificados para mTLS con los terminales.
// En desarrollo local pueden estar vacíos o ser "none".
type TLSConfig struct {
	CertPath string
	KeyPath  string
	CAPath   string
}

// TLSEnabled indica si hay configuración TLS real (no vacío ni "none").
func (t TLSConfig) TLSEnabled() bool {
	return t.CertPath != "" && t.CertPath != "none" &&
		t.KeyPath != "" && t.KeyPath != "none"
}

type SessionConfig struct {
	TTLSeconds          int
	ExpiredCleanupEvery time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort: envInt("GRPC_PORT", 9091),
		HTTPPort: envInt("HTTP_PORT", 8081),
		WSPort:   envInt("WS_PORT", 8082),

		Postgres: PostgresConfig{
			DSN:             requireEnv("POSTGRES_DSN"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 20)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", 30*time.Minute),
			MaxConnIdleTime: envDuration("POSTGRES_MAX_CONN_IDLE", 5*time.Minute),
			MigrationsDir:   envStr("MIGRATIONS_DIR", "migrations/terminal-gateway"),
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
			ServiceName:    envStr("OTEL_SERVICE_NAME", "posnet-terminal-gateway"),
			ServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
			OTLPEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
			Environment:    envStr("ENVIRONMENT", "development"),
		},

		// TLS opcionales — en dev pueden estar vacíos o "none"
		TLS: TLSConfig{
			CertPath: envStr("TLS_CERT_PATH", ""),
			KeyPath:  envStr("TLS_KEY_PATH", ""),
			CAPath:   envStr("TLS_CA_PATH", ""),
		},

		Session: SessionConfig{
			TTLSeconds:          envInt("SESSION_TTL_SECONDS", 300),
			ExpiredCleanupEvery: envDuration("SESSION_CLEANUP_EVERY", 1*time.Minute),
		},

		CORSAllowedOrigins: envList("CORS_ALLOWED_ORIGINS"),
		TerminalTokensFile: envStr("TERMINAL_TOKENS_FILE", ""),
	}

	return cfg, nil
}

// envList lee una lista separada por comas, sin elementos vacíos.
func envList(key string) []string {
	var items []string
	for _, item := range strings.Split(os.Getenv(key), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
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
