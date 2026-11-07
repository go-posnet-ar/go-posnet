// Package config centraliza la configuración del BC Terminal Gateway.
package config

import (
	"fmt"
	"net/netip"
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

	// HTTPTLSCert y HTTPTLSKey habilitan HTTPS en HTTP_PORT. Van juntos.
	HTTPTLSCert string
	HTTPTLSKey  string
	// HTTPTLSExternal declara que un proxy termina TLS. Fuera de desarrollo hace falta esto o
	// HTTPTLSCert: si no, el servicio no arranca, porque los tokens viajarían sin cifrar.
	HTTPTLSExternal bool
	// GRPCTLSCert y GRPCTLSKey habilitan TLS en el puerto gRPC, y GRPCTLSClientCA exige que el
	// cliente (Notification) presente un certificado firmado por esa CA. Fuera de desarrollo
	// hacen falta los tres.
	GRPCTLSCert     string
	GRPCTLSKey      string
	GRPCTLSClientCA string
	// GRPCTLSClientNames son los clientes aceptados, por CN o SAN DNS de su certificado.
	GRPCTLSClientNames []string

	// PayTokenKey firma el token de sesión de /pay y /status, y PayTokenKeyPrevious lo sigue
	// verificando durante una rotación. Fuera de desarrollo, PayTokenKey es obligatoria: varias
	// instancias tienen que compartirla. PayTokenRequired exige el token; por defecto, solo fuera
	// de desarrollo.
	PayTokenKey         string
	PayTokenKeyPrevious string
	PayTokenRequired    bool

	// TrustedProxies son los CIDR, separados por comas, cuyo X-Forwarded-For se acepta.
	TrustedProxies string

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
		HTTPTLSCert:        envStr("HTTP_TLS_CERT", ""),
		HTTPTLSKey:         envStr("HTTP_TLS_KEY", ""),
		HTTPTLSExternal:    envStr("HTTP_TLS_EXTERNAL", "") == "true",
		TrustedProxies:     envStr("TRUSTED_PROXIES", ""),
		GRPCTLSCert:        envStr("GRPC_TLS_CERT", ""),
		GRPCTLSKey:         envStr("GRPC_TLS_KEY", ""),
		GRPCTLSClientCA:    envStr("GRPC_TLS_CLIENT_CA", ""),
		GRPCTLSClientNames: envList("GRPC_TLS_CLIENT_NAMES"),

		PayTokenKey:         envStr("PAY_TOKEN_KEY", ""),
		PayTokenKeyPrevious: envStr("PAY_TOKEN_KEY_PREVIOUS", ""),
	}

	required, err := envBool("PAY_TOKEN_REQUIRED", cfg.OTEL.Environment != "development")
	if err != nil {
		return nil, err
	}
	cfg.PayTokenRequired = required

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if (c.HTTPTLSCert == "") != (c.HTTPTLSKey == "") {
		return fmt.Errorf("config: HTTP_TLS_CERT and HTTP_TLS_KEY must be set together")
	}
	if c.OTEL.Environment != "development" && c.HTTPTLSCert == "" && !c.HTTPTLSExternal {
		return fmt.Errorf("config: ENVIRONMENT=%s requires HTTP_TLS_CERT or HTTP_TLS_EXTERNAL=true: terminal tokens would travel unencrypted", c.OTEL.Environment)
	}
	if (c.GRPCTLSCert == "") != (c.GRPCTLSKey == "") {
		return fmt.Errorf("config: GRPC_TLS_CERT and GRPC_TLS_KEY must be set together")
	}
	if c.OTEL.Environment != "development" && c.PayTokenKey == "" {
		return fmt.Errorf("config: ENVIRONMENT=%s requires PAY_TOKEN_KEY: every instance must sign session tokens with the same key", c.OTEL.Environment)
	}
	for name, key := range map[string]string{"PAY_TOKEN_KEY": c.PayTokenKey, "PAY_TOKEN_KEY_PREVIOUS": c.PayTokenKeyPrevious} {
		if key != "" && len(key) < minPayTokenKeyLength {
			return fmt.Errorf("config: %s must have at least %d characters", name, minPayTokenKeyLength)
		}
	}
	if len(c.GRPCTLSClientNames) == 0 {
		// El único cliente gRPC del sistema.
		c.GRPCTLSClientNames = []string{"notification"}
	}
	if c.GRPCTLSClientCA != "" && c.GRPCTLSCert == "" {
		return fmt.Errorf("config: GRPC_TLS_CLIENT_CA requires GRPC_TLS_CERT and GRPC_TLS_KEY")
	}
	if c.OTEL.Environment != "development" && (c.GRPCTLSCert == "" || c.GRPCTLSClientCA == "") {
		return fmt.Errorf("config: ENVIRONMENT=%s requires GRPC_TLS_CERT, GRPC_TLS_KEY and GRPC_TLS_CLIENT_CA: gRPC would be unencrypted and accept any client", c.OTEL.Environment)
	}
	if _, err := c.TrustedProxyPrefixes(); err != nil {
		return err
	}
	return nil
}

// TrustedProxyPrefixes interpreta TRUSTED_PROXIES.
func (c *Config) TrustedProxyPrefixes() ([]netip.Prefix, error) {
	var prefixes []netip.Prefix
	for _, part := range strings.Split(c.TrustedProxies, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(part)
		if err != nil {
			return nil, fmt.Errorf("config: TRUSTED_PROXIES %q is not a CIDR: %w", part, err)
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixes, nil
}

// minPayTokenKeyLength rechaza claves triviales: con una clave adivinable, cualquiera podría
// firmar el token de cualquier sesión.
const minPayTokenKeyLength = 32

// envBool lee true o false. Un valor distinto es un error: exigir o no el token no puede
// depender de un error de tipeo.
func envBool(key string, def bool) (bool, error) {
	switch v := os.Getenv(key); v {
	case "":
		return def, nil
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("config: %s %q must be true or false", key, v)
	}
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
