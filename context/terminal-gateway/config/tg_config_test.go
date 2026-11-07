package config

import (
	"strings"
	"testing"
	"time"
)

// optionalEnvKeys son todas las variables de entorno opcionales que Load lee.
// Se usan para blanquear el entorno en los tests de defaults, evitando que
// variables ya seteadas en la máquina donde corre el test contaminen el
// resultado esperado.
var optionalEnvKeys = []string{
	"GRPC_PORT", "HTTP_PORT", "WS_PORT",
	"POSTGRES_MAX_CONNS", "POSTGRES_MIN_CONNS",
	"POSTGRES_MAX_CONN_LIFETIME", "POSTGRES_MAX_CONN_IDLE", "MIGRATIONS_DIR",
	"NATS_NKEY_PATH", "NATS_TLS_CERT", "NATS_TLS_KEY", "NATS_TLS_CA",
	"OTEL_SERVICE_NAME", "OTEL_SERVICE_VERSION", "OTEL_EXPORTER_OTLP_ENDPOINT", "ENVIRONMENT",
	"TLS_CERT_PATH", "TLS_KEY_PATH", "TLS_CA_PATH",
	"SESSION_TTL_SECONDS", "SESSION_CLEANUP_EVERY",
}

func clearOptionalEnv(t *testing.T) {
	t.Helper()
	for _, k := range optionalEnvKeys {
		t.Setenv(k, "")
	}
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "nats://localhost:4222")
}

// ─── TLSConfig.TLSEnabled ─────────────────────────────────────────────────────

func TestTLSConfig_TLSEnabled(t *testing.T) {
	tests := []struct {
		name     string
		certPath string
		keyPath  string
		want     bool
	}{
		{"both set", "cert.pem", "key.pem", true},
		{"cert empty", "", "key.pem", false},
		{"key empty", "cert.pem", "", false},
		{"both empty", "", "", false},
		{"cert none", "none", "key.pem", false},
		{"key none", "cert.pem", "none", false},
		{"both none", "none", "none", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tlsCfg := TLSConfig{CertPath: tc.certPath, KeyPath: tc.keyPath}
			if got := tlsCfg.TLSEnabled(); got != tc.want {
				t.Errorf("TLSEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─── envStr / envInt / envDuration / requireEnv ──────────────────────────────

func TestEnvStr(t *testing.T) {
	t.Run("uses env value when set", func(t *testing.T) {
		t.Setenv("TEST_ENV_STR", "custom")
		if got := envStr("TEST_ENV_STR", "default"); got != "custom" {
			t.Errorf("envStr() = %q, want %q", got, "custom")
		}
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TEST_ENV_STR", "")
		if got := envStr("TEST_ENV_STR", "default"); got != "default" {
			t.Errorf("envStr() = %q, want %q", got, "default")
		}
	})
}

func TestEnvInt(t *testing.T) {
	t.Run("uses env value when set", func(t *testing.T) {
		t.Setenv("TEST_ENV_INT", "42")
		if got := envInt("TEST_ENV_INT", 7); got != 42 {
			t.Errorf("envInt() = %d, want 42", got)
		}
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TEST_ENV_INT", "")
		if got := envInt("TEST_ENV_INT", 7); got != 7 {
			t.Errorf("envInt() = %d, want 7", got)
		}
	})

	t.Run("falls back to default when unparseable", func(t *testing.T) {
		t.Setenv("TEST_ENV_INT", "not-a-number")
		if got := envInt("TEST_ENV_INT", 7); got != 7 {
			t.Errorf("envInt() = %d, want 7", got)
		}
	})
}

func TestEnvDuration(t *testing.T) {
	t.Run("uses env value when set", func(t *testing.T) {
		t.Setenv("TEST_ENV_DURATION", "5s")
		if got := envDuration("TEST_ENV_DURATION", time.Minute); got != 5*time.Second {
			t.Errorf("envDuration() = %v, want 5s", got)
		}
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TEST_ENV_DURATION", "")
		if got := envDuration("TEST_ENV_DURATION", time.Minute); got != time.Minute {
			t.Errorf("envDuration() = %v, want 1m", got)
		}
	})

	t.Run("falls back to default when unparseable", func(t *testing.T) {
		t.Setenv("TEST_ENV_DURATION", "not-a-duration")
		if got := envDuration("TEST_ENV_DURATION", time.Minute); got != time.Minute {
			t.Errorf("envDuration() = %v, want 1m", got)
		}
	})
}

func TestRequireEnv(t *testing.T) {
	t.Run("returns value when set", func(t *testing.T) {
		t.Setenv("TEST_REQUIRE_ENV", "value")
		if got := requireEnv("TEST_REQUIRE_ENV"); got != "value" {
			t.Errorf("requireEnv() = %q, want %q", got, "value")
		}
	})

	t.Run("panics when unset", func(t *testing.T) {
		t.Setenv("TEST_REQUIRE_ENV", "")

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("requireEnv() did not panic, want panic")
			}
			msg, ok := r.(string)
			if !ok || !strings.Contains(msg, "TEST_REQUIRE_ENV") {
				t.Errorf("panic value = %v, want it to mention TEST_REQUIRE_ENV", r)
			}
		}()
		requireEnv("TEST_REQUIRE_ENV")
	})
}

// ─── Load ────────────────────────────────────────────────────────────────────

func TestLoad_Success(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("GRPC_PORT", "9191")
	t.Setenv("HTTP_PORT", "8181")
	t.Setenv("WS_PORT", "8182")
	t.Setenv("POSTGRES_MAX_CONNS", "50")
	t.Setenv("POSTGRES_MIN_CONNS", "5")
	t.Setenv("POSTGRES_MAX_CONN_LIFETIME", "1h")
	t.Setenv("POSTGRES_MAX_CONN_IDLE", "10m")
	t.Setenv("MIGRATIONS_DIR", "custom/migrations")
	t.Setenv("NATS_NKEY_PATH", "/etc/nkey")
	t.Setenv("OTEL_SERVICE_NAME", "custom-svc")
	t.Setenv("TLS_CERT_PATH", "/etc/tls/cert.pem")
	t.Setenv("TLS_KEY_PATH", "/etc/tls/key.pem")
	t.Setenv("TLS_CA_PATH", "/etc/tls/ca.pem")
	t.Setenv("SESSION_TTL_SECONDS", "600")
	t.Setenv("SESSION_CLEANUP_EVERY", "30s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9191 {
		t.Errorf("GRPCPort = %d, want 9191", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8181 {
		t.Errorf("HTTPPort = %d, want 8181", cfg.HTTPPort)
	}
	if cfg.WSPort != 8182 {
		t.Errorf("WSPort = %d, want 8182", cfg.WSPort)
	}
	if cfg.Postgres.DSN != "postgres://localhost/test" {
		t.Errorf("Postgres.DSN = %q, want %q", cfg.Postgres.DSN, "postgres://localhost/test")
	}
	if cfg.Postgres.MaxConns != 50 {
		t.Errorf("Postgres.MaxConns = %d, want 50", cfg.Postgres.MaxConns)
	}
	if cfg.Postgres.MinConns != 5 {
		t.Errorf("Postgres.MinConns = %d, want 5", cfg.Postgres.MinConns)
	}
	if cfg.Postgres.MaxConnLifetime != time.Hour {
		t.Errorf("Postgres.MaxConnLifetime = %v, want 1h", cfg.Postgres.MaxConnLifetime)
	}
	if cfg.Postgres.MaxConnIdleTime != 10*time.Minute {
		t.Errorf("Postgres.MaxConnIdleTime = %v, want 10m", cfg.Postgres.MaxConnIdleTime)
	}
	if cfg.Postgres.MigrationsDir != "custom/migrations" {
		t.Errorf("Postgres.MigrationsDir = %q, want %q", cfg.Postgres.MigrationsDir, "custom/migrations")
	}
	if cfg.NATS.URL != "nats://localhost:4222" {
		t.Errorf("NATS.URL = %q, want %q", cfg.NATS.URL, "nats://localhost:4222")
	}
	if cfg.NATS.NKeyPath != "/etc/nkey" {
		t.Errorf("NATS.NKeyPath = %q, want %q", cfg.NATS.NKeyPath, "/etc/nkey")
	}
	if cfg.NATS.MaxReconnect != -1 {
		t.Errorf("NATS.MaxReconnect = %d, want -1 (hardcoded, sin variable de entorno)", cfg.NATS.MaxReconnect)
	}
	if cfg.NATS.ReconnectWait != 2*time.Second {
		t.Errorf("NATS.ReconnectWait = %v, want 2s (hardcoded)", cfg.NATS.ReconnectWait)
	}
	if cfg.OTEL.ServiceName != "custom-svc" {
		t.Errorf("OTEL.ServiceName = %q, want %q", cfg.OTEL.ServiceName, "custom-svc")
	}
	if cfg.TLS.CertPath != "/etc/tls/cert.pem" {
		t.Errorf("TLS.CertPath = %q, want %q", cfg.TLS.CertPath, "/etc/tls/cert.pem")
	}
	if cfg.TLS.KeyPath != "/etc/tls/key.pem" {
		t.Errorf("TLS.KeyPath = %q, want %q", cfg.TLS.KeyPath, "/etc/tls/key.pem")
	}
	if cfg.TLS.CAPath != "/etc/tls/ca.pem" {
		t.Errorf("TLS.CAPath = %q, want %q", cfg.TLS.CAPath, "/etc/tls/ca.pem")
	}
	if !cfg.TLS.TLSEnabled() {
		t.Error("TLS.TLSEnabled() = false, want true with cert/key configured")
	}
	if cfg.Session.TTLSeconds != 600 {
		t.Errorf("Session.TTLSeconds = %d, want 600", cfg.Session.TTLSeconds)
	}
	if cfg.Session.ExpiredCleanupEvery != 30*time.Second {
		t.Errorf("Session.ExpiredCleanupEvery = %v, want 30s", cfg.Session.ExpiredCleanupEvery)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearOptionalEnv(t)
	setRequiredEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9091 {
		t.Errorf("GRPCPort = %d, want 9091", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8081 {
		t.Errorf("HTTPPort = %d, want 8081", cfg.HTTPPort)
	}
	if cfg.WSPort != 8082 {
		t.Errorf("WSPort = %d, want 8082", cfg.WSPort)
	}
	if cfg.Postgres.MaxConns != 20 {
		t.Errorf("Postgres.MaxConns = %d, want 20", cfg.Postgres.MaxConns)
	}
	if cfg.Postgres.MinConns != 2 {
		t.Errorf("Postgres.MinConns = %d, want 2", cfg.Postgres.MinConns)
	}
	if cfg.Postgres.MaxConnLifetime != 30*time.Minute {
		t.Errorf("Postgres.MaxConnLifetime = %v, want 30m", cfg.Postgres.MaxConnLifetime)
	}
	if cfg.Postgres.MaxConnIdleTime != 5*time.Minute {
		t.Errorf("Postgres.MaxConnIdleTime = %v, want 5m", cfg.Postgres.MaxConnIdleTime)
	}
	if cfg.Postgres.MigrationsDir != "migrations/terminal-gateway" {
		t.Errorf("Postgres.MigrationsDir = %q, want %q", cfg.Postgres.MigrationsDir, "migrations/terminal-gateway")
	}
	if cfg.NATS.NKeyPath != "" {
		t.Errorf("NATS.NKeyPath = %q, want empty", cfg.NATS.NKeyPath)
	}
	if cfg.OTEL.ServiceName != "posnet-terminal-gateway" {
		t.Errorf("OTEL.ServiceName = %q, want %q", cfg.OTEL.ServiceName, "posnet-terminal-gateway")
	}
	if cfg.OTEL.ServiceVersion != "1.0.0" {
		t.Errorf("OTEL.ServiceVersion = %q, want %q", cfg.OTEL.ServiceVersion, "1.0.0")
	}
	if cfg.OTEL.OTLPEndpoint != "otel-collector:4317" {
		t.Errorf("OTEL.OTLPEndpoint = %q, want %q", cfg.OTEL.OTLPEndpoint, "otel-collector:4317")
	}
	if cfg.OTEL.Environment != "development" {
		t.Errorf("OTEL.Environment = %q, want %q", cfg.OTEL.Environment, "development")
	}
	if cfg.TLS.CertPath != "" || cfg.TLS.KeyPath != "" || cfg.TLS.CAPath != "" {
		t.Error("TLS paths should default to empty")
	}
	if cfg.TLS.TLSEnabled() {
		t.Error("TLS.TLSEnabled() = true, want false without TLS configured")
	}
	if cfg.Session.TTLSeconds != 300 {
		t.Errorf("Session.TTLSeconds = %d, want 300", cfg.Session.TTLSeconds)
	}
	if cfg.Session.ExpiredCleanupEvery != time.Minute {
		t.Errorf("Session.ExpiredCleanupEvery = %v, want 1m", cfg.Session.ExpiredCleanupEvery)
	}
}

func TestLoad_PanicsWithoutPostgresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("NATS_URL", "nats://localhost:4222")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Load() did not panic, want panic due to missing POSTGRES_DSN")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "POSTGRES_DSN") {
			t.Errorf("panic value = %v, want it to mention POSTGRES_DSN", r)
		}
	}()
	_, _ = Load()
}

func TestLoad_PanicsWithoutNATSURL(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Load() did not panic, want panic due to missing NATS_URL")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "NATS_URL") {
			t.Errorf("panic value = %v, want it to mention NATS_URL", r)
		}
	}()
	_, _ = Load()
}

func TestLoad_CORSAllowedOrigins(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	t.Setenv("CORS_ALLOWED_ORIGINS", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.CORSAllowedOrigins != nil {
		t.Errorf("CORSAllowedOrigins = %v, want nil para usar los del frontend", cfg.CORSAllowedOrigins)
	}

	t.Setenv("CORS_ALLOWED_ORIGINS", " http://a:5173 , ,https://b ")
	if cfg, err = Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.CORSAllowedOrigins; len(got) != 2 || got[0] != "http://a:5173" || got[1] != "https://b" {
		t.Errorf("CORSAllowedOrigins = %q", got)
	}
}

func TestLoad_HTTPTLS(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	cases := []struct {
		name                           string
		env, cert, key, external, cidr string
		wantErr                        bool
	}{
		{name: "desarrollo sin TLS", env: "development"},
		{name: "producción sin TLS", env: "production", wantErr: true},
		{name: "staging sin TLS", env: "staging", wantErr: true},
		{name: "producción con certificado", env: "production", cert: "c.pem", key: "k.pem"},
		{name: "producción detrás de un proxy TLS", env: "production", external: "true"},
		{name: "certificado sin clave", env: "development", cert: "c.pem", wantErr: true},
		{name: "proxy de confianza válido", env: "development", cidr: "10.0.0.0/8, 192.168.1.10/32"},
		{name: "proxy de confianza inválido", env: "development", cidr: "10.0.0.0/33", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", tc.env)
			t.Setenv("HTTP_TLS_CERT", tc.cert)
			t.Setenv("HTTP_TLS_KEY", tc.key)
			t.Setenv("HTTP_TLS_EXTERNAL", tc.external)
			t.Setenv("TRUSTED_PROXIES", tc.cidr)
			// gRPC con mTLS, para que solo influya lo que se prueba acá.
			t.Setenv("GRPC_TLS_CERT", "g.pem")
			t.Setenv("GRPC_TLS_KEY", "g.key")
			t.Setenv("GRPC_TLS_CLIENT_CA", "ca.pem")
			t.Setenv("PAY_TOKEN_KEY", testPayTokenKey)
			cfg, err := Load()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && tc.cidr != "" {
				if prefixes, _ := cfg.TrustedProxyPrefixes(); len(prefixes) != 2 {
					t.Errorf("TrustedProxyPrefixes() = %v, want 2", prefixes)
				}
			}
		})
	}
}

func TestLoad_GRPCTLS(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("HTTP_TLS_EXTERNAL", "true")
	t.Setenv("PAY_TOKEN_KEY", testPayTokenKey)

	cases := []struct {
		name               string
		env, cert, key, ca string
		wantErr            bool
	}{
		{name: "desarrollo sin TLS", env: "development"},
		{name: "producción sin TLS", env: "production", wantErr: true},
		{name: "producción con TLS pero sin verificar clientes", env: "production", cert: "c", key: "k", wantErr: true},
		{name: "producción con mTLS", env: "production", cert: "c", key: "k", ca: "ca"},
		{name: "desarrollo con TLS sin mTLS", env: "development", cert: "c", key: "k"},
		{name: "certificado sin clave", env: "development", cert: "c", wantErr: true},
		{name: "CA sin certificado", env: "development", ca: "ca", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", tc.env)
			t.Setenv("GRPC_TLS_CERT", tc.cert)
			t.Setenv("GRPC_TLS_KEY", tc.key)
			t.Setenv("GRPC_TLS_CLIENT_CA", tc.ca)
			if _, err := Load(); (err != nil) != tc.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLoad_GRPCTLSClientNames(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	t.Setenv("GRPC_TLS_CLIENT_NAMES", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.GRPCTLSClientNames; len(got) != 1 || got[0] != "notification" {
		t.Errorf("GRPCTLSClientNames = %v, want [notification] por defecto", got)
	}

	t.Setenv("GRPC_TLS_CLIENT_NAMES", "notification, notification-2")
	if cfg, err = Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.GRPCTLSClientNames; len(got) != 2 || got[1] != "notification-2" {
		t.Errorf("GRPCTLSClientNames = %v", got)
	}
}

const testPayTokenKey = "0123456789abcdef0123456789abcdef"

func TestLoad_PayToken(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")
	t.Setenv("HTTP_TLS_EXTERNAL", "true")
	t.Setenv("GRPC_TLS_CERT", "g.pem")
	t.Setenv("GRPC_TLS_KEY", "g.key")
	t.Setenv("GRPC_TLS_CLIENT_CA", "ca.pem")

	cases := []struct {
		name, env, key, previous, required string
		wantRequired                       bool
		wantErr                            bool
	}{
		{name: "desarrollo: no se exige por defecto", env: "development"},
		{name: "producción: se exige por defecto", env: "production", key: testPayTokenKey, wantRequired: true},
		{name: "producción sin clave", env: "production", wantErr: true},
		{name: "desarrollo exigiéndolo", env: "development", required: "true", wantRequired: true},
		{name: "producción sin exigirlo", env: "production", key: testPayTokenKey, required: "false"},
		{name: "valor inválido", env: "development", required: "yes", wantErr: true},
		{name: "clave corta", env: "development", key: "corta", wantErr: true},
		{name: "clave anterior corta", env: "development", key: testPayTokenKey, previous: "corta", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", tc.env)
			t.Setenv("PAY_TOKEN_KEY", tc.key)
			t.Setenv("PAY_TOKEN_KEY_PREVIOUS", tc.previous)
			t.Setenv("PAY_TOKEN_REQUIRED", tc.required)
			cfg, err := Load()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Load() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil && cfg.PayTokenRequired != tc.wantRequired {
				t.Errorf("PayTokenRequired = %v, want %v", cfg.PayTokenRequired, tc.wantRequired)
			}
		})
	}
}
