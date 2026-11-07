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
	"GRPC_PORT", "HTTP_PORT",
	"POSTGRES_MAX_CONNS", "POSTGRES_MIN_CONNS",
	"POSTGRES_MAX_CONN_LIFETIME", "POSTGRES_MAX_CONN_IDLE", "MIGRATIONS_DIR",
	"NATS_NKEY_PATH", "NATS_TLS_CERT", "NATS_TLS_KEY", "NATS_TLS_CA",
	"OTEL_SERVICE_NAME", "OTEL_SERVICE_VERSION", "OTEL_EXPORTER_OTLP_ENDPOINT", "ENVIRONMENT",
	"WEBHOOK_TIMEOUT", "WEBHOOK_DEFAULT_ENDPOINT",
	"RETRY_JOB_INTERVAL", "RETRY_BATCH_SIZE",
	"GRPC_CLIENT_TLS_CERT", "GRPC_CLIENT_TLS_KEY", "GRPC_CLIENT_TLS_CA",
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
	t.Setenv("TERMINAL_GATEWAY_GRPC_ADDR", "terminal-gateway:9091")
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

// ─── validate ─────────────────────────────────────────────────────────────────

func TestValidate(t *testing.T) {
	tests := []struct {
		name          string
		webhookTO     time.Duration
		batchSize     int
		wantErrSubstr string
	}{
		{"valid config", 10 * time.Second, 50, ""},
		{"webhook timeout at max boundary", 30 * time.Second, 50, ""},
		{"webhook timeout over max", 31 * time.Second, 50, "WEBHOOK_TIMEOUT"},
		{"batch size at min boundary", 10 * time.Second, 1, ""},
		{"batch size at max boundary", 10 * time.Second, 500, ""},
		{"batch size below min", 10 * time.Second, 0, "RETRY_BATCH_SIZE"},
		{"batch size above max", 10 * time.Second, 501, "RETRY_BATCH_SIZE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				Webhook: WebhookConfig{Timeout: tc.webhookTO},
				Retry:   RetryConfig{BatchSize: tc.batchSize},
				OTEL:    OTELConfig{Environment: "development"},
			}
			err := c.validate()
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Errorf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("validate() error = %v, want it to contain %q", err, tc.wantErrSubstr)
			}
		})
	}
}

// ─── Load ────────────────────────────────────────────────────────────────────

func TestLoad_Success(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("TERMINAL_GATEWAY_GRPC_ADDR", "terminal-gateway:9091")
	t.Setenv("GRPC_PORT", "9094")
	t.Setenv("HTTP_PORT", "8084")
	t.Setenv("POSTGRES_MAX_CONNS", "50")
	t.Setenv("POSTGRES_MIN_CONNS", "5")
	t.Setenv("POSTGRES_MAX_CONN_LIFETIME", "1h")
	t.Setenv("POSTGRES_MAX_CONN_IDLE", "10m")
	t.Setenv("MIGRATIONS_DIR", "custom/migrations")
	t.Setenv("NATS_NKEY_PATH", "/etc/nkey")
	t.Setenv("OTEL_SERVICE_NAME", "custom-svc")
	t.Setenv("WEBHOOK_TIMEOUT", "15s")
	t.Setenv("WEBHOOK_DEFAULT_ENDPOINT", "https://merchant.example.com/webhook")
	t.Setenv("RETRY_JOB_INTERVAL", "30s")
	t.Setenv("RETRY_BATCH_SIZE", "100")
	t.Setenv("GRPC_CLIENT_TLS_CERT", "/etc/client-cert.pem")
	t.Setenv("GRPC_CLIENT_TLS_KEY", "/etc/client-key.pem")
	t.Setenv("GRPC_CLIENT_TLS_CA", "/etc/ca.pem")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9094 {
		t.Errorf("GRPCPort = %d, want 9094", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8084 {
		t.Errorf("HTTPPort = %d, want 8084", cfg.HTTPPort)
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
	if cfg.Webhook.Timeout != 15*time.Second {
		t.Errorf("Webhook.Timeout = %v, want 15s", cfg.Webhook.Timeout)
	}
	if cfg.Webhook.DefaultEndpoint != "https://merchant.example.com/webhook" {
		t.Errorf("Webhook.DefaultEndpoint = %q, want %q", cfg.Webhook.DefaultEndpoint, "https://merchant.example.com/webhook")
	}
	if cfg.Retry.JobInterval != 30*time.Second {
		t.Errorf("Retry.JobInterval = %v, want 30s", cfg.Retry.JobInterval)
	}
	if cfg.Retry.BatchSize != 100 {
		t.Errorf("Retry.BatchSize = %d, want 100", cfg.Retry.BatchSize)
	}
	if cfg.GRPCClient.TerminalGatewayAddr != "terminal-gateway:9091" {
		t.Errorf("GRPCClient.TerminalGatewayAddr = %q, want %q", cfg.GRPCClient.TerminalGatewayAddr, "terminal-gateway:9091")
	}
	if cfg.GRPCClient.TLSCertPath != "/etc/client-cert.pem" {
		t.Errorf("GRPCClient.TLSCertPath = %q, want %q", cfg.GRPCClient.TLSCertPath, "/etc/client-cert.pem")
	}
	if cfg.GRPCClient.TLSKeyPath != "/etc/client-key.pem" {
		t.Errorf("GRPCClient.TLSKeyPath = %q, want %q", cfg.GRPCClient.TLSKeyPath, "/etc/client-key.pem")
	}
	if cfg.GRPCClient.TLSCAPath != "/etc/ca.pem" {
		t.Errorf("GRPCClient.TLSCAPath = %q, want %q", cfg.GRPCClient.TLSCAPath, "/etc/ca.pem")
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearOptionalEnv(t)
	setRequiredEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9094 {
		t.Errorf("GRPCPort = %d, want 9094", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8084 {
		t.Errorf("HTTPPort = %d, want 8084", cfg.HTTPPort)
	}
	if cfg.Postgres.MaxConns != 10 {
		t.Errorf("Postgres.MaxConns = %d, want 10", cfg.Postgres.MaxConns)
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
	if cfg.Postgres.MigrationsDir != "migrations/notification" {
		t.Errorf("Postgres.MigrationsDir = %q, want %q", cfg.Postgres.MigrationsDir, "migrations/notification")
	}
	if cfg.NATS.NKeyPath != "" {
		t.Errorf("NATS.NKeyPath = %q, want empty", cfg.NATS.NKeyPath)
	}
	if cfg.OTEL.ServiceName != "posnet-notification" {
		t.Errorf("OTEL.ServiceName = %q, want %q", cfg.OTEL.ServiceName, "posnet-notification")
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
	if cfg.Webhook.Timeout != 10*time.Second {
		t.Errorf("Webhook.Timeout = %v, want 10s", cfg.Webhook.Timeout)
	}
	if cfg.Webhook.DefaultEndpoint != "" {
		t.Errorf("Webhook.DefaultEndpoint = %q, want empty", cfg.Webhook.DefaultEndpoint)
	}
	if cfg.Retry.JobInterval != time.Minute {
		t.Errorf("Retry.JobInterval = %v, want 1m", cfg.Retry.JobInterval)
	}
	if cfg.Retry.BatchSize != 50 {
		t.Errorf("Retry.BatchSize = %d, want 50", cfg.Retry.BatchSize)
	}
	if cfg.GRPCClient.TerminalGatewayAddr != "terminal-gateway:9091" {
		t.Errorf("GRPCClient.TerminalGatewayAddr = %q, want %q", cfg.GRPCClient.TerminalGatewayAddr, "terminal-gateway:9091")
	}
	if cfg.GRPCClient.TLSCertPath != "" {
		t.Errorf("GRPCClient.TLSCertPath = %q, want empty", cfg.GRPCClient.TLSCertPath)
	}
}

func TestLoad_PanicsWithoutPostgresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("TERMINAL_GATEWAY_GRPC_ADDR", "terminal-gateway:9091")

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
	t.Setenv("TERMINAL_GATEWAY_GRPC_ADDR", "terminal-gateway:9091")

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

func TestLoad_PanicsWithoutTerminalGatewayAddr(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "nats://localhost:4222")
	t.Setenv("TERMINAL_GATEWAY_GRPC_ADDR", "")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Load() did not panic, want panic due to missing TERMINAL_GATEWAY_GRPC_ADDR")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "TERMINAL_GATEWAY_GRPC_ADDR") {
			t.Errorf("panic value = %v, want it to mention TERMINAL_GATEWAY_GRPC_ADDR", r)
		}
	}()
	_, _ = Load()
}

func TestLoad_ReturnsErrorWhenValidateFails(t *testing.T) {
	clearOptionalEnv(t)
	setRequiredEnv(t)
	t.Setenv("WEBHOOK_TIMEOUT", "1m")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "WEBHOOK_TIMEOUT") {
		t.Fatalf("Load() error = %v, want it to mention WEBHOOK_TIMEOUT", err)
	}
}

func TestValidate_GRPCClientTLS(t *testing.T) {
	full := GRPCClientConfig{TLSCAPath: "ca.pem", TLSCertPath: "c.pem", TLSKeyPath: "k.pem"}
	tests := []struct {
		name          string
		environment   string
		grpc          GRPCClientConfig
		wantErrSubstr string
	}{
		{"desarrollo sin TLS", "development", GRPCClientConfig{}, ""},
		{"producción sin TLS", "production", GRPCClientConfig{}, "requires GRPC_CLIENT_TLS_CA"},
		{"staging sin certificado de cliente", "staging", GRPCClientConfig{TLSCAPath: "ca.pem"}, "requires GRPC_CLIENT_TLS_CA"},
		{"producción con mTLS", "production", full, ""},
		{"certificado sin clave", "development", GRPCClientConfig{TLSCAPath: "ca.pem", TLSCertPath: "c.pem"}, "must be set together"},
		{"certificado sin CA", "development", GRPCClientConfig{TLSCertPath: "c.pem", TLSKeyPath: "k.pem"}, "requires GRPC_CLIENT_TLS_CA"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				Webhook:    WebhookConfig{Timeout: 10 * time.Second},
				Retry:      RetryConfig{BatchSize: 50},
				OTEL:       OTELConfig{Environment: tc.environment},
				GRPCClient: tc.grpc,
			}
			err := c.validate()
			if tc.wantErrSubstr == "" {
				if err != nil {
					t.Errorf("validate() error = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Errorf("validate() error = %v, want it to contain %q", err, tc.wantErrSubstr)
			}
		})
	}
}
