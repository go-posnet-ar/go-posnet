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
	"SETTLEMENT_MDR_PERCENT", "SETTLEMENT_BATCH_CLOSE_HOUR",
	"SETTLEMENT_SUBMIT_RETRIES", "SETTLEMENT_SUBMIT_TIMEOUT", "SETTLEMENT_PROCESSOR", "SETTLEMENT_BUSINESS_TIMEZONE",
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

// ─── envStr / envInt / envFloat / envDuration / requireEnv ───────────────────

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

func TestEnvFloat(t *testing.T) {
	t.Run("uses env value when set", func(t *testing.T) {
		t.Setenv("TEST_ENV_FLOAT", "3.75")
		if got := envFloat("TEST_ENV_FLOAT", 1.5); got != 3.75 {
			t.Errorf("envFloat() = %v, want 3.75", got)
		}
	})

	t.Run("falls back to default when unset", func(t *testing.T) {
		t.Setenv("TEST_ENV_FLOAT", "")
		if got := envFloat("TEST_ENV_FLOAT", 1.5); got != 1.5 {
			t.Errorf("envFloat() = %v, want 1.5", got)
		}
	})

	t.Run("falls back to default when unparseable", func(t *testing.T) {
		t.Setenv("TEST_ENV_FLOAT", "not-a-float")
		if got := envFloat("TEST_ENV_FLOAT", 1.5); got != 1.5 {
			t.Errorf("envFloat() = %v, want 1.5", got)
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
		mdrPercent    float64
		batchClose    int
		wantErrSubstr string
	}{
		{"valid config", 2.5, 23, ""},
		{"mdr at min boundary", 0, 12, ""},
		{"mdr at max boundary", 10, 12, ""},
		{"mdr below min", -0.1, 12, "SETTLEMENT_MDR_PERCENT"},
		{"mdr above max", 10.1, 12, "SETTLEMENT_MDR_PERCENT"},
		{"batch close hour at min boundary", 2.5, 0, ""},
		{"batch close hour at max boundary", 2.5, 23, ""},
		{"batch close hour below min", 2.5, -1, "SETTLEMENT_BATCH_CLOSE_HOUR"},
		{"batch close hour above max", 2.5, 24, "SETTLEMENT_BATCH_CLOSE_HOUR"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				Settlement: SettlementConfig{MDRPercent: tc.mdrPercent, BatchCloseHour: tc.batchClose},
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
	t.Setenv("GRPC_PORT", "9093")
	t.Setenv("HTTP_PORT", "8083")
	t.Setenv("POSTGRES_MAX_CONNS", "50")
	t.Setenv("POSTGRES_MIN_CONNS", "5")
	t.Setenv("POSTGRES_MAX_CONN_LIFETIME", "1h")
	t.Setenv("POSTGRES_MAX_CONN_IDLE", "10m")
	t.Setenv("MIGRATIONS_DIR", "custom/migrations")
	t.Setenv("NATS_NKEY_PATH", "/etc/nkey")
	t.Setenv("OTEL_SERVICE_NAME", "custom-svc")
	t.Setenv("SETTLEMENT_MDR_PERCENT", "3.25")
	t.Setenv("SETTLEMENT_BATCH_CLOSE_HOUR", "22")
	t.Setenv("SETTLEMENT_SUBMIT_RETRIES", "5")
	t.Setenv("SETTLEMENT_SUBMIT_TIMEOUT", "45s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9093 {
		t.Errorf("GRPCPort = %d, want 9093", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8083 {
		t.Errorf("HTTPPort = %d, want 8083", cfg.HTTPPort)
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
	if cfg.Settlement.MDRPercent != 3.25 {
		t.Errorf("Settlement.MDRPercent = %v, want 3.25", cfg.Settlement.MDRPercent)
	}
	if cfg.Settlement.BatchCloseHour != 22 {
		t.Errorf("Settlement.BatchCloseHour = %d, want 22", cfg.Settlement.BatchCloseHour)
	}
	if cfg.Settlement.SubmitRetries != 5 {
		t.Errorf("Settlement.SubmitRetries = %d, want 5", cfg.Settlement.SubmitRetries)
	}
	if cfg.Settlement.SubmitTimeout != 45*time.Second {
		t.Errorf("Settlement.SubmitTimeout = %v, want 45s", cfg.Settlement.SubmitTimeout)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearOptionalEnv(t)
	setRequiredEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9093 {
		t.Errorf("GRPCPort = %d, want 9093", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8083 {
		t.Errorf("HTTPPort = %d, want 8083", cfg.HTTPPort)
	}
	if cfg.Postgres.MaxConns != 15 {
		t.Errorf("Postgres.MaxConns = %d, want 15", cfg.Postgres.MaxConns)
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
	if cfg.Postgres.MigrationsDir != "migrations/settlement" {
		t.Errorf("Postgres.MigrationsDir = %q, want %q", cfg.Postgres.MigrationsDir, "migrations/settlement")
	}
	if cfg.NATS.NKeyPath != "" {
		t.Errorf("NATS.NKeyPath = %q, want empty", cfg.NATS.NKeyPath)
	}
	if cfg.OTEL.ServiceName != "posnet-settlement" {
		t.Errorf("OTEL.ServiceName = %q, want %q", cfg.OTEL.ServiceName, "posnet-settlement")
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
	if cfg.Settlement.MDRPercent != 2.5 {
		t.Errorf("Settlement.MDRPercent = %v, want 2.5", cfg.Settlement.MDRPercent)
	}
	if cfg.Settlement.BatchCloseHour != 3 {
		t.Errorf("Settlement.BatchCloseHour = %d, want 3", cfg.Settlement.BatchCloseHour)
	}
	if cfg.Settlement.SubmitRetries != 3 {
		t.Errorf("Settlement.SubmitRetries = %d, want 3", cfg.Settlement.SubmitRetries)
	}
	if cfg.Settlement.SubmitTimeout != 30*time.Second {
		t.Errorf("Settlement.SubmitTimeout = %v, want 30s", cfg.Settlement.SubmitTimeout)
	}
	if cfg.Settlement.Processor != ProcessorNone {
		t.Errorf("Settlement.Processor = %q, want %q", cfg.Settlement.Processor, ProcessorNone)
	}
	if cfg.Settlement.BusinessTimezone != "America/Argentina/Buenos_Aires" {
		t.Errorf("Settlement.BusinessTimezone = %q", cfg.Settlement.BusinessTimezone)
	}
}

func TestValidate_Processor(t *testing.T) {
	tests := []struct {
		processor string
		wantErr   bool
	}{
		{ProcessorNone, false},
		{ProcessorMock, false},
		{"visa", true},
	}

	for _, tc := range tests {
		t.Run(tc.processor, func(t *testing.T) {
			c := &Config{Settlement: SettlementConfig{MDRPercent: 2.5, BatchCloseHour: 23, Processor: tc.processor}}
			err := c.validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
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

func TestLoad_ReturnsErrorWhenValidateFails(t *testing.T) {
	clearOptionalEnv(t)
	setRequiredEnv(t)
	t.Setenv("SETTLEMENT_MDR_PERCENT", "15")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "SETTLEMENT_MDR_PERCENT") {
		t.Fatalf("Load() error = %v, want it to mention SETTLEMENT_MDR_PERCENT", err)
	}
}

func TestValidate_BusinessTimezone(t *testing.T) {
	c := &Config{Settlement: SettlementConfig{MDRPercent: 2.5, BatchCloseHour: 3, BusinessTimezone: "Marte/Olympus"}}
	if err := c.validate(); err == nil || !strings.Contains(err.Error(), "SETTLEMENT_BUSINESS_TIMEZONE") {
		t.Errorf("validate() error = %v, want it to mention SETTLEMENT_BUSINESS_TIMEZONE", err)
	}
}
