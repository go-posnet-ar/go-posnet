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
	"ACQUIRER_HOST", "ACQUIRER_PORT", "ACQUIRER_TLS_CERT", "ACQUIRER_TLS_KEY", "ACQUIRER_TLS_CA",
	"ACQUIRER_TIMEOUT_SECONDS", "INVALID_TRANSACTIONS_RETENTION", "FRAUD_CHECK_TIMEOUT",
}

func clearOptionalEnv(t *testing.T) {
	t.Helper()
	for _, k := range optionalEnvKeys {
		t.Setenv(k, "")
	}
}

// ─── AcquirerConfig.TLSEnabled ────────────────────────────────────────────────

func TestAcquirerConfig_TLSEnabled(t *testing.T) {
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
			a := AcquirerConfig{TLSCertPath: tc.certPath, TLSKeyPath: tc.keyPath}
			if got := a.TLSEnabled(); got != tc.want {
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
	t.Setenv("GRPC_PORT", "9091")
	t.Setenv("HTTP_PORT", "8081")
	t.Setenv("POSTGRES_MAX_CONNS", "50")
	t.Setenv("POSTGRES_MIN_CONNS", "5")
	t.Setenv("POSTGRES_MAX_CONN_LIFETIME", "1h")
	t.Setenv("POSTGRES_MAX_CONN_IDLE", "10m")
	t.Setenv("MIGRATIONS_DIR", "custom/migrations")
	t.Setenv("NATS_NKEY_PATH", "/etc/nkey")
	t.Setenv("OTEL_SERVICE_NAME", "custom-svc")
	t.Setenv("ACQUIRER_HOST", "acquirer.internal")
	t.Setenv("ACQUIRER_PORT", "9200")
	t.Setenv("ACQUIRER_TIMEOUT_SECONDS", "45")

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
	if cfg.Acquirer.Host != "acquirer.internal" {
		t.Errorf("Acquirer.Host = %q, want %q", cfg.Acquirer.Host, "acquirer.internal")
	}
	if cfg.Acquirer.Port != 9200 {
		t.Errorf("Acquirer.Port = %d, want 9200", cfg.Acquirer.Port)
	}
	if cfg.Acquirer.TimeoutSeconds != 45 {
		t.Errorf("Acquirer.TimeoutSeconds = %d, want 45", cfg.Acquirer.TimeoutSeconds)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearOptionalEnv(t)
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("NATS_URL", "nats://localhost:4222")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.GRPCPort != 9090 {
		t.Errorf("GRPCPort = %d, want 9090", cfg.GRPCPort)
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.HTTPPort)
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
	if cfg.Postgres.MigrationsDir != "migrations/authorization" {
		t.Errorf("Postgres.MigrationsDir = %q, want %q", cfg.Postgres.MigrationsDir, "migrations/authorization")
	}
	if cfg.NATS.NKeyPath != "" {
		t.Errorf("NATS.NKeyPath = %q, want empty", cfg.NATS.NKeyPath)
	}
	if cfg.OTEL.ServiceName != "posnet-authorization" {
		t.Errorf("OTEL.ServiceName = %q, want %q", cfg.OTEL.ServiceName, "posnet-authorization")
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
	if cfg.Acquirer.Host != "localhost" {
		t.Errorf("Acquirer.Host = %q, want %q", cfg.Acquirer.Host, "localhost")
	}
	if cfg.Acquirer.Port != 9100 {
		t.Errorf("Acquirer.Port = %d, want 9100", cfg.Acquirer.Port)
	}
	if cfg.Acquirer.TimeoutSeconds != 30 {
		t.Errorf("Acquirer.TimeoutSeconds = %d, want 30", cfg.Acquirer.TimeoutSeconds)
	}
	if cfg.Acquirer.TLSEnabled() {
		t.Error("Acquirer.TLSEnabled() = true, want false (sin TLS configurado por defecto)")
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

func TestLoad_ReconcilerConfig(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Acquirer.ReconcileEvery != 30*time.Second || cfg.Acquirer.StuckAfter != 2*time.Minute {
		t.Errorf("defaults = %v, %v", cfg.Acquirer.ReconcileEvery, cfg.Acquirer.StuckAfter)
	}

	// Con un límite menor que el timeout, revertiría llamadas en curso.
	t.Setenv("ACQUIRER_STUCK_AFTER", "30s")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with ACQUIRER_STUCK_AFTER equal to the timeout")
	}

	t.Setenv("ACQUIRER_STUCK_AFTER", "2m")
	t.Setenv("ACQUIRER_RECONCILE_EVERY", "-1s")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with a negative ACQUIRER_RECONCILE_EVERY")
	}
}

func TestLoad_InvalidTransactionsRetention(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	tests := []struct {
		value   string
		want    time.Duration
		wantErr bool
	}{
		{value: "", want: 90 * 24 * time.Hour},
		{value: "720h", want: 30 * 24 * time.Hour},
		{value: "0", want: 0},
		{value: "30d", wantErr: true},
		{value: "-1h", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("INVALID_TRANSACTIONS_RETENTION", tc.value)
			cfg, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Fatal("Load() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.InvalidTransactionsRetention != tc.want {
				t.Errorf("retention = %v, want %v", cfg.InvalidTransactionsRetention, tc.want)
			}
		})
	}
}

func TestLoad_AcquirerCallsConfig(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Acquirer.MaxConcurrent != 20 || cfg.Acquirer.MockScenarios {
		t.Errorf("defaults = %d, %v", cfg.Acquirer.MaxConcurrent, cfg.Acquirer.MockScenarios)
	}

	t.Setenv("ACQUIRER_MAX_CONCURRENT", "0")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with ACQUIRER_MAX_CONCURRENT=0")
	}
	t.Setenv("ACQUIRER_MAX_CONCURRENT", "20")

	// Montos que no se aprueban, en producción: nunca.
	t.Setenv("ACQUIRER_MOCK_SCENARIOS", "true")
	t.Setenv("ENVIRONMENT", "production")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with mock scenarios in production")
	}
	t.Setenv("ENVIRONMENT", "development")
	if cfg, err := Load(); err != nil || !cfg.Acquirer.MockScenarios {
		t.Errorf("Load() = %+v, %v", cfg, err)
	}
}

func TestLoad_AcquirerMode(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	cfg, err := Load()
	if err != nil || cfg.Acquirer.Mode != AcquirerModeMock {
		t.Fatalf("default = %+v, %v", cfg, err)
	}
	t.Setenv("ACQUIRER_MODE", "otro")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with an unknown mode")
	}
	t.Setenv("ACQUIRER_MODE", AcquirerModeISO8583)
	if cfg, err := Load(); err != nil || cfg.Acquirer.Mode != AcquirerModeISO8583 {
		t.Errorf("iso8583 = %+v, %v", cfg, err)
	}
	// Usa tarjetas de prueba: en producción, nunca.
	t.Setenv("ENVIRONMENT", "production")
	if _, err := Load(); err == nil {
		t.Error("Load() error = nil with test cards in production")
	}
}

func TestLoad_FraudCheckTimeout(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://x")
	t.Setenv("NATS_URL", "nats://x")

	tests := []struct {
		value   string
		want    time.Duration
		wantErr bool
	}{
		{value: "", want: 5 * time.Second},
		{value: "10s", want: 10 * time.Second},
		{value: "1s", want: time.Second},
		{value: "500ms", wantErr: true},
		{value: "0", wantErr: true},
		{value: "5", wantErr: true},
		{value: "-5s", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("FRAUD_CHECK_TIMEOUT", tc.value)
			cfg, err := Load()
			if tc.wantErr {
				if err == nil {
					t.Fatal("Load() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.FraudCheckTimeout != tc.want {
				t.Errorf("FraudCheckTimeout = %v, want %v", cfg.FraudCheckTimeout, tc.want)
			}
		})
	}
}
