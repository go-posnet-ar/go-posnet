package config

import (
	"strings"
	"testing"
)

var allEnvVars = []string{
	"GATEWAY_PORT", "GATEWAY_ADMIN_PORT", "TERMINAL_GATEWAY_URL", "GATEWAY_UPSTREAM_CA",
	"GATEWAY_CORS_ALLOWED_ORIGINS", "GATEWAY_TLS_CERT", "GATEWAY_TLS_KEY", "GATEWAY_TLS_EXTERNAL",
	"GATEWAY_TRUSTED_PROXIES", "GATEWAY_PAY_PER_MINUTE", "GATEWAY_STATUS_PER_MINUTE",
	"GATEWAY_MAX_BODY_BYTES", "ENVIRONMENT",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvVars {
		t.Setenv(k, "")
	}
	// SETTLEMENT_URL distingue ausente de vacía: t.Setenv no puede dejarla ausente, así que se
	// le da el default explícito.
	t.Setenv("SETTLEMENT_URL", "http://localhost:8084")
}

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Port != 8000 || cfg.AdminPort != 8001 {
		t.Errorf("ports = %d, %d, want 8000, 8001", cfg.Port, cfg.AdminPort)
	}
	if cfg.TerminalGatewayURL != "http://localhost:8081" || cfg.SettlementURL != "http://localhost:8084" {
		t.Errorf("upstreams = %q, %q", cfg.TerminalGatewayURL, cfg.SettlementURL)
	}
	if len(cfg.CORSAllowedOrigins) != 1 || cfg.CORSAllowedOrigins[0] != "http://localhost:5173" {
		t.Errorf("CORSAllowedOrigins = %v", cfg.CORSAllowedOrigins)
	}
	if cfg.PayPerMinute != 20 || cfg.StatusPerMinute != 240 || cfg.MaxBodyBytes != 1<<20 {
		t.Errorf("limits = %d, %d, %d", cfg.PayPerMinute, cfg.StatusPerMinute, cfg.MaxBodyBytes)
	}
}

func TestLoad_EmptySettlementURLDisablesItsRoutes(t *testing.T) {
	clearEnv(t)
	t.Setenv("SETTLEMENT_URL", "")
	cfg, err := Load()
	if err != nil || cfg.SettlementURL != "" {
		t.Fatalf("Load() = %+v, %v, want SettlementURL vacía", cfg, err)
	}
}

func TestLoad_Validation(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{"producción sin TLS", map[string]string{"ENVIRONMENT": "production"}, "GATEWAY_TLS_CERT or GATEWAY_TLS_EXTERNAL"},
		{"producción detrás de un proxy TLS", map[string]string{"ENVIRONMENT": "production", "GATEWAY_TLS_EXTERNAL": "true"}, ""},
		{"certificado sin clave", map[string]string{"GATEWAY_TLS_CERT": "c.pem"}, "set together"},
		{"puertos iguales", map[string]string{"GATEWAY_ADMIN_PORT": "8000"}, "must differ"},
		{"URL sin esquema", map[string]string{"TERMINAL_GATEWAY_URL": "terminal-gateway:8081"}, "TERMINAL_GATEWAY_URL"},
		{"URL con path", map[string]string{"TERMINAL_GATEWAY_URL": "http://tg:8081/api"}, "cannot have a path"},
		{"settlement inválida", map[string]string{"SETTLEMENT_URL": "ftp://st"}, "SETTLEMENT_URL"},
		{"límite en cero", map[string]string{"GATEWAY_PAY_PER_MINUTE": "0"}, "at least 1"},
		{"CORS * fuera de desarrollo", map[string]string{"ENVIRONMENT": "staging", "GATEWAY_TLS_EXTERNAL": "true", "GATEWAY_CORS_ALLOWED_ORIGINS": "*"}, "cannot be *"},
		{"CORS * en desarrollo", map[string]string{"GATEWAY_CORS_ALLOWED_ORIGINS": "*"}, ""},
		{"proxy inválido", map[string]string{"GATEWAY_TRUSTED_PROXIES": "10.0.0.0/33"}, "not a CIDR"},
		{"proxies válidos", map[string]string{"GATEWAY_TRUSTED_PROXIES": "10.0.0.0/8, 192.168.1.1/32"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Load() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Load() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
