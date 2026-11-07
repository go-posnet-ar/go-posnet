// Package config centraliza la configuración del API Gateway.
package config

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Config es la configuración del gateway.
type Config struct {
	// Port es el puerto público, y AdminPort el de /healthz, /readyz y /metrics, que no se exponen
	// hacia afuera.
	Port      int
	AdminPort int

	// TerminalGatewayURL es obligatoria. SettlementURL es opcional: vacía, sus rutas no se publican.
	TerminalGatewayURL string
	SettlementURL      string
	// TerminalGatewayWSURL es el WebSocket de los terminales. Vacía, /ws no se publica.
	TerminalGatewayWSURL string
	// UpstreamCA verifica a los servicios que usan HTTPS con un certificado propio.
	UpstreamCA string

	CORSAllowedOrigins []string

	// TLSCert y TLSKey habilitan HTTPS en Port. TLSExternal declara que TLS lo termina otro
	// proxy. Fuera de desarrollo hace falta uno de los dos.
	TLSCert     string
	TLSKey      string
	TLSExternal bool

	// TrustedProxies son los CIDR, separados por comas, cuyo X-Forwarded-For se acepta.
	TrustedProxies string

	// Límites por IP de las rutas públicas, por minuto.
	PayPerMinute    int
	StatusPerMinute int

	// MaxBodyBytes limita el cuerpo de cada pedido.
	MaxBodyBytes int64

	ServiceName    string
	ServiceVersion string
	OTLPEndpoint   string
	Environment    string
}

// Load carga la configuración desde variables de entorno y la valida.
func Load() (*Config, error) {
	cfg := &Config{
		Port:      envInt("GATEWAY_PORT", 8000),
		AdminPort: envInt("GATEWAY_ADMIN_PORT", 8001),

		TerminalGatewayURL: envStr("TERMINAL_GATEWAY_URL", "http://localhost:8081"),
		// Definida y vacía, desactiva sus rutas.
		SettlementURL:        envOptional("SETTLEMENT_URL", "http://localhost:8084"),
		TerminalGatewayWSURL: envOptional("TERMINAL_GATEWAY_WS_URL", "http://localhost:8082"),
		UpstreamCA:           envStr("GATEWAY_UPSTREAM_CA", ""),

		CORSAllowedOrigins: envList("GATEWAY_CORS_ALLOWED_ORIGINS", "http://localhost:5173"),

		TLSCert:     envStr("GATEWAY_TLS_CERT", ""),
		TLSKey:      envStr("GATEWAY_TLS_KEY", ""),
		TLSExternal: envStr("GATEWAY_TLS_EXTERNAL", "") == "true",

		TrustedProxies: envStr("GATEWAY_TRUSTED_PROXIES", ""),

		PayPerMinute:    envInt("GATEWAY_PAY_PER_MINUTE", 20),
		StatusPerMinute: envInt("GATEWAY_STATUS_PER_MINUTE", 240),
		MaxBodyBytes:    int64(envInt("GATEWAY_MAX_BODY_BYTES", 1<<20)),

		ServiceName:    envStr("OTEL_SERVICE_NAME", "posnet-gateway"),
		ServiceVersion: envStr("OTEL_SERVICE_VERSION", "1.0.0"),
		OTLPEndpoint:   envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317"),
		Environment:    envStr("ENVIRONMENT", "development"),
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	var errs []error
	if err := validateUpstream(c.TerminalGatewayURL); err != nil {
		errs = append(errs, fmt.Errorf("TERMINAL_GATEWAY_URL=%q: %w", c.TerminalGatewayURL, err))
	}
	if c.SettlementURL != "" {
		if err := validateUpstream(c.SettlementURL); err != nil {
			errs = append(errs, fmt.Errorf("SETTLEMENT_URL=%q: %w", c.SettlementURL, err))
		}
	}
	if c.TerminalGatewayWSURL != "" {
		if err := validateUpstream(c.TerminalGatewayWSURL); err != nil {
			errs = append(errs, fmt.Errorf("TERMINAL_GATEWAY_WS_URL=%q: %w", c.TerminalGatewayWSURL, err))
		}
	}
	if c.Port == c.AdminPort {
		errs = append(errs, errors.New("GATEWAY_PORT and GATEWAY_ADMIN_PORT must differ: /metrics would be public"))
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		errs = append(errs, errors.New("GATEWAY_TLS_CERT and GATEWAY_TLS_KEY must be set together"))
	}
	if c.Environment != "development" && c.TLSCert == "" && !c.TLSExternal {
		errs = append(errs, fmt.Errorf("ENVIRONMENT=%s requires GATEWAY_TLS_CERT or GATEWAY_TLS_EXTERNAL=true: terminal tokens would travel unencrypted", c.Environment))
	}
	if c.PayPerMinute < 1 || c.StatusPerMinute < 1 {
		errs = append(errs, errors.New("GATEWAY_PAY_PER_MINUTE and GATEWAY_STATUS_PER_MINUTE must be at least 1"))
	}
	if c.MaxBodyBytes < 1 {
		errs = append(errs, errors.New("GATEWAY_MAX_BODY_BYTES must be at least 1"))
	}
	for _, o := range c.CORSAllowedOrigins {
		if o == "*" && c.Environment != "development" {
			errs = append(errs, errors.New("GATEWAY_CORS_ALLOWED_ORIGINS cannot be * outside development"))
		}
	}
	if _, err := c.TrustedProxyPrefixes(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// TrustedProxyPrefixes interpreta GATEWAY_TRUSTED_PROXIES.
func (c *Config) TrustedProxyPrefixes() ([]netip.Prefix, error) {
	var prefixes []netip.Prefix
	for _, part := range strings.Split(c.TrustedProxies, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		prefix, err := netip.ParsePrefix(part)
		if err != nil {
			return nil, fmt.Errorf("GATEWAY_TRUSTED_PROXIES %q is not a CIDR: %w", part, err)
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixes, nil
}

func validateUpstream(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("the scheme must be http or https")
	}
	if u.Host == "" {
		return errors.New("the host is missing")
	}
	if u.Path != "" && u.Path != "/" {
		return errors.New("it cannot have a path: the gateway keeps the request path")
	}
	return nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envOptional distingue una variable ausente, que toma el default, de una vacía, que lo desactiva.
func envOptional(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func envList(key, def string) []string {
	var items []string
	for _, item := range strings.Split(envStr(key, def), ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}
