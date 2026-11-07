// Command gateway es el API Gateway de posnet: el único punto de entrada HTTP hacia los
// servicios. Publica solo las rutas pensadas para afuera, termina TLS, aplica CORS y limita los
// pedidos de las rutas públicas.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juantevez/go-posnet/gateway/internal/config"
	"github.com/juantevez/go-posnet/gateway/internal/middleware"
	"github.com/juantevez/go-posnet/gateway/internal/proxy"
	"github.com/juantevez/go-posnet/pkg/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	observability.InitLogger(cfg.Environment, slog.LevelInfo)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracer, err := observability.InitTracer(ctx, observability.TracerConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		OTLPEndpoint:   cfg.OTLPEndpoint,
		Environment:    cfg.Environment,
	})
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer func() { _ = shutdownTracer(context.Background()) }()
	shutdownMeter, err := observability.InitMeter(ctx, cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("init meter: %w", err)
	}
	defer func() { _ = shutdownMeter(context.Background()) }()

	public, err := newPublicServer(cfg)
	if err != nil {
		return err
	}
	admin := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.AdminPort),
		Handler:           proxy.NewAdminRouter(observability.MetricsHandler()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errs := make(chan error, 2)
	go func() {
		serve := public.ListenAndServe
		if public.TLSConfig != nil {
			serve = func() error { return public.ListenAndServeTLS("", "") }
		}
		errs <- serve()
	}()
	go func() { errs <- admin.ListenAndServe() }()
	slog.Info("gateway listening",
		slog.Int("port", cfg.Port), slog.Int("admin_port", cfg.AdminPort),
		slog.Bool("tls", public.TLSConfig != nil),
		slog.String("terminal_gateway", cfg.TerminalGatewayURL),
		slog.String("settlement", cfg.SettlementURL),
		slog.Any("cors_allowed_origins", cfg.CORSAllowedOrigins),
	)

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server stopped: %w", err)
		}
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = public.Shutdown(shutdownCtx)
	_ = admin.Shutdown(shutdownCtx)
	slog.Info("gateway stopped")
	return nil
}

// newPublicServer arma el servidor público: rutas, límites, CORS y TLS.
func newPublicServer(cfg *config.Config) (*http.Server, error) {
	handler, err := newHandler(cfg)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if cfg.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load TLS certificate: %w", err)
		}
		srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	} else if !cfg.TLSExternal {
		slog.Warn("the gateway has no TLS — terminal tokens travel unencrypted (development only)")
	}
	return srv, nil
}

// newHandler arma la cadena de middlewares y las rutas.
func newHandler(cfg *config.Config) (http.Handler, error) {
	proxies, err := cfg.TrustedProxyPrefixes()
	if err != nil {
		return nil, err
	}
	clientIP := middleware.ClientIP{TrustedProxies: proxies}
	transport, err := proxy.NewTransport(cfg.UpstreamCA)
	if err != nil {
		return nil, err
	}

	metrics, err := middleware.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init gateway metrics: %w", err)
	}
	routes := proxy.Routes{
		PayLimit:    middleware.NewRateLimiter(cfg.PayPerMinute, clientIP),
		StatusLimit: middleware.NewRateLimiter(cfg.StatusPerMinute, clientIP),
		Metrics:     metrics,
	}
	if routes.TerminalGateway, err = proxy.NewUpstream("terminal-gateway", cfg.TerminalGatewayURL, transport, clientIP); err != nil {
		return nil, err
	}
	if cfg.SettlementURL != "" {
		if routes.Settlement, err = proxy.NewUpstream("settlement", cfg.SettlementURL, transport, clientIP); err != nil {
			return nil, err
		}
	} else {
		slog.Warn("SETTLEMENT_URL is empty — settlement routes are not published")
	}

	var h http.Handler = proxy.NewRouter(routes)
	h = middleware.MaxBody(cfg.MaxBodyBytes, h)
	h = middleware.CORS(cfg.CORSAllowedOrigins, h)
	h = middleware.SecurityHeaders(h)
	h = observability.HTTPMiddleware(h)
	return h, nil
}
