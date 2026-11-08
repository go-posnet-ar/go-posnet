// Package main es el entrypoint del BC Fraud Detection.
// Su única responsabilidad es orquestar el arranque y el shutdown graceful.
// Todo el wiring de dependencias está en wire.go.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juantevez/go-posnet/context/fraud-detection/config"
	"github.com/juantevez/go-posnet/pkg/observability"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run() error {
	// ── 1. Configuración ───────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// ── 2. Logger ──────────────────────────────────────────────────────────────
	observability.InitLogger(cfg.OTEL.Environment, slog.LevelInfo)
	log := slog.Default().With(slog.String("service", cfg.OTEL.ServiceName))

	// ── 3. Contexto raíz con cancelación ──────────────────────────────────────
	// Se cancela con SIGINT o SIGTERM, también si llegan durante el arranque: así una señal
	// temprana no mata el proceso sin cerrar lo que ya se abrió.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── 4. Tracer OpenTelemetry ────────────────────────────────────────────────
	shutdownTracer, err := observability.InitTracer(ctx, observability.TracerConfig{
		ServiceName:    cfg.OTEL.ServiceName,
		ServiceVersion: cfg.OTEL.ServiceVersion,
		OTLPEndpoint:   cfg.OTEL.OTLPEndpoint,
		Environment:    cfg.OTEL.Environment,
	})
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	// Último en cerrarse: exporta también los spans del apagado.
	defer shutdownTelemetry("tracer", shutdownTracer)

	// ── 5. Meter Prometheus ────────────────────────────────────────────────────
	shutdownMeter, err := observability.InitMeter(ctx, cfg.OTEL.ServiceName)
	if err != nil {
		return fmt.Errorf("init meter: %w", err)
	}
	defer shutdownTelemetry("meter", shutdownMeter)

	log.Info("starting fraud-detection BC",
		slog.String("version", cfg.OTEL.ServiceVersion),
		slog.String("env", cfg.OTEL.Environment),
		slog.Int("score_threshold_reject", cfg.Engine.ScoreThresholdReject),
		slog.Int("score_threshold_review", cfg.Engine.ScoreThresholdReview),
		slog.Duration("eval_timeout", cfg.Engine.EvalTimeout),
	)

	// ── 6. Wiring de dependencias ──────────────────────────────────────────────
	app, err := wire(ctx, cfg)
	if err != nil {
		return fmt.Errorf("wire dependencies: %w", err)
	}
	// Si el arranque falla, cancela el contexto antes de cerrar: close espera a los jobs que lo
	// escuchan, y sin cancelarlo se quedaría esperando. close es idempotente: en el camino
	// normal se llama abajo.
	defer func() {
		stop()
		app.close()
	}()

	// ── 7. Arrancar servicios ──────────────────────────────────────────────────
	if err := app.start(ctx); err != nil {
		return fmt.Errorf("start services: %w", err)
	}

	log.Info("fraud-detection BC ready — waiting for messages",
		slog.Int("http_port", cfg.HTTPPort),
		slog.Int("grpc_port", cfg.GRPCPort),
	)

	// ── 8. Graceful shutdown ───────────────────────────────────────────────────
	<-ctx.Done()
	// Restaura el comportamiento por defecto: si el cierre se demora, una segunda señal termina
	// el proceso sin esperar.
	stop()

	log.Info("shutdown signal received — stopping services")
	app.close()

	log.Info("fraud-detection BC stopped cleanly")
	return nil
}

// telemetryShutdownTimeout limita lo que se espera para exportar los últimos spans y métricas.
const telemetryShutdownTimeout = 5 * time.Second

// shutdownTelemetry cierra el tracer o el meter con un contexto propio: el raíz ya está
// cancelado, y con él el exportador descarta lo que quedó en cola en lugar de enviarlo.
func shutdownTelemetry(name string, shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), telemetryShutdownTimeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		slog.Warn("telemetry shutdown failed", slog.String("component", name), slog.String("error", err.Error()))
	}
}
