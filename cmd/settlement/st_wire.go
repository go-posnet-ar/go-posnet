package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/config"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	grpcserver "github.com/juantevez/go-posnet/context/settlement/infrastructure/grpc/server"
	httpinfra "github.com/juantevez/go-posnet/context/settlement/infrastructure/http"
	natsinfra "github.com/juantevez/go-posnet/context/settlement/infrastructure/nats"
	pginfra "github.com/juantevez/go-posnet/context/settlement/infrastructure/postgres"
	stprocessor "github.com/juantevez/go-posnet/context/settlement/infrastructure/processor"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	natsclient "github.com/nats-io/nats.go"
)

// app agrupa todos los componentes del servicio y sus recursos abiertos.
type app struct {
	pool       *pgxpool.Pool
	natsConn   *natsclient.Conn
	subscriber *natsinfra.Subscriber
	grpcSrv    *grpcserver.SettlementServer
	grpcPort   int
	httpSrv    *http.Server

	batchHandler   *command.BatchHandler
	batchCloseHour int

	outboxRelay *outbox.Relay
	// relayWG espera al Relay en el apagado, antes de cerrar NATS y Postgres.
	relayWG sync.WaitGroup
}

// wire construye el grafo de dependencias completo del BC Settlement.
func wire(ctx context.Context, cfg *config.Config) (*app, error) {

	// ── PostgreSQL ─────────────────────────────────────────────────────────────
	pool, err := pgutil.NewPool(ctx, pgutil.Config{
		DSN:             cfg.Postgres.DSN,
		MaxConns:        cfg.Postgres.MaxConns,
		MinConns:        cfg.Postgres.MinConns,
		MaxConnLifetime: cfg.Postgres.MaxConnLifetime,
		MaxConnIdleTime: cfg.Postgres.MaxConnIdleTime,
	})
	if err != nil {
		return nil, fmt.Errorf("wire: init postgres pool: %w", err)
	}
	if err := pgutil.RegisterPoolMetrics(pool); err != nil {
		return nil, fmt.Errorf("wire: register pgx pool metrics: %w", err)
	}

	if err := pgutil.Migrate(ctx, pool, cfg.Postgres.MigrationsDir); err != nil {
		pool.Close()
		return nil, fmt.Errorf("wire: run migrations: %w", err)
	}
	slog.Info("postgres ready — migrations applied")

	// ── NATS JetStream ─────────────────────────────────────────────────────────
	natsConn, err := natsutil.Connect(natsutil.Config{
		URL:           cfg.NATS.URL,
		NKeyPath:      cfg.NATS.NKeyPath,
		TLSCertPath:   cfg.NATS.TLSCertPath,
		TLSKeyPath:    cfg.NATS.TLSKeyPath,
		TLSCAPath:     cfg.NATS.TLSCAPath,
		MaxReconnect:  cfg.NATS.MaxReconnect,
		ReconnectWait: cfg.NATS.ReconnectWait,
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("wire: connect to NATS: %w", err)
	}

	js, err := natsutil.JetStream(natsConn)
	if err != nil {
		return nil, fmt.Errorf("wire: init JetStream: %w", err)
	}

	if err := natsutil.EnsureStreams(js); err != nil {
		return nil, fmt.Errorf("wire: ensure NATS streams: %w", err)
	}
	slog.Info("NATS ready — streams and consumers configured")

	// ── Infraestructura ────────────────────────────────────────────────────────
	batchRepo := pginfra.NewSettlementBatchRepo(pool)
	idempotency := natsutil.NewIdempotencyStore(pool, "settlement")
	natsPub := natsutil.NewPublisher(js)
	eventPub := natsinfra.NewEventPublisher(natsPub)
	outboxStore := outbox.NewStore("settlement")
	outboxRelay := outbox.NewRelay(pool, js, "settlement", time.Second, 50)

	// SettlementProcessor — adaptador hacia el procesador externo (Visa/MC).
	// TODO: agregar el adaptador real cuando esté implementado.
	processor := newProcessor(cfg.Settlement.Processor)

	// ── Aplicación ─────────────────────────────────────────────────────────────
	batchHandler := command.NewBatchHandler(
		batchRepo,
		eventPub,
		outboxStore,
		processor,
		idempotency,
		pool,
		cfg.Settlement.MDRPercent,
	)
	settlementMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init settlement metrics: %w", err)
	}
	batchHandler.SetMetrics(settlementMetrics)
	businessLocation, err := time.LoadLocation(cfg.Settlement.BusinessTimezone)
	if err != nil {
		return nil, fmt.Errorf("wire: load business timezone: %w", err)
	}
	batchHandler.SetBusinessLocation(businessLocation)
	if err := command.RegisterBatchAttentionGauge(batchRepo, businessLocation); err != nil {
		return nil, fmt.Errorf("wire: register batch attention gauge: %w", err)
	}
	adminHandler := command.NewAdminHandler(batchRepo, processor)
	adminHandler.ConfigureSettlement(pool, eventPub, outboxStore, cfg.Settlement.MDRPercent)
	queryHandler := query.NewBatchQueryHandler(batchRepo)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────

	// NATS Subscriber — consume AuthApproved, ReversalCompleted, BatchCloseRequested
	subscriber := natsinfra.NewSubscriber(js, batchHandler)

	// gRPC Server — placeholder
	grpcSrv := grpcserver.NewSettlementServer()

	// HTTP Server — healthz, readyz, metrics, gestión de batches (operaciones)
	if cfg.Settlement.AdminAPIKey == "" {
		slog.Warn("SETTLEMENT_ADMIN_API_KEY is not configured — admin write operations are disabled")
	}
	router := httpinfra.NewRouter(queryHandler, adminHandler, pool, cfg.Settlement.AdminAPIKey, natsConn.IsConnected)
	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return &app{
		pool:       pool,
		natsConn:   natsConn,
		subscriber: subscriber,
		grpcSrv:    grpcSrv,
		grpcPort:   cfg.GRPCPort,
		httpSrv:    httpSrv,

		batchHandler:   batchHandler,
		batchCloseHour: cfg.Settlement.BatchCloseHour,

		outboxRelay: outboxRelay,
	}, nil
}

// newProcessor selecciona el procesador externo. Sin procesador (nil), los lotes
// quedan en CLOSED: BatchHandler y AdminHandler lo manejan como ErrProcessorUnavailable.
func newProcessor(mode string) service.SettlementProcessor {
	if mode == config.ProcessorMock {
		slog.Warn("settlement processor is a mock — batches are marked SUBMITTED without being sent")
		return stprocessor.NewMockProcessor()
	}
	slog.Warn("no settlement processor configured — closed batches will not be submitted")
	return nil
}

// start arranca todos los servidores en goroutines independientes.
// Incluye el job de cierre automático de lotes si BatchCloseHour > 0.
func (a *app) start(ctx context.Context) error {
	// NATS consumers
	if err := a.subscriber.Subscribe(); err != nil {
		return fmt.Errorf("start: subscribe NATS consumers: %w", err)
	}
	slog.Info("NATS consumers active",
		slog.String("consumers", "settlement-auth, settlement-reversal, settlement-batch"),
	)

	// Outbox relay — publica batch-closed y settlement.completed, con reintentos
	a.relayWG.Add(1)
	go func() {
		defer a.relayWG.Done()
		a.outboxRelay.Run(ctx)
	}()
	slog.Info("outbox relay active")

	// gRPC server
	go func() {
		if err := grpcserver.Start(a.grpcSrv, a.grpcPort); err != nil {
			slog.Error("gRPC server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("gRPC server starting", slog.Int("port", a.grpcPort))

	// HTTP server
	go func() {
		if err := a.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	// Job de cierre automático de lotes — corre a BatchCloseHour UTC cada día
	go a.runBatchCloser(ctx)
	slog.Info("batch closer job active")

	return nil
}

// runBatchCloser es el job periódico de cierre automático de lotes. Revisa cada
// minuto, y también al arrancar, para no perder la corrida si el servicio estaba
// caído a la hora de cierre. Reintenta mientras alguna corrida del día tenga fallas.
func (a *app) runBatchCloser(ctx context.Context) {
	if a.batchCloseHour == 0 {
		slog.Info("automatic batch close disabled")
		return
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastRunDate string
	run := func(now time.Time) {
		utc := now.UTC()
		today := utc.Format("2006-01-02")
		if utc.Hour() < a.batchCloseHour || lastRunDate == today {
			return
		}

		closed, failed, err := a.batchHandler.CloseStaleBatches(ctx, utc)
		if err != nil {
			slog.Error("automatic batch close failed", slog.String("error", err.Error()))
			return
		}
		slog.Info("automatic batch close finished",
			slog.String("date", today),
			slog.Int("closed", closed),
			slog.Int("failed", failed),
		)
		if failed == 0 {
			lastRunDate = today
		}
	}

	run(time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			run(now)
		}
	}
}

// close libera todos los recursos en orden inverso al de creación.
func (a *app) close() {
	slog.Info("closing resources")

	if a.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.httpSrv.Shutdown(ctx)
	}

	if a.grpcSrv != nil {
		a.grpcSrv.Stop()
	}

	// El contexto ya se canceló: el Relay termina su ciclo y sale.
	a.relayWG.Wait()

	if a.natsConn != nil {
		_ = a.natsConn.Drain()
		a.natsConn.Close()
	}

	if a.pool != nil {
		a.pool.Close()
	}

	slog.Info("all resources closed")
}
