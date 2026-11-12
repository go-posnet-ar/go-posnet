package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/notification/application/command"
	"github.com/juantevez/go-posnet/context/notification/application/query"
	"github.com/juantevez/go-posnet/context/notification/config"
	grpcclient "github.com/juantevez/go-posnet/context/notification/infrastructure/grpc/client"
	grpcserver "github.com/juantevez/go-posnet/context/notification/infrastructure/grpc/server"
	httpinfra "github.com/juantevez/go-posnet/context/notification/infrastructure/http"
	natsinfra "github.com/juantevez/go-posnet/context/notification/infrastructure/nats"
	pginfra "github.com/juantevez/go-posnet/context/notification/infrastructure/postgres"
	"github.com/juantevez/go-posnet/context/notification/infrastructure/webhook"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	natsclient "github.com/nats-io/nats.go"
)

// app agrupa todos los componentes del servicio y sus recursos abiertos.
type app struct {
	pool       *pgxpool.Pool
	natsConn   *natsclient.Conn
	grpcClient *grpcclient.TerminalGatewayClient // único cliente gRPC del sistema
	subscriber *natsinfra.Subscriber
	grpcSrv    *grpcserver.NotificationServer
	httpSrv    *http.Server
	// Referencias para el job de reintentos
	notifRepo     *pginfra.NotificationRepo
	notifyHandler *command.NotifyHandler
	retryCfg      config.RetryConfig

	closeOnce sync.Once
}

// wire construye el grafo de dependencias completo del BC Notification.
// Orden: infraestructura → repositorios → dispatchers → handlers → adaptadores.
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

	// ── gRPC Client → Terminal Gateway ────────────────────────────────────────
	// Único cliente gRPC del sistema — Notification llama a Terminal Gateway
	// para entregar el comprobante al WebSocket del terminal.
	g := cfg.GRPCClient
	grpcTLS, err := grpcclient.TLSConfig(g.TLSCAPath, g.TLSCertPath, g.TLSKeyPath, g.TLSServerName)
	if err != nil {
		return nil, fmt.Errorf("wire: terminal gateway gRPC TLS: %w", err)
	}
	if grpcTLS == nil {
		slog.Warn("the terminal gateway gRPC client has no TLS — receipts travel unencrypted")
	}
	grpcClient, err := grpcclient.NewTerminalGatewayClient(g.TerminalGatewayAddr, grpcTLS)
	if err != nil {
		return nil, fmt.Errorf("wire: init terminal gateway gRPC client: %w", err)
	}
	slog.Info("gRPC client connected",
		slog.String("target", g.TerminalGatewayAddr),
		slog.Bool("tls", grpcTLS != nil),
	)

	// ── Infraestructura ────────────────────────────────────────────────────────
	notifRepo := pginfra.NewNotificationRepo(pool)
	idempotency := natsutil.NewIdempotencyStore(pool, "notification")
	natsPub := natsutil.NewPublisher(js)
	eventPub := natsinfra.NewEventPublisher(natsPub)

	// Webhook dispatcher — envía HTTP POST al endpoint del comercio
	webhookDispatcher := webhook.NewDispatcher(
		cfg.Webhook.Timeout,
		cfg.Webhook.DefaultEndpoint,
	)

	// ── Aplicación ─────────────────────────────────────────────────────────────
	notifyHandler := command.NewNotifyHandler(
		notifRepo,
		grpcClient,        // TerminalNotifier — implementado por el gRPC client
		webhookDispatcher, // WebhookDispatcher — implementado por webhook.Dispatcher
		eventPub,
		idempotency,
		pool,
	)
	notifMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init notification metrics: %w", err)
	}
	notifyHandler.SetMetrics(notifMetrics)
	adminHandler := command.NewAdminHandler(notifRepo, notifyHandler)
	queryHandler := query.NewNotificationQueryHandler(notifRepo)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────

	// NATS Subscriber — consume AuthApproved, AuthRejected, BatchClosed
	subscriber := natsinfra.NewSubscriber(js, notifyHandler)

	// gRPC Server — placeholder (Notification solo actúa como cliente gRPC)
	grpcSrv := grpcserver.NewNotificationServer()

	// HTTP Server — healthz, readyz, metrics, gestión de notificaciones (operaciones)
	router := httpinfra.NewRouter(queryHandler, adminHandler, pool)
	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	return &app{
		pool:          pool,
		natsConn:      natsConn,
		grpcClient:    grpcClient,
		subscriber:    subscriber,
		grpcSrv:       grpcSrv,
		httpSrv:       httpSrv,
		notifRepo:     notifRepo,
		notifyHandler: notifyHandler,
		retryCfg:      cfg.Retry,
	}, nil
}

// start arranca todos los servidores y jobs en goroutines independientes.
func (a *app) start(ctx context.Context) error {
	// NATS consumers — consume eventos de Authorization y Settlement
	if err := a.subscriber.Subscribe(); err != nil {
		return fmt.Errorf("start: subscribe NATS consumers: %w", err)
	}
	slog.Info("NATS consumers active",
		slog.String("consumers", "notify-auth-approved, notify-auth-rejected, notify-batch-closed, notify-settlement-completed"),
	)

	// gRPC server — placeholder
	go func() {
		if err := grpcserver.Start(a.grpcSrv, 9094); err != nil {
			slog.Error("gRPC server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("gRPC server starting", slog.Int("port", 9094))

	// HTTP server
	go func() {
		if err := a.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	// Job de reintentos — procesa notificaciones RETRYING cuyo next_retry_at pasó
	go a.runRetryJob(ctx)
	slog.Info("retry job active",
		slog.Duration("interval", a.retryCfg.JobInterval),
		slog.Int("batch_size", a.retryCfg.BatchSize),
	)

	return nil
}

// runRetryJob procesa periódicamente las notificaciones pendientes de reintento.
// Corre en background y se detiene cuando el contexto es cancelado.
func (a *app) runRetryJob(ctx context.Context) {
	ticker := time.NewTicker(a.retryCfg.JobInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, err := a.notifRepo.FindPendingRetries(ctx, a.retryCfg.BatchSize)
			if err != nil {
				slog.Error("retry job: find pending retries failed",
					slog.String("error", err.Error()),
				)
				continue
			}

			if len(pending) == 0 {
				continue
			}

			slog.Info("retry job: processing pending retries",
				slog.Int("count", len(pending)),
			)

			for _, n := range pending {
				// Cada reintento corre en su propia goroutine para no bloquear el job
				n := n
				go func() {
					if err := a.notifyHandler.RetryFailed(ctx, n.ID()); err != nil {
						slog.Error("retry job: retry failed",
							slog.String("notification_id", n.ID()),
							slog.String("error", err.Error()),
						)
					}
				}()
			}
		}
	}
}

// close libera todos los recursos en orden inverso al de creación.
// close libera los recursos. Es idempotente: run lo llama al apagar y también en un defer, por
// si el arranque falla.
func (a *app) close() {
	a.closeOnce.Do(a.closeResources)
}

func (a *app) closeResources() {
	slog.Info("closing resources")

	if a.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.httpSrv.Shutdown(ctx)
	}

	// Cerrar el cliente gRPC antes que NATS
	if a.grpcClient != nil {
		if err := a.grpcClient.Close(); err != nil {
			slog.Error("failed to close gRPC client", slog.String("error", err.Error()))
		}
	}

	if a.natsConn != nil {
		_ = a.natsConn.Drain()
		a.natsConn.Close()
	}

	if a.pool != nil {
		a.pool.Close()
	}

	slog.Info("all resources closed")
}
