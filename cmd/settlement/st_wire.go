package main

import (
	"context"
	"crypto/tls"
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
	httpSrv    *http.Server

	batchHandler   *command.BatchHandler
	batchCloseHour int
	adminTokens    *httpinfra.TokenStore

	outboxRelay *outbox.Relay
	// relayWG espera al Relay en el apagado, antes de cerrar NATS y Postgres.
	relayWG sync.WaitGroup

	closeOnce sync.Once
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
	processor := newProcessor(cfg.Settlement.Processor, cfg.Settlement.SubmitRetries, cfg.Settlement.SubmitTimeout)

	// ── Aplicación ─────────────────────────────────────────────────────────────
	batchHandler := command.NewBatchHandler(
		batchRepo,
		eventPub,
		outboxStore,
		processor,
		idempotency,
		pool,
	)
	settlementMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init settlement metrics: %w", err)
	}
	batchHandler.SetMetrics(settlementMetrics)
	batchHandler.SetReversalReviews(batchRepo)
	batchHandler.SetMerchantTimezones(batchRepo)
	batchHandler.SetMaxCloseAge(cfg.Settlement.CloseMaxAgeDays)
	batchHandler.SetCloseGrace(cfg.Settlement.BatchCloseGrace)
	batchHandler.SetBatchReopens(batchRepo)
	batchHandler.SetAuditLog(batchRepo)
	batchHandler.SetTerminalDayBatches(batchRepo)
	batchHandler.SetBatchSubmissions(batchRepo)
	batchHandler.SetPendingTerminalCloses(batchRepo)
	businessLocation, err := time.LoadLocation(cfg.Settlement.BusinessTimezone)
	if err != nil {
		return nil, fmt.Errorf("wire: load business timezone: %w", err)
	}
	batchHandler.SetBusinessLocation(businessLocation)
	if err := command.RegisterBatchAttentionGauge(batchRepo, businessLocation, cfg.Settlement.BatchCloseGrace); err != nil {
		return nil, fmt.Errorf("wire: register batch attention gauge: %w", err)
	}
	adminHandler := command.NewAdminHandler(batchRepo, processor)
	adminHandler.SetMetrics(settlementMetrics)
	adminHandler.RequireReason(cfg.Settlement.RequireReason)
	if !cfg.Settlement.RequireReason {
		slog.Warn("force-close, resubmit and settle accept requests without a reason — set SETTLEMENT_REQUIRE_REASON=true once clients send it")
	}
	adminHandler.ConfigureSettlement(pool, batchRepo, eventPub, outboxStore, cfg.Settlement.MDRPercent)
	adminHandler.SetReversalReviews(batchRepo)
	adminHandler.SetBatchSubmissions(batchRepo)
	adminHandler.SetMerchantTimezones(batchRepo)
	adminHandler.SetBusinessLocation(businessLocation)
	adminHandler.SetBatchReleaser(batchHandler)
	queryHandler := query.NewBatchQueryHandler(batchRepo)
	queryHandler.ConfigureReviews(batchRepo)
	queryHandler.ConfigureAudit(batchRepo)
	queryHandler.ConfigureTimezones(batchRepo, cfg.Settlement.BusinessTimezone)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────

	// NATS Subscriber — consume AuthApproved, ReversalCompleted, BatchCloseRequested
	subscriber := natsinfra.NewSubscriber(js, batchHandler)
	deadLetters := natsinfra.NewDeadLetterQueue(js, subscriber)
	if err := command.RegisterDeadLetterGauge(deadLetters); err != nil {
		return nil, fmt.Errorf("wire: dead letter gauge: %w", err)
	}

	// HTTP Server — healthz, readyz, metrics, gestión de batches (operaciones)
	adminTokens, err := httpinfra.NewTokenStore(cfg.Settlement.AdminAPIKey, cfg.Settlement.AdminTokensFile)
	if err != nil {
		return nil, fmt.Errorf("wire: load admin tokens: %w", err)
	}
	if !adminTokens.Configured() {
		slog.Warn("no admin tokens configured — the batch API is disabled")
	}
	if adminTokens.UsesLegacyKey() {
		slog.Warn("SETTLEMENT_ADMIN_API_KEY is enabled — it has the highest role and does not identify the operator")
	}
	trustedProxies, err := cfg.Settlement.TrustedProxyPrefixes()
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	router := httpinfra.NewRouter(queryHandler, adminHandler, pool,
		httpinfra.Security{Tokens: adminTokens, TrustedProxies: trustedProxies},
		natsConn.IsConnected, batchRepo.MissingTables, deadLetters)
	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	if cfg.Settlement.TLSCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.Settlement.TLSCert, cfg.Settlement.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("wire: load TLS certificate: %w", err)
		}
		httpSrv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	} else if !cfg.Settlement.TLSExternal {
		slog.Warn("the HTTP API has no TLS — admin tokens travel unencrypted")
	}

	return &app{
		pool:       pool,
		natsConn:   natsConn,
		subscriber: subscriber,
		httpSrv:    httpSrv,

		batchHandler:   batchHandler,
		batchCloseHour: cfg.Settlement.BatchCloseHour,
		adminTokens:    adminTokens,

		outboxRelay: outboxRelay,
	}, nil
}

// newProcessor selecciona el procesador externo, con un plazo por intento y reintentos. Sin
// procesador (nil), los lotes quedan en CLOSED: BatchHandler y AdminHandler lo manejan como
// ErrProcessorUnavailable.
func newProcessor(mode string, retries int, timeout time.Duration) service.SettlementProcessor {
	if mode == config.ProcessorMock {
		slog.Warn("settlement processor is a mock — batches are marked SUBMITTED without being sent")
		return stprocessor.NewRetryingProcessor(stprocessor.NewMockProcessor(), retries, timeout)
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

	// HTTP server
	go func() {
		serve := a.httpSrv.ListenAndServe
		if a.httpSrv.TLSConfig != nil {
			// El certificado ya está cargado en TLSConfig.
			serve = func() error { return a.httpSrv.ListenAndServeTLS("", "") }
		}
		if err := serve(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	// Tokens de la API — se pueden cambiar sin reiniciar
	go a.adminTokens.Run(ctx, adminTokensReload)

	// Job de cierre automático de lotes — revisa cada 15 minutos, todo el día
	go a.runBatchCloser(ctx)
	slog.Info("batch closer job active")

	// Reintento de las liberaciones fallidas — aparte del cierre: corre aunque esté deshabilitado
	go a.runHeldReleaser(ctx)
	slog.Info("held batch release retry active", slog.Duration("every", heldReleaseRetry))

	return nil
}

// adminTokensReload es cada cuánto se relee el archivo de tokens: lo que tarda en regir una
// baja o una rotación.
const adminTokensReload = 30 * time.Second

// batchCloseSweep es cada cuánto se buscan lotes vencidos. Corre todo el día porque cada
// comercio tiene su medianoche, y hay zonas con desfasajes de 15, 30 y 45 minutos.
const batchCloseSweep = 15 * time.Minute

// runBatchCloser es el job periódico de cierre automático de lotes. Comprueba cada
// minuto, y también al arrancar, para no perder la corrida si el servicio estaba
// caído a la hora de cierre; barre una vez por cada tramo de batchCloseSweep. Reintenta
// mientras la última corrida tenga fallas. Cada lote se cierra después de la medianoche de su
// comercio más el plazo configurado: eso lo decide la consulta, no la hora del job.
func (a *app) runBatchCloser(ctx context.Context) {
	if a.batchCloseHour == 0 {
		slog.Info("automatic batch close disabled")
		return
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastSweep time.Time
	run := func(now time.Time) {
		utc := now.UTC()
		sweep := utc.Truncate(batchCloseSweep)
		if sweep.Equal(lastSweep) {
			return
		}

		closed, failed, err := a.batchHandler.CloseStaleBatches(ctx, utc)
		if err != nil {
			slog.Error("automatic batch close failed", slog.String("error", err.Error()))
			return
		}
		if closed > 0 || failed > 0 {
			slog.Info("automatic batch close finished",
				slog.Time("sweep", sweep),
				slog.Int("closed", closed),
				slog.Int("failed", failed),
			)
		}
		if failed == 0 {
			lastSweep = sweep
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

// heldReleaseRetry es cada cuánto se reintenta liberar los lotes retenidos cuyas revisiones ya se
// cerraron: lo que puede tardar en enviarse un lote cuya liberación falló.
const heldReleaseRetry = 5 * time.Minute

// runHeldReleaser reintenta liberar los lotes que no se liberaron al cerrar su última revisión,
// al arrancar y cada heldReleaseRetry. Corre aparte del cierre automático: con
// SETTLEMENT_BATCH_CLOSE_HOUR=0, o si el barrido de cierre falla, los lotes no quedan DISPUTED
// hasta un resolve-dispute.
func (a *app) runHeldReleaser(ctx context.Context) {
	run := func() {
		retried, failed, err := a.batchHandler.ReleaseHeldBatches(ctx)
		if err != nil {
			slog.Error("held batch release retry failed", slog.String("error", err.Error()))
			return
		}
		if retried > 0 {
			slog.Info("held batch release retry finished", slog.Int("retried", retried), slog.Int("failed", failed))
		}
	}
	ticker := time.NewTicker(heldReleaseRetry)
	defer ticker.Stop()
	run()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
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
