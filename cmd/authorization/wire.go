package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/authorization/application/command"
	"github.com/juantevez/go-posnet/context/authorization/application/query"
	"github.com/juantevez/go-posnet/context/authorization/config"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/carddata"
	grpcserver "github.com/juantevez/go-posnet/context/authorization/infrastructure/grpc/server"
	httpinfra "github.com/juantevez/go-posnet/context/authorization/infrastructure/http"
	isoclient "github.com/juantevez/go-posnet/context/authorization/infrastructure/iso8583"
	natsinfra "github.com/juantevez/go-posnet/context/authorization/infrastructure/nats"
	pginfra "github.com/juantevez/go-posnet/context/authorization/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	natsclient "github.com/nats-io/nats.go"
)

// app agrupa todos los componentes del servicio y sus recursos abiertos.
type app struct {
	pool       *pgxpool.Pool    // tipo concreto de pgx — no pgutil.Pool
	natsConn   *natsclient.Conn // tipo concreto de nats.go — no natsutil.Conn
	subscriber *natsinfra.Subscriber
	grpcSrv    *grpcserver.AuthorizationServer
	httpSrv    *http.Server

	authHandler    *command.AuthorizationHandler
	acquirerClient *isoclient.Client // nil con el mock
	stopAcquirer   context.CancelFunc
	acquirerWG     sync.WaitGroup
	reconcileEvery time.Duration
	stuckAfter     time.Duration
	callTimeout    time.Duration
	stopReconciler context.CancelFunc
	reconcilerWG   sync.WaitGroup

	invalidRetention time.Duration
	stopPurge        context.CancelFunc
	purgeWG          sync.WaitGroup

	closeOnce sync.Once

	grpcPort int
	// missingSchema informa lo que falta en la base: sin eso, el servicio no arranca.
	missingSchema func(context.Context) ([]string, error)
	grpcStarted   bool
}

// wire construye el grafo de dependencias completo del BC Authorization.
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
		natsConn.Close()
		pool.Close()
		return nil, fmt.Errorf("wire: init JetStream: %w", err)
	}

	if err := natsutil.EnsureStreams(js); err != nil {
		natsConn.Close()
		pool.Close()
		return nil, fmt.Errorf("wire: ensure NATS streams: %w", err)
	}

	slog.Info("NATS ready — streams and consumers configured")

	// ── Infraestructura ────────────────────────────────────────────────────────
	txRepo := pginfra.NewTransactionRepo(pool)
	blockedCards := pginfra.NewBlockedCardRepo(pool)
	idempotency := natsutil.NewIdempotencyStore(pool, "pn_authorization")
	natsPub := natsutil.NewPublisher(js)
	eventPub := natsinfra.NewEventPublisher(natsPub)

	// ── Aplicación ─────────────────────────────────────────────────────────────
	acquirerGW, acquirerClient, err := newAcquirer(cfg)
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	authHandler := command.NewAuthorizationHandler(
		txRepo,
		acquirerGW,
		eventPub,
		idempotency,
		pool,
	)
	authMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init authorization metrics: %w", err)
	}
	authHandler.SetMetrics(authMetrics)
	authHandler.SetBlockedCards(blockedCards)
	acquirerTimeout := time.Duration(cfg.Acquirer.TimeoutSeconds) * time.Second
	authHandler.ConfigureAcquirerCalls(cfg.Acquirer.MaxConcurrent, acquirerTimeout)
	if cfg.Acquirer.MockScenarios {
		slog.Warn("acquirer mock scenarios enabled — some amounts are not approved")
	}
	if err := command.RegisterUnresolvedGauge(txRepo, cfg.Acquirer.StuckAfter, acquirerTimeout); err != nil {
		return nil, fmt.Errorf("wire: register unresolved gauge: %w", err)
	}
	queryHandler := query.NewTransactionQueryHandler(txRepo)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────
	subscriber := natsinfra.NewSubscriber(js, authHandler)
	grpcSrv := grpcserver.NewAuthorizationServer(queryHandler)

	router := httpinfra.NewRouter(queryHandler, pool, httpinfra.Readiness{
		MissingSchema: txRepo.MissingSchema,
		NATSConnected: natsConn.IsConnected,
	})
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
		httpSrv:    httpSrv,

		authHandler:    authHandler,
		acquirerClient: acquirerClient,
		reconcileEvery: cfg.Acquirer.ReconcileEvery,
		stuckAfter:     cfg.Acquirer.StuckAfter,
		callTimeout:    acquirerTimeout,

		invalidRetention: cfg.InvalidTransactionsRetention,

		grpcPort:      cfg.GRPCPort,
		missingSchema: txRepo.MissingSchema,
	}, nil
}

// newAcquirer elige el adquirente. Devuelve el cliente ISO 8583 aparte, porque hay que iniciarlo.
func newAcquirer(cfg *config.Config) (service.AcquirerGateway, *isoclient.Client, error) {
	if cfg.Acquirer.Mode != config.AcquirerModeISO8583 {
		return pginfra.NewMockAcquirerGateway(cfg.Acquirer.MockScenarios), nil, nil
	}
	slog.Warn("card data comes from a stub with test cards — replace it with card-management-service")
	client := isoclient.NewClient(isoclient.Config{
		Address:    net.JoinHostPort(cfg.Acquirer.Host, strconv.Itoa(cfg.Acquirer.Port)),
		AcquirerID: cfg.Acquirer.InstitutionID,
		EchoEvery:  acquirerEchoEvery,
	}, carddata.NewStub())
	if err := client.RegisterConnectedGauge(); err != nil {
		return nil, nil, fmt.Errorf("register acquirer gauge: %w", err)
	}
	return client, client, nil
}

// acquirerEchoEvery es cada cuánto se prueba la conexión con el host.
const acquirerEchoEvery = 30 * time.Second

// start arranca todos los servidores en goroutines independientes.
func (a *app) start(ctx context.Context) error {
	// Sin una migración, cada evento fallaría hasta agotar sus reintentos, y se perdería. Sin
	// consumir, quedan en NATS hasta que se aplique.
	if a.missingSchema != nil {
		missing, err := a.missingSchema(ctx)
		if err != nil {
			return fmt.Errorf("start: check the database schema: %w", err)
		}
		if len(missing) > 0 {
			return fmt.Errorf("start: database schema incomplete — apply the migrations before starting (see AUTHORIZATION.md §9): %v", missing)
		}
	}

	// Si el puerto gRPC está ocupado, el servicio no arranca.
	grpcLis, err := grpcserver.Listen(a.grpcPort)
	if err != nil {
		return fmt.Errorf("start: %w", err)
	}
	a.grpcStarted = true

	// Antes de consumir eventos, para que las primeras ventas ya tengan conexión.
	if a.acquirerClient != nil {
		acquirerCtx, stop := context.WithCancel(ctx)
		a.stopAcquirer = stop
		a.acquirerWG.Add(1)
		go func() {
			defer a.acquirerWG.Done()
			a.acquirerClient.Run(acquirerCtx)
		}()
	}

	if err := a.subscriber.Subscribe(); err != nil {
		return fmt.Errorf("start: subscribe NATS consumers: %w", err)
	}
	slog.Info("NATS consumers active")

	go func() {
		if err := a.grpcSrv.Serve(grpcLis); err != nil {
			slog.Error("gRPC server stopped", slog.String("error", err.Error()))
		}
	}()

	go func() {
		if err := a.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	if a.reconcileEvery > 0 {
		reconcileCtx, stop := context.WithCancel(ctx)
		a.stopReconciler = stop
		a.reconcilerWG.Add(1)
		go func() {
			defer a.reconcilerWG.Done()
			a.runReconciler(reconcileCtx)
		}()
		slog.Info("reconciler active", slog.Duration("every", a.reconcileEvery), slog.Duration("stuck_after", a.stuckAfter))
	} else {
		slog.Warn("reconciler disabled — unresolved transactions will not be reversed")
	}

	if a.invalidRetention > 0 {
		purgeCtx, stop := context.WithCancel(ctx)
		a.stopPurge = stop
		a.purgeWG.Add(1)
		go func() {
			defer a.purgeWG.Done()
			a.runInvalidPurge(purgeCtx)
		}()
		slog.Info("invalid transactions purge active", slog.Duration("retention", a.invalidRetention))
	} else {
		slog.Warn("invalid transactions purge disabled — the table will grow without limit")
	}

	return nil
}

// invalidPurgeEvery es cada cuánto se purgan las transacciones inválidas. La retención se mide en
// días, así que no hace falta más precisión.
const invalidPurgeEvery = time.Hour

// runInvalidPurge borra periódicamente el registro vencido de las transacciones inválidas. También
// corre al arrancar, para no depender de que una instancia viva más que el intervalo. Si falla,
// reintenta en la corrida siguiente: lo vencido sigue ahí.
func (a *app) runInvalidPurge(ctx context.Context) {
	run := func() {
		deleted, err := a.authHandler.PurgeInvalidRejections(ctx, time.Now(), a.invalidRetention)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("invalid transactions purge failed", slog.String("error", err.Error()))
			}
			return
		}
		if deleted > 0 {
			slog.Info("invalid transactions purged", slog.Int64("deleted", deleted))
		}
	}

	run()
	ticker := time.NewTicker(invalidPurgeEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

// runReconciler resuelve periódicamente las transacciones sin un resultado conocido. También
// corre al arrancar: lo que quedó trabado por una caída es justo lo que tiene que encontrar.
func (a *app) runReconciler(ctx context.Context) {
	run := func() {
		resolved, failed, err := a.authHandler.ReconcileUnresolved(ctx, time.Now(), a.stuckAfter)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("reconciler run failed", slog.String("error", err.Error()))
			}
			return
		}
		if resolved > 0 || failed > 0 {
			slog.Info("reconciler run finished", slog.Int("resolved", resolved), slog.Int("failed", failed))
		}
	}

	run()
	ticker := time.NewTicker(a.reconcileEvery)
	defer ticker.Stop()
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

	// Antes de cerrar la base: una reversa interrumpida sigue en INDETERMINATE y se retoma.
	if a.stopReconciler != nil {
		a.stopReconciler()
		a.reconcilerWG.Wait()
	}
	if a.stopPurge != nil {
		a.stopPurge()
		a.purgeWG.Wait()
	}

	if a.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.httpSrv.Shutdown(ctx)
	}
	if a.grpcSrv != nil && a.grpcStarted {
		a.grpcSrv.GracefulStop(10 * time.Second)
	}

	if a.natsConn != nil {
		_ = a.natsConn.Drain()
		a.natsConn.Close()
	}

	// Con NATS cerrado no entran más. Lo que no termine a tiempo sigue en PROCESSING, o con
	// su pedido de reversa guardado, y lo retoma el conciliador.
	if a.authHandler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.callTimeout+5*time.Second)
		defer cancel()
		a.authHandler.WaitAcquirerCalls(ctx)
	}
	if a.stopAcquirer != nil {
		a.stopAcquirer()
		a.acquirerWG.Wait()
	}

	if a.pool != nil {
		a.pool.Close()
	}

	slog.Info("all resources closed")
}
