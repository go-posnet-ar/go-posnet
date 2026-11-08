package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/fraud-detection/application/command"
	"github.com/juantevez/go-posnet/context/fraud-detection/application/query"
	"github.com/juantevez/go-posnet/context/fraud-detection/config"
	"github.com/juantevez/go-posnet/context/fraud-detection/domain/service"
	grpcserver "github.com/juantevez/go-posnet/context/fraud-detection/infrastructure/grpc/server"
	httpinfra "github.com/juantevez/go-posnet/context/fraud-detection/infrastructure/http"
	natsinfra "github.com/juantevez/go-posnet/context/fraud-detection/infrastructure/nats"
	pginfra "github.com/juantevez/go-posnet/context/fraud-detection/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/metric"
)

// app agrupa todos los componentes del servicio y sus recursos abiertos.
type app struct {
	pool       *pgxpool.Pool
	natsConn   *nats.Conn
	subscriber *natsinfra.FD_Subscriber
	grpcSrv    *grpcserver.FraudDetectionServer
	httpSrv    *http.Server

	// missingSchema devuelve las tablas que faltan; sin ellas el servicio no arranca.
	missingSchema func(context.Context) ([]string, error)

	outboxRelay *outbox.Relay
	stopRelay   context.CancelFunc
	relayWG     sync.WaitGroup

	closeOnce sync.Once
}

// Outbox de Fraud Detection. El relay corre seguido: Authorization espera el score con la venta
// en curso, y cada ciclo agrega hasta outboxRelayEvery a la respuesta.
const (
	outboxSchema     = "fraud_detection"
	outboxRelayEvery = 200 * time.Millisecond
	outboxRelayBatch = 100
	// outboxStuckAfter es desde cuándo un score sin publicar cuenta como trabado.
	outboxStuckAfter  = time.Minute
	gaugeQueryTimeout = 2 * time.Second
)

// wire construye el grafo de dependencias completo del BC Fraud Detection.
// Orden: infraestructura → repositorios → motor de reglas → handlers → adaptadores.
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
	fraudCaseRepo := pginfra.NewFraudCaseRepo(pool)
	ruleRepo := pginfra.NewFraudRuleRepo(pool, cfg.Engine.RulesCacheTTL)
	historyRepo := pginfra.NewTransactionHistoryRepo(pool)
	idempotency := natsutil.NewIdempotencyStore(pool, "fraud_detection")
	natsPub := natsutil.NewPublisher(js)
	// El score se escribe en el outbox, en la transacción que guarda el caso, y el relay lo publica.
	eventPublisher := natsinfra.NewEventPublisher(natsPub, outbox.NewStore(outboxSchema))
	outboxRelay := outbox.NewRelay(pool, js, outboxSchema, outboxRelayEvery, outboxRelayBatch)
	if err := registerGauges(fraudCaseRepo, ruleRepo); err != nil {
		return nil, fmt.Errorf("wire: register gauges: %w", err)
	}

	// ── Motor de reglas ────────────────────────────────────────────────────────
	// El RuleEngine es el Domain Service más importante del BC.
	// Recibe el timeout configurado desde config.Engine.EvalTimeout.
	engine := service.NewRuleEngine(ruleRepo, historyRepo, cfg.Engine.EvalTimeout)
	if timeouts, err := observability.Meter("posnet.fraud_detection").Int64Counter(
		"posnet_fraud_engine_timeouts",
		metric.WithDescription("Evaluaciones del motor terminadas por EvalTimeout."),
	); err == nil {
		engine.SetTimeoutCounter(timeouts)
	}

	// Precalentar el cache de reglas al arrancar para evitar latencia en la
	// primera transacción. Si falla, el motor las cargará en la primera eval.
	if rules, err := ruleRepo.FindAllActive(ctx); err == nil && len(rules) == 0 {
		slog.Warn("no active fraud rules — the fraud check is off: every transaction is approved with score 0")
	} else if err == nil {
		slog.Info("fraud rules loaded", slog.Int("count", len(rules)))
	} else {
		slog.Warn("could not preload fraud rules — will load on first evaluation",
			slog.String("error", err.Error()),
		)
	}

	// ── Aplicación ─────────────────────────────────────────────────────────────
	evaluateHandler := command.NewEvaluateTransactionHandler(
		fraudCaseRepo,
		engine,
		eventPublisher,
		idempotency,
		pool,
	)
	fraudMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init fraud metrics: %w", err)
	}
	evaluateHandler.SetMetrics(fraudMetrics)
	adminHandler := command.NewAdminHandler(ruleRepo, fraudCaseRepo)
	queryHandler := query.NewFraudQueryHandler(fraudCaseRepo, ruleRepo)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────

	// NATS Subscriber — consume FraudCheckRequested y publica FraudScoreCalculated
	subscriber := natsinfra.NewSubscriber(js, evaluateHandler)

	// gRPC Server — placeholder para servicios futuros
	grpcSrv := grpcserver.NewFraudDetectionServer()

	// HTTP Server — healthz, readyz, metrics, gestión de reglas (admin)
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
		subscriber:    subscriber,
		grpcSrv:       grpcSrv,
		httpSrv:       httpSrv,
		missingSchema: fraudCaseRepo.MissingSchema,
		outboxRelay:   outboxRelay,
	}, nil
}

// registerGauges publica los scores trabados en el outbox y las reglas activas. Si una consulta
// falla, la serie no se informa en ese scrape: un cero la ocultaría.
func registerGauges(cases *pginfra.FraudCaseRepo, rules *pginfra.FraudRuleRepo) error {
	meter := observability.Meter("posnet.fraud_detection")
	stuck, err := meter.Int64ObservableGauge("posnet_fraud_outbox_stuck",
		metric.WithDescription("Scores que esperan en el outbox desde hace más de un minuto."))
	if err != nil {
		return err
	}
	active, err := meter.Int64ObservableGauge("posnet_fraud_active_rules",
		metric.WithDescription("Reglas de fraude activas. En 0, el control está apagado: todo se aprueba con score 0."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, gaugeQueryTimeout)
		defer cancel()
		if n, err := cases.CountOutboxStuck(ctx, time.Now().Add(-outboxStuckAfter)); err == nil {
			o.ObserveInt64(stuck, n)
		} else {
			slog.ErrorContext(ctx, "failed to count stuck outbox events", slog.String("error", err.Error()))
		}
		if n, err := rules.ActiveCount(ctx); err == nil {
			o.ObserveInt64(active, int64(n))
		} else {
			slog.ErrorContext(ctx, "failed to count active fraud rules", slog.String("error", err.Error()))
		}
		return nil
	}, stuck, active)
	return err
}

// start arranca todos los servidores en goroutines independientes.
func (a *app) start(ctx context.Context) error {
	// Sin el outbox, cada evento fallaría hasta agotar sus reintentos, y se perdería. Sin consumir,
	// quedan en NATS hasta que se aplique la migración.
	if a.missingSchema != nil {
		missing, err := a.missingSchema(ctx)
		if err != nil {
			return fmt.Errorf("start: check the database schema: %w", err)
		}
		if len(missing) > 0 {
			return fmt.Errorf("start: database schema incomplete — apply the migrations before starting (see FRAUD-DETECTION.md §10): %v", missing)
		}
	}

	// Publica lo que el outbox tenga pendiente, también lo que quedó de una ejecución anterior.
	if a.outboxRelay != nil {
		relayCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
		a.stopRelay = stop
		a.relayWG.Add(1)
		go func() {
			defer a.relayWG.Done()
			a.outboxRelay.Run(relayCtx)
		}()
		slog.Info("outbox relay active", slog.Duration("every", outboxRelayEvery))
	}

	// NATS consumer — consume FraudCheckRequested
	if err := a.subscriber.Subscribe(); err != nil {
		return fmt.Errorf("start: subscribe NATS consumers: %w", err)
	}
	slog.Info("NATS consumer active",
		slog.String("durable", "fraud-check-consumer"),
	)

	// gRPC server
	go func() {
		if err := grpcserver.Start(a.grpcSrv, 9092); err != nil {
			slog.Error("gRPC server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("gRPC server starting", slog.Int("port", 9092))

	// HTTP server
	go func() {
		if err := a.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	return nil
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

	// Drain espera a los handlers en curso; el relay sigue publicando sus scores mientras la
	// conexión siga abierta. Lo que no llegue a salir queda en el outbox para el próximo arranque.
	if a.natsConn != nil {
		_ = a.natsConn.Drain()
		a.natsConn.Close()
	}
	if a.stopRelay != nil {
		a.stopRelay()
		a.relayWG.Wait()
	}

	if a.pool != nil {
		a.pool.Close()
	}

	slog.Info("all resources closed")
}
