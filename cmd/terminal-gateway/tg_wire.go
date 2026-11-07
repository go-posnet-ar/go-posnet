package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/command"
	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/config"
	grpcserver "github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/grpc/server"
	httpinfra "github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/http"
	natsinfra "github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/nats"
	pginfra "github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/postgres"
	wsinfra "github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/websocket"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/observability"
	"github.com/juantevez/go-posnet/pkg/outbox"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	nats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
)

// app agrupa todos los componentes del servicio y sus recursos abiertos.
type app struct {
	pool        *pgxpool.Pool
	natsConn    *nats.Conn
	subscriber  *natsinfra.TG_Subscriber
	outboxRelay *outbox.Relay
	grpcSrv     *grpcserver.TerminalGatewayServer
	httpSrv     *http.Server
	metrics     *command.Metrics

	sessionHandler *command.SessionHandler
	cleanupEvery   time.Duration
	tokens         *httpinfra.TerminalTokens // nil = API abierta, solo en desarrollo
	grpcOpts       []grpc.ServerOption
	// wsSrv   *websocket.Server  ← agregar cuando se implemente infrastructure/websocket/

	// wg sincroniza el apagado de los jobs de background (outbox relay,
	// cleaner de sesiones) con el cierre de recursos compartidos (pool, NATS).
	// Sin esto, close() puede cerrar el pool mientras una goroutine todavía
	// está a mitad de una query.
	wg sync.WaitGroup

	// closeOnce garantiza que close() sea idempotente: puede llamarse
	// explícitamente en el flujo normal de shutdown Y desde un defer de
	// seguridad, sin liberar recursos dos veces.
	closeOnce sync.Once
}

// wire construye el grafo de dependencias completo del BC Terminal Gateway.
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
	sessionRepo := pginfra.NewPaymentSessionRepo(pool)
	terminalRepo := pginfra.NewTerminalRepo(pool)
	idempotency := natsutil.NewIdempotencyStore(pool, "terminal_gateway")
	natsPub := natsutil.NewPublisher(js)
	eventPub := natsinfra.NewEventPublisher(natsPub)
	outboxStore := outbox.NewStore("terminal_gateway")
	outboxRelay := outbox.NewRelay(pool, js, "terminal_gateway", 500*time.Millisecond, 50)

	// TerminalNotifier — implementado por el adaptador WebSocket.
	// TODO: instanciar websocket.NewNotifier(wsSrv) cuando esté implementado.
	// Por ahora nil; reemplazar en la siguiente iteración.

	// ── Aplicación ─────────────────────────────────────────────────────────────
	sessionHandler := command.NewSessionHandler(
		sessionRepo,
		terminalRepo,
		wsinfra.NewMockNotifier(), // MockNotifier para MVP
		eventPub,
		idempotency,
		outboxStore,
		pool,
	)
	tgMetrics, err := command.NewMetrics()
	if err != nil {
		return nil, fmt.Errorf("init terminal-gateway metrics: %w", err)
	}
	sessionHandler.SetMetrics(tgMetrics)

	// Gauge de sesiones en PROCESSING — se lee de Postgres en cada scrape.
	if _, err := observability.Meter("posnet.terminal_gateway").Int64ObservableGauge(
		"posnet_sessions_processing_current",
		metric.WithDescription("Sesiones en estado PROCESSING ahora mismo. Spike = saga atascada."),
		metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
			n, err := sessionRepo.CountProcessing(ctx)
			if err != nil {
				return err
			}
			o.Observe(n)
			return nil
		}),
	); err != nil {
		return nil, fmt.Errorf("init sessions_processing gauge: %w", err)
	}
	queryHandler := query.NewSessionQueryHandler(sessionRepo)

	// ── Adaptadores de entrada ─────────────────────────────────────────────────

	// NATS Subscriber — consume AuthorizationApproved/Rejected y notifica al terminal
	subscriber := natsinfra.NewSubscriber(js, sessionHandler)

	// gRPC Server — recibe SendReceipt desde el BC Notification
	grpcSrv := grpcserver.NewTerminalGatewayServer(nil, queryHandler) // notifier = nil hasta websocket/

	// HTTP Server — healthz, readyz, metrics, operaciones REST
	allowedOrigins := cfg.CORSAllowedOrigins
	if len(allowedOrigins) == 0 {
		allowedOrigins = httpinfra.DefaultAllowedOrigins()
	}
	slog.Info("CORS allowed origins", slog.Any("origins", allowedOrigins))
	tokens, err := terminalTokens(cfg)
	if err != nil {
		return nil, err
	}
	trustedProxies, err := cfg.TrustedProxyPrefixes()
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	payTokens, err := payTokens(cfg)
	if err != nil {
		return nil, err
	}
	router := httpinfra.NewRouter(sessionHandler, queryHandler, pool, allowedOrigins,
		httpinfra.NewTerminalAuth(tokens, trustedProxies), payTokens)
	httpSrv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	if err := configureHTTPTLS(httpSrv, cfg, tokens.Configured()); err != nil {
		return nil, err
	}
	grpcOpts, err := grpcTLSOptions(cfg)
	if err != nil {
		return nil, err
	}

	// WebSocket Server — conexiones mTLS de los terminales POSNET
	// TODO: wsSrv := websocket.NewServer(cfg.TLS, sessionHandler, terminalRepo)
	// Se arrancará en cfg.WSPort en app.start()

	return &app{
		pool:        pool,
		natsConn:    natsConn,
		subscriber:  subscriber,
		outboxRelay: outboxRelay,
		grpcSrv:     grpcSrv,
		httpSrv:     httpSrv,
		metrics:     tgMetrics,
		tokens:      tokens,
		grpcOpts:    grpcOpts,

		sessionHandler: sessionHandler,
		cleanupEvery:   cfg.Session.ExpiredCleanupEvery,
	}, nil
}

// start arranca todos los servidores en goroutines independientes.
// Los jobs de background (outbox relay, cleaner) se registran en el
// WaitGroup del app para que close() pueda esperar su finalización real
// antes de liberar recursos compartidos (pool, NATS).
func (a *app) start(ctx context.Context) error {
	// NATS consumers
	if err := a.subscriber.Subscribe(); err != nil {
		return fmt.Errorf("start: subscribe NATS consumers: %w", err)
	}
	slog.Info("NATS consumers active")

	// gRPC server
	go func() {
		if err := grpcserver.Start(a.grpcSrv, 9091, a.grpcOpts...); err != nil {
			slog.Error("gRPC server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("gRPC server starting", slog.Int("port", 9091))

	// HTTP server
	serve := a.httpSrv.ListenAndServe
	if a.httpSrv.TLSConfig != nil {
		// El certificado ya está cargado en TLSConfig.
		serve = func() error { return a.httpSrv.ListenAndServeTLS("", "") }
	}
	go func() {
		if err := serve(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server stopped", slog.String("error", err.Error()))
		}
	}()
	slog.Info("HTTP server starting", slog.String("addr", a.httpSrv.Addr))

	// WebSocket server — en puerto separado con mTLS
	// TODO: go func() { wsSrv.ListenAndServeTLS(cfg.WSPort, cfg.TLS) }()

	// Outbox relay — publica eventos pendientes a NATS con reintentos automáticos
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.outboxRelay.Run(ctx)
	}()
	slog.Info("outbox relay active")

	// Tokens de los terminales: un token revocado deja de valer sin reiniciar
	if a.tokens != nil {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.tokens.Run(ctx, terminalTokensReload)
		}()
	}

	// Job de limpieza de sesiones expiradas
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.runExpiredSessionsCleaner(ctx)
	}()
	slog.Info("expired sessions cleaner active")

	return nil
}

// runExpiredSessionsCleaner vence periódicamente las sesiones sin pagar. Se detiene cuando el
// contexto se cancela.
func (a *app) runExpiredSessionsCleaner(ctx context.Context) {
	ticker := time.NewTicker(a.cleanupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expired, err := a.sessionHandler.ExpireSessions(ctx)
			if err != nil && ctx.Err() == nil {
				slog.Error("expired sessions job failed", slog.String("error", err.Error()))
			}
			if expired > 0 {
				slog.Info("sessions expired", slog.Int("count", expired))
			}
		}
	}
}

// close libera todos los recursos en orden inverso al de creación.
//
// Orden de apagado:
//  1. Dejar de aceptar trabajo nuevo: gRPC GracefulStop + HTTP Shutdown.
//     Ambos bloquean hasta drenar las requests/streams en vuelo.
//  2. Esperar a que los jobs de background (outbox relay, cleaner) salgan
//     de su loop por ctx.Done() — vía wg.Wait().
//  3. Recién ahí cerrar recursos compartidos (NATS, pool de Postgres),
//     que ya no tienen ningún consumidor activo.
//
// Envuelto en sync.Once porque se llama tanto explícitamente en el flujo
// normal de shutdown (main.go) como desde un defer de seguridad — sin el
// Once, correría dos veces y pool.Close()/natsConn.Close() sobre un
// recurso ya cerrado.
func (a *app) close() {
	a.closeOnce.Do(func() {
		slog.Info("closing resources")

		if a.grpcSrv != nil {
			slog.Info("stopping gRPC server")
			a.grpcSrv.GracefulStop()
		}

		if a.httpSrv != nil {
			slog.Info("stopping HTTP server")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := a.httpSrv.Shutdown(shutdownCtx); err != nil {
				slog.Error("HTTP graceful shutdown failed", slog.String("error", err.Error()))
			}
		}

		slog.Info("waiting for background jobs to finish")
		a.wg.Wait()

		if a.natsConn != nil {
			_ = a.natsConn.Drain()
			a.natsConn.Close()
		}

		if a.pool != nil {
			a.pool.Close()
		}

		slog.Info("all resources closed")
	})
}

// terminalTokens carga los tokens de los terminales. Sin ellos, cualquiera con acceso de red
// opera como cualquier terminal: solo se permite en desarrollo.
func terminalTokens(cfg *config.Config) (*httpinfra.TerminalTokens, error) {
	tokens, err := httpinfra.LoadTerminalTokens(cfg.TerminalTokensFile)
	if err != nil {
		return nil, fmt.Errorf("wire: load terminal tokens: %w", err)
	}
	if !tokens.Configured() {
		if cfg.OTEL.Environment != "development" {
			return nil, fmt.Errorf("wire: TERMINAL_TOKENS_FILE is required with ENVIRONMENT=%s: without it, the terminal API is open", cfg.OTEL.Environment)
		}
		slog.Warn("no terminal tokens configured — the terminal API is open (development only)")
		return nil, nil
	}
	slog.Info("terminal tokens loaded", slog.Int("tokens", tokens.Count()))
	return tokens, nil
}

// terminalTokensReload es cada cuánto se relee el archivo de tokens: lo que tarda en regir una
// revocación.
const terminalTokensReload = 30 * time.Second

// configureHTTPTLS habilita HTTPS si hay certificado. La configuración ya exige TLS fuera de
// desarrollo; acá solo se advierte el caso de desarrollo con tokens.
func configureHTTPTLS(srv *http.Server, cfg *config.Config, withTokens bool) error {
	if cfg.HTTPTLSCert == "" {
		if withTokens && !cfg.HTTPTLSExternal {
			slog.Warn("the HTTP API has no TLS — terminal tokens travel unencrypted")
		}
		return nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.HTTPTLSCert, cfg.HTTPTLSKey)
	if err != nil {
		return fmt.Errorf("wire: load HTTP TLS certificate: %w", err)
	}
	srv.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	slog.Info("HTTP API serves HTTPS only")
	return nil
}

// grpcTLSOptions habilita mTLS en gRPC: cifra el tráfico y solo acepta a Notification, con un
// certificado firmado por GRPC_TLS_CLIENT_CA y a nombre de GRPC_TLS_CLIENT_NAMES. La configuración ya lo exige fuera de desarrollo.
func grpcTLSOptions(cfg *config.Config) ([]grpc.ServerOption, error) {
	opt, err := grpcserver.TLSOption(cfg.GRPCTLSCert, cfg.GRPCTLSKey, cfg.GRPCTLSClientCA, cfg.GRPCTLSClientNames)
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	switch {
	case opt == nil:
		slog.Warn("the gRPC server has no TLS — receipts travel unencrypted and any client is accepted")
		return nil, nil
	case cfg.GRPCTLSClientCA == "":
		slog.Warn("the gRPC server has TLS but does not verify clients — any client is accepted")
	default:
		slog.Info("the gRPC server requires mTLS", slog.Any("allowed_clients", cfg.GRPCTLSClientNames))
	}
	return []grpc.ServerOption{opt}, nil
}

// payTokens arma el emisor del token de sesión. Sin PAY_TOKEN_KEY, que solo se permite en
// desarrollo, usa una clave aleatoria: los QR dejan de valer al reiniciar, y no sirve con varias
// instancias.
func payTokens(cfg *config.Config) (*httpinfra.PayTokens, error) {
	key := []byte(cfg.PayTokenKey)
	if len(key) == 0 {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("wire: generate pay token key: %w", err)
		}
		slog.Warn("PAY_TOKEN_KEY is not set — session tokens use an ephemeral key (development only)")
	}
	if cfg.PayTokenRequired {
		slog.Info("/pay and /status require the session token")
	} else {
		slog.Warn("/pay and /status accept requests without the session token — set PAY_TOKEN_REQUIRED=true once every client sends it")
	}
	return httpinfra.NewPayTokens(key, []byte(cfg.PayTokenKeyPrevious), cfg.PayTokenRequired), nil
}
