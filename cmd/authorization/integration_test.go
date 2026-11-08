//go:build integration

// Package main — integration tests del entrypoint del BC Authorization.
//
// Ejercitan el camino feliz completo del wiring (wire → start → close) contra
// infraestructura real (Postgres + NATS JetStream) levantada con Testcontainers.
// Complementan a los unit tests de wire_test.go / main_test.go, que solo cubren
// los caminos de error y la liberación de recursos.
//
// Requisitos: un daemon de Docker accesible.
// Ejecución:
//
//	go test -tags=integration -run Integration ./cmd/authorization/ -v
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	authquery "github.com/juantevez/go-posnet/context/authorization/application/query"
	"github.com/juantevez/go-posnet/context/authorization/config"
	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	authpostgres "github.com/juantevez/go-posnet/context/authorization/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
	"github.com/juantevez/go-posnet/pkg/pgutil"
	natsclient "github.com/nats-io/nats.go"

	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres levanta un Postgres efímero con el schema del proyecto ya
// aplicado (01-init.sql) y devuelve su DSN.
func startPostgres(ctx context.Context, t *testing.T) string {
	t.Helper()

	initScript, err := filepath.Abs(
		"../../deployments/docker/docker-entrypoint-initdb.d/01-init.sql",
	)
	if err != nil {
		t.Fatalf("resolver init script: %v", err)
	}

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("posnet"),
		tcpostgres.WithUsername("posnet"),
		tcpostgres.WithPassword("posnet"),
		tcpostgres.WithInitScripts(initScript),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("arrancar contenedor postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("obtener DSN postgres: %v", err)
	}
	return dsn
}

// startNATS levanta un NATS con JetStream habilitado y devuelve su URL.
func startNATS(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := tcnats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Fatalf("arrancar contenedor nats: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("obtener URL nats: %v", err)
	}
	return url
}

// freePort reserva y libera un puerto TCP local, devolviendo su número para
// evitar colisiones en el servidor HTTP del app bajo test.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reservar puerto libre: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// getWithRetry hace GET a url reintentando hasta que responde o se agota el
// deadline (el servidor HTTP arranca de forma asíncrona en app.start).
func getWithRetry(t *testing.T, url string, timeout time.Duration) *http.Response {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // test helper
		if err == nil {
			return resp
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("GET %s no respondió dentro de %s: %v", url, timeout, lastErr)
	return nil
}

// TestIntegration_WireStartClose ejercita el ciclo de vida completo del app:
// wire construye el grafo de dependencias contra Postgres y NATS reales,
// start arranca los servidores, se verifican los probes HTTP y close libera
// todos los recursos de forma limpia.
// newIntegrationTx crea una transacción en FRAUD_CHECKING con el ID indicado.
func newIntegrationTx(t *testing.T, id domain.TransactionID) *aggregate.Transaction {
	t.Helper()
	amount, err := domain.NewMoney(5000, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	stan, err := domain.NewSTAN(1)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
	}
	pan, err := domain.NewPAN("1234", domain.NetworkVisa)
	if err != nil {
		t.Fatalf("NewPAN() error = %v", err)
	}
	tx, err := aggregate.NewTransaction(
		id, domain.NewTerminalID(), domain.NewMerchantID(), amount, stan, pan,
		valueobject.EntryModeChip, domain.CardToken{}, "", nil,
	)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	if err := tx.StartFraudCheck(); err != nil {
		t.Fatalf("StartFraudCheck() error = %v", err)
	}
	return tx
}

// TestIntegration_TransactionRepo prueba contra Postgres lo que un mock no puede: el alta
// participa de la transacción, no pisa una fila existente, y las reglas de fraude se guardan
// en la columna JSONB y se vuelven a leer.
func TestIntegration_TransactionRepo(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, startPostgres(ctx, t))
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	t.Cleanup(pool.Close)
	repo := authpostgres.NewTransactionRepo(pool)

	insert := func(tx *aggregate.Transaction, fnErr error) (bool, error) {
		var created bool
		err := pgutil.WithReadCommitted(ctx, pool, func(dbTx pgx.Tx) error {
			var err error
			if created, err = repo.InsertTx(ctx, dbTx, tx); err != nil {
				return err
			}
			return fnErr
		})
		return created, err
	}

	t.Run("la purga solo borra los rechazos vencidos", func(t *testing.T) {
		now := time.Now().UTC()
		save := func(rejectedAt time.Time) domain.TransactionID {
			t.Helper()
			id := domain.NewTransactionID()
			if err := pgutil.WithReadCommitted(ctx, pool, func(dbTx pgx.Tx) error {
				return repo.SaveInvalidRejectionTx(ctx, dbTx, valueobject.InvalidTransactionRejection{
					TransactionID:   id,
					Rejection:       valueobject.NewRejectionFromValidation(valueobject.CodeInvalidData),
					ValidationError: "motivo",
					RejectedAt:      rejectedAt,
				})
			}); err != nil {
				t.Fatalf("SaveInvalidRejectionTx() error = %v", err)
			}
			return id
		}
		expired := save(now.Add(-91 * 24 * time.Hour))
		current := save(now.Add(-89 * 24 * time.Hour))

		deleted, err := repo.DeleteInvalidRejectionsBefore(ctx, now.Add(-90*24*time.Hour))
		if err != nil || deleted != 1 {
			t.Fatalf("DeleteInvalidRejectionsBefore() = %d, %v, want 1, nil", deleted, err)
		}
		if got, err := repo.FindInvalidRejection(ctx, expired); err != nil || got != nil {
			t.Errorf("vencido: FindInvalidRejection() = %v, %v, want nil, nil", got, err)
		}
		if got, err := repo.FindInvalidRejection(ctx, current); err != nil || got == nil {
			t.Errorf("vigente: FindInvalidRejection() = %v, %v, want a record", got, err)
		}
	})

	t.Run("un rollback deshace el alta", func(t *testing.T) {
		id := domain.NewTransactionID()
		boom := errors.New("boom")
		if _, err := insert(newIntegrationTx(t, id), boom); !errors.Is(err, boom) {
			t.Fatalf("insert() error = %v, want %v", err, boom)
		}
		if _, err := repo.FindByID(ctx, id); err == nil {
			t.Fatal("FindByID() error = nil: el alta sobrevivió al rollback")
		}
	})

	t.Run("sin decisión de fraude, las reglas quedan en NULL", func(t *testing.T) {
		id := domain.NewTransactionID()
		if created, err := insert(newIntegrationTx(t, id), nil); err != nil || !created {
			t.Fatalf("insert() = %v, %v, want true, nil", created, err)
		}
		var isNull bool
		if err := pool.QueryRow(ctx,
			`SELECT fraud_rules_hit IS NULL FROM pn_authorization.transactions WHERE id = $1`, id.String(),
		).Scan(&isNull); err != nil {
			t.Fatalf("leer fraud_rules_hit: %v", err)
		}
		if !isNull {
			t.Error("fraud_rules_hit no es NULL")
		}
	})

	t.Run("un alta repetida no reinicia la transacción y conserva las reglas", func(t *testing.T) {
		id := domain.NewTransactionID()
		tx := newIntegrationTx(t, id)
		if created, err := insert(tx, nil); err != nil || !created {
			t.Fatalf("insert() = %v, %v, want true, nil", created, err)
		}
		decision, err := valueobject.NewFraudDecision(55, valueobject.FraudDecisionReview, []string{"R1", "R2"})
		if err != nil {
			t.Fatalf("NewFraudDecision() error = %v", err)
		}
		if err := tx.ApplyFraudDecision(decision); err != nil {
			t.Fatalf("ApplyFraudDecision() error = %v", err)
		}
		if saved, err := repo.SaveIfState(ctx, tx, valueobject.StateFraudChecking); err != nil || !saved {
			t.Fatalf("SaveIfState() = %v, %v, want true, nil", saved, err)
		}

		if created, err := insert(newIntegrationTx(t, id), nil); err != nil || created {
			t.Fatalf("segundo insert() = %v, %v, want false, nil", created, err)
		}

		got, err := repo.FindByID(ctx, id)
		if err != nil {
			t.Fatalf("FindByID() error = %v", err)
		}
		if got.State() != valueobject.StateProcessing {
			t.Errorf("State() = %v, want %v", got.State(), valueobject.StateProcessing)
		}
		rules := got.FraudDecision().RulesHit
		if len(rules) != 2 || rules[0] != "R1" || rules[1] != "R2" {
			t.Errorf("RulesHit = %v, want [R1 R2]", rules)
		}
	})
}

// TestIntegration_InvalidTransactionIsRejected recorre el servicio real: un transaction.received
// inválido publica auth.rejected, salvo que la transacción ya exista.
func TestIntegration_InvalidTransactionIsRejected(t *testing.T) {
	ctx := context.Background()
	natsURL := startNATS(ctx, t)
	cfg := &config.Config{
		GRPCPort: freePort(t),
		HTTPPort: freePort(t),
		Postgres: config.PostgresConfig{
			DSN:             startPostgres(ctx, t),
			MaxConns:        5,
			MinConns:        1,
			MaxConnLifetime: 30 * time.Minute,
			MaxConnIdleTime: 5 * time.Minute,
		},
		NATS: config.NATSConfig{URL: natsURL, MaxReconnect: -1, ReconnectWait: 2 * time.Second},
	}
	app, err := wire(ctx, cfg)
	if err != nil {
		t.Fatalf("wire() error = %v", err)
	}
	if err := app.start(ctx); err != nil {
		app.close()
		t.Fatalf("start() error = %v", err)
	}
	t.Cleanup(app.close)

	conn, err := natsclient.Connect(natsURL)
	if err != nil {
		t.Fatalf("conectar a NATS: %v", err)
	}
	t.Cleanup(conn.Close)
	rejected, err := conn.SubscribeSync(events.SubjectAuthRejected)
	if err != nil {
		t.Fatalf("suscribir a %s: %v", events.SubjectAuthRejected, err)
	}
	js, err := conn.JetStream()
	if err != nil {
		t.Fatalf("JetStream() error = %v", err)
	}

	send := func(txID, cardLast4 string) {
		t.Helper()
		envelope, err := events.Wrap(events.SubjectTransactionReceived, txID, "Transaction", txID, "", events.TransactionReceivedPayload{
			TransactionID: txID,
			TerminalID:    domain.NewTerminalID().String(),
			MerchantID:    domain.NewMerchantID().String(),
			AmountCents:   5000,
			Currency:      "ARS",
			STAN:          1,
			EntryMode:     "CHIP",
			CardLast4:     cardLast4,
			CardNetwork:   "VISA",
		})
		if err != nil {
			t.Fatalf("Wrap() error = %v", err)
		}
		data, err := json.Marshal(envelope)
		if err != nil {
			t.Fatalf("Marshal() error = %v", err)
		}
		if _, err := js.Publish(events.SubjectTransactionReceived, data); err != nil {
			t.Fatalf("publicar transaction.received: %v", err)
		}
	}

	// Primero, una transacción que ya existe: no debe generar un rechazo. El mensaje siguiente
	// lo confirma sin esperas, porque el primer rechazo que llega tiene que ser el suyo.
	existing := domain.NewTransactionID()
	if err := pgutil.WithReadCommitted(ctx, app.pool, func(dbTx pgx.Tx) error {
		_, err := authpostgres.NewTransactionRepo(app.pool).InsertTx(ctx, dbTx, newIntegrationTx(t, existing))
		return err
	}); err != nil {
		t.Fatalf("crear la transacción existente: %v", err)
	}
	send(existing.String(), "12")

	invalid := domain.NewTransactionID().String()
	send(invalid, "4111111111111111")

	msg, err := rejected.NextMsg(20 * time.Second)
	if err != nil {
		t.Fatalf("esperar auth.rejected: %v", err)
	}
	envelope, err := events.UnmarshalEnvelope(msg.Data)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope() error = %v", err)
	}
	payload, err := events.Unwrap[events.AuthorizationRejectedPayload](envelope)
	if err != nil {
		t.Fatalf("Unwrap() error = %v", err)
	}
	if payload.TransactionID != invalid {
		t.Fatalf("TransactionID = %q, want %q (%q ya existía)", payload.TransactionID, invalid, existing)
	}
	if payload.RejectionCode != valueobject.CodeInvalidData || payload.Source != string(valueobject.SourceValidation) {
		t.Errorf("rechazo = %s(%s), want %s(%s)", payload.Source, payload.RejectionCode, valueobject.SourceValidation, valueobject.CodeInvalidData)
	}
	if payload.CardLast4 != "" {
		t.Errorf("CardLast4 = %q, want vacío", payload.CardLast4)
	}
	if strings.Contains(string(msg.Data), "4111111111111111") {
		t.Error("el evento publicado contiene el PAN")
	}

	id, err := domain.ParseTransactionID(invalid)
	if err != nil {
		t.Fatalf("ParseTransactionID() error = %v", err)
	}
	repo := authpostgres.NewTransactionRepo(app.pool)
	if _, err := repo.FindByID(ctx, id); err == nil {
		t.Error("la transacción inválida quedó en transactions")
	}

	// La consulta de estado la informa como rechazada, sin el dato inválido en el registro.
	status, err := authquery.NewTransactionQueryHandler(repo).GetTransactionStatus(ctx, id)
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if status.State != "REJECTED" || status.RejectionCode != valueobject.CodeInvalidData || status.AmountCents != 5000 {
		t.Errorf("status = %+v", status)
	}
	record, err := repo.FindInvalidRejection(ctx, id)
	if err != nil || record == nil {
		t.Fatalf("FindInvalidRejection() = %v, %v", record, err)
	}
	if !strings.Contains(record.ValidationError, "card_last4") || strings.Contains(record.ValidationError, "4111") {
		t.Errorf("ValidationError = %q", record.ValidationError)
	}
	if _, err := domain.ParseTerminalID(record.TerminalID); err != nil {
		t.Errorf("TerminalID = %q: %v", record.TerminalID, err)
	}

	// Un ID que nunca llegó sigue siendo NotFound.
	var notFound *pkgerrors.NotFoundError
	if _, err := authquery.NewTransactionQueryHandler(repo).GetTransactionStatus(ctx, domain.NewTransactionID()); !errors.As(err, &notFound) {
		t.Errorf("error = %v, want NotFoundError", err)
	}
}

// TestIntegration_InvalidTransactionsMigration aplica la migración sobre una base existente:
// la baja, la alta, y la alta otra vez, porque tiene que ser idempotente.
func TestIntegration_InvalidTransactionsMigration(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, startPostgres(ctx, t))
	if err != nil {
		t.Fatalf("pgx.Connect() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	for _, file := range []string{"down", "up", "up"} {
		script, err := os.ReadFile("../../migrations/pn_authorization/000004_invalid_transactions." + file + ".sql")
		if err != nil {
			t.Fatalf("leer migración: %v", err)
		}
		if _, err := conn.PgConn().Exec(ctx, string(script)).ReadAll(); err != nil {
			t.Fatalf("aplicar %s: %v", file, err)
		}
	}
	var table *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('pn_authorization.invalid_transactions')::text`).Scan(&table); err != nil {
		t.Fatalf("to_regclass: %v", err)
	}
	if table == nil {
		t.Error("la tabla no existe después de la migración")
	}
}

func TestIntegration_WireStartClose(t *testing.T) {
	ctx := context.Background()

	dsn := startPostgres(ctx, t)
	natsURL := startNATS(ctx, t)
	httpPort := freePort(t)

	cfg := &config.Config{
		GRPCPort: freePort(t),
		HTTPPort: httpPort,
		Postgres: config.PostgresConfig{
			DSN:             dsn,
			MaxConns:        5,
			MinConns:        1,
			MaxConnLifetime: 30 * time.Minute,
			MaxConnIdleTime: 5 * time.Minute,
			MigrationsDir:   "migrations/pn_authorization",
		},
		NATS: config.NATSConfig{
			URL:           natsURL,
			MaxReconnect:  -1,
			ReconnectWait: 2 * time.Second,
		},
	}

	// ── wire: construcción del grafo de dependencias ────────────────────────────
	app, err := wire(ctx, cfg)
	if err != nil {
		t.Fatalf("wire() error = %v, want nil", err)
	}
	if app.pool == nil || app.natsConn == nil || app.subscriber == nil ||
		app.grpcSrv == nil || app.httpSrv == nil {
		t.Fatalf("wire() dejó campos nil en app: %+v", app)
	}
	if !app.natsConn.IsConnected() {
		t.Error("natsConn no está conectado tras wire()")
	}

	// ── start: arranque de subscriber + servidores ──────────────────────────────
	if err := app.start(ctx); err != nil {
		app.close()
		t.Fatalf("start() error = %v, want nil", err)
	}

	base := fmt.Sprintf("http://127.0.0.1:%d", httpPort)

	// /healthz — liveness (estático, confirma que el servidor HTTP está arriba).
	resp := getWithRetry(t, base+"/healthz", 5*time.Second)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want 200", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// /readyz — readiness: hace un ping real a Postgres a través del pool wireado.
	resp = getWithRetry(t, base+"/readyz", 5*time.Second)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz status = %d, want 200 (Postgres debería estar disponible)", resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	// ── close: shutdown graceful ─────────────────────────────────────────────────
	app.close()

	// Tras close el servidor HTTP ya no debe aceptar conexiones.
	if _, err := http.Get(base + "/healthz"); err == nil { //nolint:noctx // test
		t.Error("el servidor HTTP sigue respondiendo tras close()")
	}
	if app.natsConn.IsConnected() {
		t.Error("natsConn sigue conectado tras close()")
	}
}

// TestIntegration_StartRefusesIncompleteSchema: con una columna de una migración faltante, el
// servicio no arranca, y no crea los consumidores de NATS.
func TestIntegration_StartRefusesIncompleteSchema(t *testing.T) {
	ctx := context.Background()
	dsn := startPostgres(ctx, t)
	natsURL := startNATS(ctx, t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect() error = %v", err)
	}
	if _, err := conn.Exec(ctx, `ALTER TABLE pn_authorization.transactions DROP COLUMN reversal_requested_at`); err != nil {
		t.Fatalf("borrar la columna: %v", err)
	}
	_ = conn.Close(ctx)

	cfg := &config.Config{
		GRPCPort: freePort(t),
		HTTPPort: freePort(t),
		Postgres: config.PostgresConfig{DSN: dsn, MaxConns: 5, MinConns: 1, MaxConnLifetime: 30 * time.Minute, MaxConnIdleTime: 5 * time.Minute},
		NATS:     config.NATSConfig{URL: natsURL, MaxReconnect: -1, ReconnectWait: 2 * time.Second},
	}
	app, err := wire(ctx, cfg)
	if err != nil {
		t.Fatalf("wire() error = %v", err)
	}
	defer app.close()

	err = app.start(ctx)
	if err == nil || !strings.Contains(err.Error(), "reversal_requested_at") {
		t.Fatalf("start() error = %v, want que nombre la columna faltante", err)
	}

	js, err := app.natsConn.JetStream()
	if err != nil {
		t.Fatalf("JetStream() error = %v", err)
	}
	if _, err := js.ConsumerInfo("POSNET_TRANSACTIONS", "auth-txn-receiver"); err == nil {
		t.Error("se creó el consumidor de transaction.received: los eventos se consumirían y fallarían")
	}
}
