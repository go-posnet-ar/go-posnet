package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
)

// setMinimalEnv configura las variables requeridas por config.Load y limpia
// las opcionales para que run() llegue hasta el wiring sin depender del entorno
// de la máquina. El DSN de Postgres es inválido a propósito: run debe fallar en
// el wiring, no antes.
func setMinimalEnv(t *testing.T) {
	t.Helper()
	// Requeridas por config.Load (requireEnv panica si faltan).
	t.Setenv("POSTGRES_DSN", "host=localhost port=notanumber")
	t.Setenv("NATS_URL", "nats://127.0.0.1:1")

	// Opcionales que podrían apuntar a colectores reales; las neutralizamos.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:1")
	t.Setenv("ENVIRONMENT", "test")
	t.Setenv("OTEL_SERVICE_NAME", "posnet-authorization-test")
}

// run debe propagar el error del wiring cuando las dependencias no pueden
// construirse (DSN de Postgres inválido), envuelto con "wire dependencies".
// De paso ejercita config.Load y la inicialización de observabilidad.
func TestRun_WireError(t *testing.T) {
	setMinimalEnv(t)

	err := run()
	if err == nil {
		t.Fatal("run() error = nil, want error de wiring")
	}
	if !strings.Contains(err.Error(), "wire dependencies") {
		t.Errorf("run() error = %q, want que contenga %q", err.Error(), "wire dependencies")
	}
}

// El contexto raíz ya está cancelado al apagar: el tracer y el meter reciben uno propio, con
// plazo, para poder exportar lo que quedó en cola.
func TestShutdownTelemetry_UsesAFreshContext(t *testing.T) {
	called := false
	shutdownTelemetry("tracer", func(ctx context.Context) error {
		called = true
		if ctx.Err() != nil {
			t.Errorf("contexto = %v, want vigente", ctx.Err())
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("el contexto no tiene plazo: un colector caído colgaría el apagado")
		}
		return nil
	})
	if !called {
		t.Fatal("shutdownTelemetry no llamó a la función de cierre")
	}
}

// close se llama al apagar y de nuevo en un defer: la segunda vez no hace nada.
func TestAppClose_IsIdempotent(t *testing.T) {
	a := &app{}
	a.close()
	a.close()
}

// Sin una migración, el servicio no arranca: cada evento fallaría hasta agotar sus reintentos.
func TestStart_RefusesAnIncompleteSchema(t *testing.T) {
	cases := []struct {
		name    string
		missing func(context.Context) ([]string, error)
		want    string
	}{
		{"falta una columna", func(context.Context) ([]string, error) {
			return []string{"pn_authorization.transactions.reversal_requested_at"}, nil
		}, "database schema incomplete"},
		{"no se puede verificar", func(context.Context) ([]string, error) {
			return nil, errors.New("db down")
		}, "check the database schema"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Sin subscriber ni servidores: tiene que cortar antes de usarlos.
			err := (&app{missingSchema: tc.missing}).start(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("start() error = %v, want %q", err, tc.want)
			}
		})
	}
}

// Si el puerto gRPC está ocupado, el servicio no arranca, en lugar de seguir sin gRPC.
func TestStart_RefusesABusyGRPCPort(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer occupied.Close()

	a := &app{
		grpcPort:      occupied.Addr().(*net.TCPAddr).Port,
		missingSchema: func(context.Context) ([]string, error) { return nil, nil },
	}
	if err := a.start(context.Background()); err == nil || !strings.Contains(err.Error(), "listen on port") {
		t.Fatalf("start() error = %v, want el error del puerto ocupado", err)
	}
}
