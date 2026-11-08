package main

import (
	"context"
	"testing"
)

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
