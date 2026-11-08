package command_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

// newMockPool crea un pool pgxmock y registra su cierre y la verificación de
// expectations al finalizar el test.
func newMockPool(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := pool.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet pgxmock expectations: %v", err)
		}
	})
	return pool
}

// expectClaimed configura el pool mock para simular el claim de idempotencia
// que hacen AuthorizeTransaction/ApplyFraudScore/ProcessReversal al inicio:
// BEGIN → INSERT ... ON CONFLICT DO NOTHING → COMMIT → (rollback diferido, no-op).
// rowsAffected=1 simula un evento nuevo, 0 simula un duplicado ya procesado.
func expectClaimed(pool pgxmock.PgxPoolIface, rowsAffected int64) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", rowsAffected))
	pool.ExpectCommit()
	pool.ExpectRollback()
}

// expectClaimedThenRollback simula un claim exitoso seguido de un error en la misma
// transacción: el evento no queda marcado y la reentrega puede reprocesarlo.
func expectClaimedThenRollback(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	pool.ExpectRollback()
}

// expectTx configura una transacción de base sin consultas propias del mock: la del resultado y
// su evento en el outbox, que hacen el fakeRepo y el fakePublisher.
func expectTx(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectCommit()
	pool.ExpectRollback()
}

// txPool es un pool falso que abre transacciones sin consultas propias y siempre las confirma.
// Sirve para los tests que no verifican la base, como los del conciliador: el fakeRepo y el
// fakePublisher reciben la transacción, pero no la usan.
type txPool struct{}

func (txPool) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) { return noopTx{}, nil }
func (txPool) Ping(context.Context) error                             { return nil }

type noopTx struct{ pgx.Tx }

func (noopTx) Commit(context.Context) error   { return nil }
func (noopTx) Rollback(context.Context) error { return nil }
