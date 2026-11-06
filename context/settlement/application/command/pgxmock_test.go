package command_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

const idempotencySchema = "settlement"

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

// expectClaimed configura el pool mock para simular el claim de idempotencia:
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

// expectClaimedThenFail simula un claim de idempotencia exitoso donde el resto
// del callback de la transacción falla después — nunca se llega a Commit, solo
// se espera el Rollback diferido de pgutil.WithTransaction.
func expectClaimedThenFail(pool pgxmock.PgxPoolIface, rowsAffected int64) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", rowsAffected))
	pool.ExpectRollback()
}

// expectOutboxInsert espera la inserción de un evento en el outbox.
func expectOutboxInsert(pool pgxmock.PgxPoolIface) {
	pool.ExpectExec("INSERT INTO "+idempotencySchema+".outbox").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
}

// expectClaimedWithEvent simula el evento nuevo que guarda el lote y encola su evento,
// todo en la misma transacción.
func expectClaimedWithEvent(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	expectOutboxInsert(pool)
	pool.ExpectCommit()
	pool.ExpectRollback()
}

// expectEventTx simula la transacción de una operación de administración que encola
// la cantidad indicada de eventos.
func expectEventTx(pool pgxmock.PgxPoolIface, events int) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	for range events {
		expectOutboxInsert(pool)
	}
	pool.ExpectCommit()
	pool.ExpectRollback()
}

// expectRolledBackTx simula una transacción que falla antes de confirmar.
func expectRolledBackTx(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectRollback()
}
