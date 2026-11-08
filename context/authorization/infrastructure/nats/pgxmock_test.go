package nats

import (
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
// que hace AuthorizationHandler al inicio de cada comando: BEGIN → INSERT ...
// ON CONFLICT DO NOTHING → COMMIT → (rollback diferido, no-op).
// rowsAffected=1 simula un evento nuevo, 0 simula un duplicado ya procesado.
func expectClaimed(pool pgxmock.PgxPoolIface, rowsAffected int64) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectExec("INSERT INTO " + idempotencySchema + ".processed_events").
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", rowsAffected))
	pool.ExpectCommit()
	pool.ExpectRollback()
}

// expectTx configura una transacción vacía del pool mock: la que abre el handler
// para guardar un estado y encolar su evento en el outbox. El repositorio y el
// publisher son fakes, así que la transacción no ejecuta sentencias.
func expectTx(pool pgxmock.PgxPoolIface) {
	pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	pool.ExpectCommit()
	pool.ExpectRollback()
}
