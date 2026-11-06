// Comando settlement-recover-reversals: registra las reversas que Settlement descartó
// antes de guardar las filas REVERSAL. Sus eventos figuran como procesados, y
// pueden ya no estar en el stream, así que parte de lo que informa Authorization.
//
// Sin -apply solo lista. Se puede repetir: lo que ya se registró deja de aparecer.
//
// Ver docs/runbooks/batch-close-failure.md, Escenario E.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	pginfra "github.com/juantevez/go-posnet/context/settlement/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/natsutil"
	"github.com/juantevez/go-posnet/pkg/pgutil"
)

const defaultTimezone = "America/Argentina/Buenos_Aires"

func main() {
	apply := flag.Bool("apply", false, "registra las reversas; sin esto, solo las lista")
	operator := flag.String("operator", "", "quién ejecuta, para la auditoría; obligatorio con -apply")
	reason := flag.String("reason", "", "motivo o ticket, para la auditoría; obligatorio con -apply")
	limit := flag.Int("limit", 500, "máximo de reversas por ejecución")
	flag.Parse()

	if err := run(*apply, *operator, *reason, *limit); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(apply bool, operator, reason string, limit int) error {
	if apply && (operator == "" || reason == "") {
		return fmt.Errorf("-apply requires -operator and -reason")
	}
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is not set")
	}
	timezone := os.Getenv("SETTLEMENT_BUSINESS_TIMEZONE")
	if timezone == "" {
		timezone = defaultTimezone
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return fmt.Errorf("load timezone: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	repo := pginfra.NewSettlementBatchRepo(pool)
	found, err := repo.FindUnrecordedReversals(ctx, limit)
	if err != nil {
		return err
	}
	if len(found) == 0 {
		fmt.Println("no reversals to recover")
		return nil
	}

	// Sin publicador ni procesador: registrar una reversa no publica ni envía nada.
	handler := command.NewBatchHandler(repo, nil, nil, nil, natsutil.NewIdempotencyStore(pool, "settlement"), pool, 0)
	handler.SetBusinessLocation(location)
	handler.SetMerchantTimezones(repo)
	handler.SetReversalReviews(repo)

	var failed int
	for _, u := range found {
		fmt.Printf("%s merchant=%s terminal=%s amount=%d %s purchase_batch=%s (%s)\n",
			u.TransactionID, u.MerchantID, u.TerminalID, u.AmountCents, u.Currency, u.BatchID, u.BatchState)
		if !apply {
			continue
		}
		if err := recoverReversal(ctx, pool, repo, handler, u, operator, reason); err != nil {
			failed++
			slog.Error("reversal not recovered", slog.String("transaction_id", u.TransactionID), slog.String("error", err.Error()))
		}
	}

	if !apply {
		fmt.Printf("\n%d reversals found. Nothing was changed: run with -apply to register them.\n", len(found))
		return nil
	}
	fmt.Printf("\n%d recovered, %d failed.\n", len(found)-failed, failed)
	if failed > 0 {
		return fmt.Errorf("%d reversals failed", failed)
	}
	return nil
}

// recoverReversal registra la reversa con el flujo normal, en el día de negocio de la
// compra: no entra en el lote de hoy, cuyo cierre no la espera. Authorization no guarda
// cuándo se completó la reversa.
//
// Si falla la auditoría, la reversa ya está registrada y no vuelve a aparecer: el error
// lo informa, y hay que asentarla a mano.
func recoverReversal(ctx context.Context, pool *pgxpool.Pool, repo *pginfra.SettlementBatchRepo, handler *command.BatchHandler, u pginfra.UnrecordedReversal, operator, reason string) error {
	err := handler.RegisterReversal(ctx, port.RegisterReversalCommand{
		EventID:               uuid.NewSHA1(uuid.NameSpaceOID, []byte("settlement-reversal-recovery:"+u.TransactionID)).String(),
		OriginalTransactionID: u.TransactionID,
		TerminalID:            u.TerminalID,
		MerchantID:            u.MerchantID,
		AmountCents:           u.AmountCents,
		Currency:              u.Currency,
		CompletedAt:           u.AuthorizedAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	err = pgutil.WithReadCommitted(ctx, pool, func(tx pgx.Tx) error {
		batchID, state, err := repo.FindReversalBatchTx(ctx, tx, u.TransactionID)
		if err != nil {
			return err
		}
		return repo.InsertAuditTx(ctx, tx, repository.BatchAuditEntry{
			BatchID: batchID, Action: repository.AuditActionRecoverReversal, OperatorID: operator,
			Reason: reason + " (transaction " + u.TransactionID + ")", FromState: state, ToState: state,
		})
	})
	if err != nil {
		return fmt.Errorf("reversal registered but not audited: %w", err)
	}
	return nil
}
