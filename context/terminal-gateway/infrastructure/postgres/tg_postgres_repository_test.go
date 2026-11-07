package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/entity"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/postgres"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── PaymentSessionRepo.Save ────────────────────────────────────────────────────

func TestPaymentSessionRepo_Save_Success(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("INSERT INTO terminal_gateway.payment_sessions").
		WithArgs(anyArgs(14)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewPaymentSessionRepo(pool)
	if err := repo.Save(context.Background(), newAwaitingSession(t)); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestPaymentSessionRepo_Save_WithAuthCode(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("INSERT INTO terminal_gateway.payment_sessions").
		WithArgs(anyArgs(14)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewPaymentSessionRepo(pool)
	if err := repo.Save(context.Background(), newApprovedSession(t)); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestPaymentSessionRepo_Save_Error(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("INSERT INTO terminal_gateway.payment_sessions").
		WithArgs(anyArgs(14)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewPaymentSessionRepo(pool)
	err := repo.Save(context.Background(), newAwaitingSession(t))
	if err == nil || !strings.Contains(err.Error(), "PaymentSessionRepo.Save") {
		t.Fatalf("error = %v, want it to contain %q", err, "PaymentSessionRepo.Save")
	}
}

// ─── PaymentSessionRepo.SaveTx ───────────────────────────────────────────────────

func TestPaymentSessionRepo_SaveTx_Success(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("pool.Begin() error = %v", err)
	}
	pool.ExpectExec("INSERT INTO terminal_gateway.payment_sessions").
		WithArgs(anyArgs(14)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewPaymentSessionRepo(pool)
	if err := repo.SaveTx(context.Background(), tx, newAwaitingSession(t)); err != nil {
		t.Fatalf("SaveTx() error = %v", err)
	}
}

func TestPaymentSessionRepo_SaveTx_Error(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectBegin()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("pool.Begin() error = %v", err)
	}
	pool.ExpectExec("INSERT INTO terminal_gateway.payment_sessions").
		WithArgs(anyArgs(14)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewPaymentSessionRepo(pool)
	err = repo.SaveTx(context.Background(), tx, newAwaitingSession(t))
	if err == nil || !strings.Contains(err.Error(), "PaymentSessionRepo.SaveTx") {
		t.Fatalf("error = %v, want it to contain %q", err, "PaymentSessionRepo.SaveTx")
	}
}

// ─── PaymentSessionRepo.FindByID ─────────────────────────────────────────────────

func TestPaymentSessionRepo_FindByID_Success(t *testing.T) {
	pool := newMockPool(t)
	row := newSessionRow()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions WHERE id = \\$1").
		WithArgs(row.id).
		WillReturnRows(row.rows())

	repo := postgres.NewPaymentSessionRepo(pool)
	txID, err := domain.ParseTransactionID(row.id)
	if err != nil {
		t.Fatalf("ParseTransactionID() error = %v", err)
	}
	s, err := repo.FindByID(context.Background(), txID)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if s.ID().String() != row.id {
		t.Errorf("ID() = %q, want %q", s.ID().String(), row.id)
	}
	if s.TerminalID().String() != row.terminalID {
		t.Errorf("TerminalID() = %q, want %q", s.TerminalID().String(), row.terminalID)
	}
	if s.State() != valueobject.StateAwaitingPayment {
		t.Errorf("State() = %v, want %v", s.State(), valueobject.StateAwaitingPayment)
	}
	if s.Channel() != valueobject.ChannelNFC {
		t.Errorf("Channel() = %v, want %v", s.Channel(), valueobject.ChannelNFC)
	}
	if s.Amount().Cents() != row.amountCents {
		t.Errorf("Amount().Cents() = %d, want %d", s.Amount().Cents(), row.amountCents)
	}
	if s.STAN().Value() != row.stan {
		t.Errorf("STAN().Value() = %d, want %d", s.STAN().Value(), row.stan)
	}
	if !s.ExpiresAt().Equal(row.expiresAt) {
		t.Errorf("ExpiresAt() = %v, want %v", s.ExpiresAt(), row.expiresAt)
	}
}

func TestPaymentSessionRepo_FindByID_WithAuthCodeAndClosedAt(t *testing.T) {
	pool := newMockPool(t)
	row := newSessionRow()
	authCode := "AUTH123"
	row.authCode = &authCode
	row.state = string(valueobject.StateApproved)
	closedAt := row.expiresAt
	row.closedAt = &closedAt
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions WHERE id = \\$1").
		WithArgs(row.id).
		WillReturnRows(row.rows())

	repo := postgres.NewPaymentSessionRepo(pool)
	txID, _ := domain.ParseTransactionID(row.id)
	s, err := repo.FindByID(context.Background(), txID)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if s.AuthCode() != authCode {
		t.Errorf("AuthCode() = %q, want %q", s.AuthCode(), authCode)
	}
	if s.ClosedAt() == nil || !s.ClosedAt().Equal(closedAt) {
		t.Errorf("ClosedAt() = %v, want %v", s.ClosedAt(), closedAt)
	}
	if s.State() != valueobject.StateApproved {
		t.Errorf("State() = %v, want %v", s.State(), valueobject.StateApproved)
	}
}

func TestPaymentSessionRepo_FindByID_NotFound(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions WHERE id = \\$1").
		WithArgs(txID.String()).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewPaymentSessionRepo(pool)
	_, err := repo.FindByID(context.Background(), txID)

	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want *pkgerrors.NotFoundError", err)
	}
}

func TestPaymentSessionRepo_FindByID_ScanError(t *testing.T) {
	pool := newMockPool(t)
	txID := domain.NewTransactionID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions WHERE id = \\$1").
		WithArgs(txID.String()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewPaymentSessionRepo(pool)
	_, err := repo.FindByID(context.Background(), txID)
	if err == nil || !strings.Contains(err.Error(), "scanSession") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanSession")
	}
}

// ─── PaymentSessionRepo.FindActiveByTerminal ─────────────────────────────────────

func TestPaymentSessionRepo_FindActiveByTerminal_Success(t *testing.T) {
	pool := newMockPool(t)
	row := newSessionRow()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions(.|\n)*WHERE terminal_id = \\$1").
		WithArgs(row.terminalID).
		WillReturnRows(row.rows())

	repo := postgres.NewPaymentSessionRepo(pool)
	terminalID, _ := domain.ParseTerminalID(row.terminalID)
	s, err := repo.FindActiveByTerminal(context.Background(), terminalID)
	if err != nil {
		t.Fatalf("FindActiveByTerminal() error = %v", err)
	}
	if s == nil {
		t.Fatal("session = nil, want non-nil")
	}
	if s.TerminalID().String() != row.terminalID {
		t.Errorf("TerminalID() = %q, want %q", s.TerminalID().String(), row.terminalID)
	}
}

func TestPaymentSessionRepo_FindActiveByTerminal_NotFound(t *testing.T) {
	pool := newMockPool(t)
	terminalID := domain.NewTerminalID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions(.|\n)*WHERE terminal_id = \\$1").
		WithArgs(terminalID.String()).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewPaymentSessionRepo(pool)
	s, err := repo.FindActiveByTerminal(context.Background(), terminalID)
	if err != nil {
		t.Fatalf("FindActiveByTerminal() error = %v, want nil", err)
	}
	if s != nil {
		t.Errorf("session = %v, want nil", s)
	}
}

func TestPaymentSessionRepo_FindActiveByTerminal_OtherError(t *testing.T) {
	pool := newMockPool(t)
	terminalID := domain.NewTerminalID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.payment_sessions(.|\n)*WHERE terminal_id = \\$1").
		WithArgs(terminalID.String()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewPaymentSessionRepo(pool)
	_, err := repo.FindActiveByTerminal(context.Background(), terminalID)
	if err == nil || !strings.Contains(err.Error(), "scanSession") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanSession")
	}
}

// ─── PaymentSessionRepo.ExpireAwaiting ───────────────────────────────────────────

func TestPaymentSessionRepo_ExpireAwaiting(t *testing.T) {
	pool := newMockPool(t)
	txID, terminalID := domain.NewTransactionID(), domain.NewTerminalID()
	pool.ExpectQuery(`UPDATE terminal_gateway.payment_sessions[\s\S]*SET state = 'EXPIRED'[\s\S]*state = 'AWAITING_PAYMENT' AND expires_at < NOW\(\)[\s\S]*FOR UPDATE SKIP LOCKED`).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"id", "terminal_id"}).AddRow(txID.String(), terminalID.String()))

	got, err := postgres.NewPaymentSessionRepo(pool).ExpireAwaiting(context.Background(), 100)
	if err != nil {
		t.Fatalf("ExpireAwaiting() error = %v", err)
	}
	if len(got) != 1 || got[0].TransactionID != txID || got[0].TerminalID != terminalID {
		t.Errorf("ExpireAwaiting() = %+v", got)
	}
}

func TestPaymentSessionRepo_ExpireAwaiting_Error(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("UPDATE terminal_gateway.payment_sessions").WithArgs(100).
		WillReturnError(errors.New("connection reset"))

	_, err := postgres.NewPaymentSessionRepo(pool).ExpireAwaiting(context.Background(), 100)
	if err == nil || !strings.Contains(err.Error(), "PaymentSessionRepo.ExpireAwaiting") {
		t.Fatalf("error = %v", err)
	}
}

// ─── TerminalRepo.FindByID ────────────────────────────────────────────────────────

func TestTerminalRepo_FindByID_Success(t *testing.T) {
	pool := newMockPool(t)
	row := newTerminalRow()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.terminals WHERE id = \\$1").
		WithArgs(row.id).
		WillReturnRows(row.rows())

	repo := postgres.NewTerminalRepo(pool)
	terminalID, _ := domain.ParseTerminalID(row.id)
	term, err := repo.FindByID(context.Background(), terminalID)
	if err != nil {
		t.Fatalf("FindByID() error = %v", err)
	}
	if term.ID().String() != row.id {
		t.Errorf("ID() = %q, want %q", term.ID().String(), row.id)
	}
	if term.TerminalCode() != row.code {
		t.Errorf("TerminalCode() = %q, want %q", term.TerminalCode(), row.code)
	}
	if term.CertificateCN() != row.cn {
		t.Errorf("CertificateCN() = %q, want %q", term.CertificateCN(), row.cn)
	}
	if term.Status() != entity.TerminalActive {
		t.Errorf("Status() = %v, want %v", term.Status(), entity.TerminalActive)
	}
}

func TestTerminalRepo_FindByID_NotFound(t *testing.T) {
	pool := newMockPool(t)
	terminalID := domain.NewTerminalID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.terminals WHERE id = \\$1").
		WithArgs(terminalID.String()).
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewTerminalRepo(pool)
	_, err := repo.FindByID(context.Background(), terminalID)

	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want *pkgerrors.NotFoundError", err)
	}
}

func TestTerminalRepo_FindByID_ScanError(t *testing.T) {
	pool := newMockPool(t)
	terminalID := domain.NewTerminalID()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.terminals WHERE id = \\$1").
		WithArgs(terminalID.String()).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewTerminalRepo(pool)
	_, err := repo.FindByID(context.Background(), terminalID)
	if err == nil || !strings.Contains(err.Error(), "scanTerminal") {
		t.Fatalf("error = %v, want it to contain %q", err, "scanTerminal")
	}
}

// ─── TerminalRepo.FindByCertificateCN ─────────────────────────────────────────────

func TestTerminalRepo_FindByCertificateCN_Success(t *testing.T) {
	pool := newMockPool(t)
	row := newTerminalRow()
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.terminals WHERE certificate_cn = \\$1").
		WithArgs(row.cn).
		WillReturnRows(row.rows())

	repo := postgres.NewTerminalRepo(pool)
	term, err := repo.FindByCertificateCN(context.Background(), row.cn)
	if err != nil {
		t.Fatalf("FindByCertificateCN() error = %v", err)
	}
	if term.CertificateCN() != row.cn {
		t.Errorf("CertificateCN() = %q, want %q", term.CertificateCN(), row.cn)
	}
}

func TestTerminalRepo_FindByCertificateCN_NotFound(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectQuery("SELECT (.|\n)*FROM terminal_gateway.terminals WHERE certificate_cn = \\$1").
		WithArgs("unknown-cn").
		WillReturnError(pgx.ErrNoRows)

	repo := postgres.NewTerminalRepo(pool)
	_, err := repo.FindByCertificateCN(context.Background(), "unknown-cn")

	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want *pkgerrors.NotFoundError", err)
	}
}

// ─── TerminalRepo.Save ─────────────────────────────────────────────────────────────

func TestTerminalRepo_Save_Success(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("INSERT INTO terminal_gateway.terminals").
		WithArgs(anyArgs(7)...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	repo := postgres.NewTerminalRepo(pool)
	if err := repo.Save(context.Background(), newTerminal(t)); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
}

func TestTerminalRepo_Save_Error(t *testing.T) {
	pool := newMockPool(t)
	pool.ExpectExec("INSERT INTO terminal_gateway.terminals").
		WithArgs(anyArgs(7)...).
		WillReturnError(errors.New("connection reset"))

	repo := postgres.NewTerminalRepo(pool)
	err := repo.Save(context.Background(), newTerminal(t))
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error = %v, want it to contain %q", err, "connection reset")
	}
}
