package server_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/repository"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/grpc/server"
	"github.com/juantevez/go-posnet/pkg/domain"
	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
)

// fakeSessionRepo implementa repository.PaymentSessionRepository — solo
// FindByID/FindActiveByTerminal se ejercitan desde este servidor gRPC.
type fakeSessionRepo struct {
	findResult *aggregate.PaymentSession
	findErr    error

	findActiveResult *aggregate.PaymentSession
	findActiveErr    error
}

func (f *fakeSessionRepo) Save(context.Context, *aggregate.PaymentSession) error { return nil }
func (f *fakeSessionRepo) SaveTx(context.Context, pgx.Tx, *aggregate.PaymentSession) error {
	return nil
}

func (f *fakeSessionRepo) FindByID(context.Context, domain.TransactionID) (*aggregate.PaymentSession, error) {
	return f.findResult, f.findErr
}

func (f *fakeSessionRepo) FindActiveByTerminal(context.Context, domain.TerminalID) (*aggregate.PaymentSession, error) {
	return f.findActiveResult, f.findActiveErr
}

func (f *fakeSessionRepo) ExpireAwaiting(context.Context, int) ([]repository.ExpiredSession, error) {
	return nil, nil
}

func mustMoney(t *testing.T) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(1000, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	return m
}

func mustSTAN(t *testing.T) domain.STAN {
	t.Helper()
	s, err := domain.NewSTAN(123456)
	if err != nil {
		t.Fatalf("NewSTAN() error = %v", err)
	}
	return s
}

func newSession(t *testing.T, terminalID domain.TerminalID) *aggregate.PaymentSession {
	t.Helper()
	return aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: terminalID,
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelQR,
		State:      valueobject.StateAwaitingPayment,
		ExpiresAt:  time.Now().UTC().Add(5 * time.Minute),
		CreatedAt:  time.Now().UTC(),
	})
}

func grpcStatusCode(t *testing.T, err error) grpccodes.Code {
	t.Helper()
	st, ok := grpcstatus.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a grpc status error", err)
	}
	return st.Code()
}

// ─── NewTerminalGatewayServer / Start ─────────────────────────────────────────

func TestNewTerminalGatewayServer(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))
	if srv == nil {
		t.Fatal("NewTerminalGatewayServer() = nil, want non-nil")
	}
}

func TestStart_ListenError(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer occupied.Close()
	port := occupied.Addr().(*net.TCPAddr).Port

	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))
	err = server.Start(srv, port)
	if err == nil {
		t.Fatal("Start() error = nil, want bind error")
	}
	if !strings.Contains(err.Error(), "listen on port") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "listen on port")
	}
}

func TestStart_ServesSuccessfully(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(srv, 0)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Start() returned early with error = %v, want it to keep serving", err)
	case <-time.After(200 * time.Millisecond):
	}
}

// ─── SendReceipt ──────────────────────────────────────────────────────────────

func validSendReceiptReq(terminalID, txID string) *tgv1.SendReceiptRequest {
	return &tgv1.SendReceiptRequest{
		TerminalId:    terminalID,
		TransactionId: txID,
		Receipt: &tgv1.Receipt{
			TransactionId: txID,
			MerchantName:  "Merchant Inc",
			Result:        tgv1.ReceiptResult_RECEIPT_RESULT_APPROVED,
		},
	}
}

func TestSendReceipt_EmptyTerminalID(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	_, err := srv.SendReceipt(context.Background(), &tgv1.SendReceiptRequest{})
	if grpcStatusCode(t, err) != grpccodes.InvalidArgument {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.InvalidArgument)
	}
}

func TestSendReceipt_NilReceipt(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	_, err := srv.SendReceipt(context.Background(), &tgv1.SendReceiptRequest{TerminalId: "term-1"})
	if grpcStatusCode(t, err) != grpccodes.InvalidArgument {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.InvalidArgument)
	}
}

func TestSendReceipt_InvalidTransactionID(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	req := validSendReceiptReq("term-1", "not-a-uuid")
	_, err := srv.SendReceipt(context.Background(), req)
	if grpcStatusCode(t, err) != grpccodes.InvalidArgument {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.InvalidArgument)
	}
}

func TestSendReceipt_SessionNotFound(t *testing.T) {
	repo := &fakeSessionRepo{findResult: nil}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	req := validSendReceiptReq(domain.NewTerminalID().String(), domain.NewTransactionID().String())
	resp, err := srv.SendReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("SendReceipt() error = %v, want nil", err)
	}
	if resp.Delivered {
		t.Error("Delivered = true, want false")
	}
	if resp.ErrorReason != "SESSION_NOT_FOUND" {
		t.Errorf("ErrorReason = %q, want %q", resp.ErrorReason, "SESSION_NOT_FOUND")
	}
}

func TestSendReceipt_QueryErrorIsInternal(t *testing.T) {
	// A diferencia de "no encontrado", un error real de la query (ej: Postgres
	// caído) debe devolver codes.Internal — no debe reportarse como
	// SESSION_NOT_FOUND, que Notification interpretaría como permanente.
	repo := &fakeSessionRepo{findErr: errors.New("connection reset")}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	req := validSendReceiptReq(domain.NewTerminalID().String(), domain.NewTransactionID().String())
	resp, err := srv.SendReceipt(context.Background(), req)
	if resp != nil {
		t.Errorf("resp = %+v, want nil", resp)
	}
	if grpcStatusCode(t, err) != grpccodes.Internal {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.Internal)
	}
}

func TestSendReceipt_Success(t *testing.T) {
	terminalID := domain.NewTerminalID()
	session := newSession(t, terminalID)
	repo := &fakeSessionRepo{findResult: session}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	req := validSendReceiptReq(terminalID.String(), session.ID().String())
	resp, err := srv.SendReceipt(context.Background(), req)
	if err != nil {
		t.Fatalf("SendReceipt() error = %v", err)
	}
	if !resp.Delivered {
		t.Error("Delivered = false, want true")
	}
	if resp.ErrorReason != "" {
		t.Errorf("ErrorReason = %q, want empty", resp.ErrorReason)
	}
}

// ─── GetTerminalStatus ──────────────────────────────────────────────────────────

func TestGetTerminalStatus_EmptyTerminalID(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	_, err := srv.GetTerminalStatus(context.Background(), &tgv1.GetTerminalStatusRequest{})
	if grpcStatusCode(t, err) != grpccodes.InvalidArgument {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.InvalidArgument)
	}
}

func TestGetTerminalStatus_InvalidTerminalID(t *testing.T) {
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))

	_, err := srv.GetTerminalStatus(context.Background(), &tgv1.GetTerminalStatusRequest{TerminalId: "not-a-uuid"})
	if grpcStatusCode(t, err) != grpccodes.InvalidArgument {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.InvalidArgument)
	}
}

func TestGetTerminalStatus_QueryError(t *testing.T) {
	repo := &fakeSessionRepo{findActiveErr: errors.New("connection reset")}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	_, err := srv.GetTerminalStatus(context.Background(), &tgv1.GetTerminalStatusRequest{TerminalId: domain.NewTerminalID().String()})
	if grpcStatusCode(t, err) != grpccodes.Internal {
		t.Fatalf("code = %v, want %v", grpcStatusCode(t, err), grpccodes.Internal)
	}
}

func TestGetTerminalStatus_Disconnected(t *testing.T) {
	repo := &fakeSessionRepo{findActiveResult: nil}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	terminalID := domain.NewTerminalID().String()
	resp, err := srv.GetTerminalStatus(context.Background(), &tgv1.GetTerminalStatusRequest{TerminalId: terminalID})
	if err != nil {
		t.Fatalf("GetTerminalStatus() error = %v", err)
	}
	if resp.Status != tgv1.TerminalConnectionStatus_TERMINAL_CONNECTION_STATUS_DISCONNECTED {
		t.Errorf("Status = %v, want DISCONNECTED", resp.Status)
	}
	if resp.TerminalId != terminalID {
		t.Errorf("TerminalId = %q, want %q", resp.TerminalId, terminalID)
	}
}

func TestGetTerminalStatus_Connected(t *testing.T) {
	terminalID := domain.NewTerminalID()
	session := newSession(t, terminalID)
	repo := &fakeSessionRepo{findActiveResult: session}
	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(repo))

	resp, err := srv.GetTerminalStatus(context.Background(), &tgv1.GetTerminalStatusRequest{TerminalId: terminalID.String()})
	if err != nil {
		t.Fatalf("GetTerminalStatus() error = %v", err)
	}
	if resp.Status != tgv1.TerminalConnectionStatus_TERMINAL_CONNECTION_STATUS_CONNECTED {
		t.Errorf("Status = %v, want CONNECTED", resp.Status)
	}
	if resp.MerchantId != session.MerchantID().String() {
		t.Errorf("MerchantId = %q, want %q", resp.MerchantId, session.MerchantID().String())
	}
}
