package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	authv1 "github.com/juantevez/go-posnet/pkg/proto/authorization/v1"
)

// ─── fakeQueryService ────────────────────────────────────────────────────────

type fakeQueryService struct {
	result     *port.TransactionStatusResult
	err        error
	calledWith domain.TransactionID
}

var _ port.QueryService = (*fakeQueryService)(nil)

func (f *fakeQueryService) GetTransactionStatus(_ context.Context, id domain.TransactionID) (*port.TransactionStatusResult, error) {
	f.calledWith = id
	return f.result, f.err
}

// ─── GetTransactionStatus ─────────────────────────────────────────────────────

func TestGetTransactionStatus_InvalidTransactionID(t *testing.T) {
	srv := NewAuthorizationServer(&fakeQueryService{})

	_, err := srv.GetTransactionStatus(context.Background(), &authv1.GetTransactionStatusRequest{TransactionId: "not-a-uuid"})
	if err == nil {
		t.Fatal("GetTransactionStatus() error = nil, want error")
	}
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("code = %v, want %v", code, codes.InvalidArgument)
	}
}

func TestGetTransactionStatus_NotFound(t *testing.T) {
	id := domain.NewTransactionID()
	qs := &fakeQueryService{err: pkgerrors.NewNotFoundError("Transaction", id.String())}
	srv := NewAuthorizationServer(qs)

	_, err := srv.GetTransactionStatus(context.Background(), &authv1.GetTransactionStatusRequest{TransactionId: id.String()})
	if err == nil {
		t.Fatal("GetTransactionStatus() error = nil, want error")
	}
	if code := status.Code(err); code != codes.NotFound {
		t.Errorf("code = %v, want %v", code, codes.NotFound)
	}
}

func TestGetTransactionStatus_InternalError(t *testing.T) {
	id := domain.NewTransactionID()
	qs := &fakeQueryService{err: errors.New("db unreachable")}
	srv := NewAuthorizationServer(qs)

	_, err := srv.GetTransactionStatus(context.Background(), &authv1.GetTransactionStatusRequest{TransactionId: id.String()})
	if err == nil {
		t.Fatal("GetTransactionStatus() error = nil, want error")
	}
	if code := status.Code(err); code != codes.Internal {
		t.Errorf("code = %v, want %v", code, codes.Internal)
	}
	// El mensaje interno no debe filtrarse al cliente gRPC.
	if strings.Contains(status.Convert(err).Message(), "db unreachable") {
		t.Errorf("internal error message leaked to client: %q", status.Convert(err).Message())
	}
}

func TestGetTransactionStatus_Success(t *testing.T) {
	id := domain.NewTransactionID()
	result := &port.TransactionStatusResult{
		TransactionID: id.String(),
		State:         "APPROVED",
		AmountCents:   5000,
		Currency:      "ARS",
		AuthCode:      "AB1234",
		AuthorizedAt:  "2026-01-01T10:00:00Z",
	}
	qs := &fakeQueryService{result: result}
	srv := NewAuthorizationServer(qs)

	resp, err := srv.GetTransactionStatus(context.Background(), &authv1.GetTransactionStatusRequest{TransactionId: id.String()})
	if err != nil {
		t.Fatalf("GetTransactionStatus() error = %v", err)
	}
	if resp.TransactionId != id.String() {
		t.Errorf("TransactionId = %q, want %q", resp.TransactionId, id.String())
	}
	if resp.State != authv1.TransactionState_TRANSACTION_STATE_APPROVED {
		t.Errorf("State = %v, want %v", resp.State, authv1.TransactionState_TRANSACTION_STATE_APPROVED)
	}
	if resp.AmountCents != 5000 {
		t.Errorf("AmountCents = %d, want 5000", resp.AmountCents)
	}
	if resp.Currency != "ARS" {
		t.Errorf("Currency = %q, want %q", resp.Currency, "ARS")
	}
	if resp.AuthCode != "AB1234" {
		t.Errorf("AuthCode = %q, want %q", resp.AuthCode, "AB1234")
	}
	if resp.AuthorizedAt != "2026-01-01T10:00:00Z" {
		t.Errorf("AuthorizedAt = %q, want %q", resp.AuthorizedAt, "2026-01-01T10:00:00Z")
	}
	if !qs.calledWith.Equals(id) {
		t.Errorf("queryService called with %v, want %v", qs.calledWith, id)
	}
}

// ─── ListTerminalTransactions ─────────────────────────────────────────────────

func TestListTerminalTransactions_Unimplemented(t *testing.T) {
	srv := NewAuthorizationServer(&fakeQueryService{})

	_, err := srv.ListTerminalTransactions(context.Background(), &authv1.ListTerminalTransactionsRequest{})
	if err == nil {
		t.Fatal("ListTerminalTransactions() error = nil, want error")
	}
	if code := status.Code(err); code != codes.Unimplemented {
		t.Errorf("code = %v, want %v", code, codes.Unimplemented)
	}
}

// ─── toProtoState ──────────────────────────────────────────────────────────────

func TestToProtoState(t *testing.T) {
	tests := []struct {
		in   string
		want authv1.TransactionState
	}{
		{"RECEIVED", authv1.TransactionState_TRANSACTION_STATE_RECEIVED},
		{"FRAUD_CHECKING", authv1.TransactionState_TRANSACTION_STATE_FRAUD_CHECKING},
		{"PROCESSING", authv1.TransactionState_TRANSACTION_STATE_PROCESSING},
		{"APPROVED", authv1.TransactionState_TRANSACTION_STATE_APPROVED},
		{"REJECTED", authv1.TransactionState_TRANSACTION_STATE_REJECTED},
		{"INDETERMINATE", authv1.TransactionState_TRANSACTION_STATE_INDETERMINATE},
		{"REVERSED", authv1.TransactionState_TRANSACTION_STATE_REVERSED},
		{"BOGUS", authv1.TransactionState_TRANSACTION_STATE_UNSPECIFIED},
		{"", authv1.TransactionState_TRANSACTION_STATE_UNSPECIFIED},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := toProtoState(tc.in); got != tc.want {
				t.Errorf("toProtoState(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ─── toProtoStatusResponse ───────────────────────────────────────────────────

func TestToProtoStatusResponse(t *testing.T) {
	r := &port.TransactionStatusResult{
		TransactionID:   "tx-1",
		State:           "REJECTED",
		AmountCents:     1000,
		Currency:        "USD",
		RejectionCode:   "05",
		RejectionReason: "Do Not Honor",
		RejectedAt:      "2026-01-01T10:00:00Z",
	}

	resp := toProtoStatusResponse(r)

	if resp.TransactionId != r.TransactionID {
		t.Errorf("TransactionId = %q, want %q", resp.TransactionId, r.TransactionID)
	}
	if resp.State != authv1.TransactionState_TRANSACTION_STATE_REJECTED {
		t.Errorf("State = %v, want %v", resp.State, authv1.TransactionState_TRANSACTION_STATE_REJECTED)
	}
	if resp.AmountCents != r.AmountCents {
		t.Errorf("AmountCents = %d, want %d", resp.AmountCents, r.AmountCents)
	}
	if resp.Currency != r.Currency {
		t.Errorf("Currency = %q, want %q", resp.Currency, r.Currency)
	}
	if resp.RejectionCode != r.RejectionCode {
		t.Errorf("RejectionCode = %q, want %q", resp.RejectionCode, r.RejectionCode)
	}
	if resp.RejectionReason != r.RejectionReason {
		t.Errorf("RejectionReason = %q, want %q", resp.RejectionReason, r.RejectionReason)
	}
	if resp.RejectedAt != r.RejectedAt {
		t.Errorf("RejectedAt = %q, want %q", resp.RejectedAt, r.RejectedAt)
	}
	if resp.AuthCode != "" {
		t.Errorf("AuthCode = %q, want empty", resp.AuthCode)
	}
	if resp.AuthorizedAt != "" {
		t.Errorf("AuthorizedAt = %q, want empty", resp.AuthorizedAt)
	}
}

// ─── Listen, Serve y GracefulStop ─────────────────────────────────────────────

func TestListen_PortInUse(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer occupied.Close()

	_, err = Listen(occupied.Addr().(*net.TCPAddr).Port)
	if err == nil || !strings.Contains(err.Error(), "listen on port") {
		t.Fatalf("Listen() error = %v, want el error del puerto ocupado", err)
	}
}

// Serve atiende llamadas hasta GracefulStop, que lo hace volver.
func TestServe_UntilGracefulStop(t *testing.T) {
	srv := NewAuthorizationServer(&fakeQueryService{})
	lis, err := Listen(0)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = authv1.NewAuthorizationServiceClient(conn).GetTransactionStatus(ctx, &authv1.GetTransactionStatusRequest{TransactionId: "x"})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument: el servidor tiene que atender", code)
	}

	srv.GracefulStop(time.Second)
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("Serve() error = %v, want nil después de GracefulStop", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve() no volvió después de GracefulStop")
	}
}

// GracefulStop antes de servir no se cuelga: close lo llama aunque el arranque haya fallado.
func TestGracefulStop_BeforeServe(t *testing.T) {
	done := make(chan struct{})
	go func() {
		NewAuthorizationServer(&fakeQueryService{}).GracefulStop(time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("GracefulStop() se colgó")
	}
}
