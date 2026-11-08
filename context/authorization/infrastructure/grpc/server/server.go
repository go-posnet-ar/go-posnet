// Package server contiene el servidor gRPC del BC Authorization.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/juantevez/go-posnet/context/authorization/application/port"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
	authv1 "github.com/juantevez/go-posnet/pkg/proto/authorization/v1"
)

// AuthorizationServer implementa authv1.AuthorizationServiceServer.
type AuthorizationServer struct {
	authv1.UnimplementedAuthorizationServiceServer
	queryService port.QueryService
	grpc         *grpc.Server
}

// NewAuthorizationServer crea el servidor, ya registrado. Se crea acá, y no al servir, para que
// GracefulStop funcione en cualquier momento.
func NewAuthorizationServer(queryService port.QueryService) *AuthorizationServer {
	s := &AuthorizationServer{queryService: queryService}
	s.grpc = grpc.NewServer(grpc.UnaryInterceptor(observability.GRPCUnaryServerInterceptor()))
	authv1.RegisterAuthorizationServiceServer(s.grpc, s)
	return s
}

// Listen abre el puerto gRPC. Va separado de Serve para que, si el puerto está ocupado, el
// servicio no arranque, en lugar de seguir sin gRPC.
func Listen(port int) (net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("grpc server: listen on port %d: %w", port, err)
	}
	return lis, nil
}

// Serve atiende en lis hasta que se llame a GracefulStop.
func (s *AuthorizationServer) Serve(lis net.Listener) error {
	slog.Info("gRPC server listening", slog.String("addr", lis.Addr().String()))
	return s.grpc.Serve(lis)
}

// GracefulStop deja de aceptar llamadas y espera las que están en curso, hasta timeout. Después
// corta las que queden.
func (s *AuthorizationServer) GracefulStop(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		s.grpc.Stop()
	}
}

// GetTransactionStatus retorna el estado de una transacción por ID.
func (s *AuthorizationServer) GetTransactionStatus(
	ctx context.Context,
	req *authv1.GetTransactionStatusRequest,
) (*authv1.GetTransactionStatusResponse, error) {
	ctx, span := observability.StartSpan(ctx, "grpc.GetTransactionStatus")
	defer span.End()

	txID, err := domain.ParseTransactionID(req.TransactionId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid transaction_id: %v", err)
	}

	result, err := s.queryService.GetTransactionStatus(ctx, txID)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			return nil, status.Errorf(codes.NotFound, "transaction %q not found", req.TransactionId)
		}
		observability.RecordError(ctx, err)
		return nil, status.Errorf(codes.Internal, "internal error")
	}

	return toProtoStatusResponse(result), nil
}

// ListTerminalTransactions — no implementado aún.
func (s *AuthorizationServer) ListTerminalTransactions(
	ctx context.Context,
	req *authv1.ListTerminalTransactionsRequest,
) (*authv1.ListTerminalTransactionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "ListTerminalTransactions not yet implemented")
}

// ─── Mapper ───────────────────────────────────────────────────────────────────

func toProtoStatusResponse(r *port.TransactionStatusResult) *authv1.GetTransactionStatusResponse {
	resp := &authv1.GetTransactionStatusResponse{
		TransactionId:   r.TransactionID,
		State:           toProtoState(r.State),
		AmountCents:     r.AmountCents,
		Currency:        r.Currency,
		AuthCode:        r.AuthCode,
		RejectionCode:   r.RejectionCode,
		RejectionReason: r.RejectionReason,
		// Timestamps como string RFC3339 — sin timestamppb
		AuthorizedAt: r.AuthorizedAt,
		RejectedAt:   r.RejectedAt,
	}
	return resp
}

func toProtoState(s string) authv1.TransactionState {
	states := map[string]authv1.TransactionState{
		"RECEIVED":       authv1.TransactionState_TRANSACTION_STATE_RECEIVED,
		"FRAUD_CHECKING": authv1.TransactionState_TRANSACTION_STATE_FRAUD_CHECKING,
		"PROCESSING":     authv1.TransactionState_TRANSACTION_STATE_PROCESSING,
		"APPROVED":       authv1.TransactionState_TRANSACTION_STATE_APPROVED,
		"REJECTED":       authv1.TransactionState_TRANSACTION_STATE_REJECTED,
		"INDETERMINATE":  authv1.TransactionState_TRANSACTION_STATE_INDETERMINATE,
		"REVERSED":       authv1.TransactionState_TRANSACTION_STATE_REVERSED,
	}
	if v, ok := states[s]; ok {
		return v
	}
	return authv1.TransactionState_TRANSACTION_STATE_UNSPECIFIED
}
