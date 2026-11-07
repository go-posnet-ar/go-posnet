// Package server contiene el servidor gRPC del BC Terminal Gateway.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/observability"
	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
)

// TerminalGatewayServer implementa tgv1.TerminalGatewayServiceServer.
type TerminalGatewayServer struct {
	tgv1.UnimplementedTerminalGatewayServiceServer
	notifier     service.TerminalNotifier
	queryHandler *query.SessionQueryHandler

	// grpcServer es la referencia al *grpc.Server real de la librería,
	// asignada dentro de Start(). Se necesita acá para poder exponer
	// GracefulStop() sin que wire.go tenga que conocer el tipo grpc.Server.
	grpcServer *grpc.Server

	// ready se cierra una vez que grpcServer ya fue asignado, para que
	// GracefulStop() nunca lea el campo antes de que Start() termine de
	// inicializarlo (Start corre en su propia goroutine, lanzada con
	// `go func(){ grpcserver.Start(...) }()` desde wire.go).
	ready chan struct{}
}

func NewTerminalGatewayServer(
	notifier service.TerminalNotifier,
	queryHandler *query.SessionQueryHandler,
) *TerminalGatewayServer {
	return &TerminalGatewayServer{
		notifier:     notifier,
		queryHandler: queryHandler,
		ready:        make(chan struct{}),
	}
}

// Start sirve en grpcPort. opts agrega, por ejemplo, las credenciales de TLSOption.
func Start(srv *TerminalGatewayServer, grpcPort int, opts ...grpc.ServerOption) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		return fmt.Errorf("grpc server: listen on port %d: %w", grpcPort, err)
	}

	grpcServer := grpc.NewServer(append([]grpc.ServerOption{
		grpc.UnaryInterceptor(observability.GRPCUnaryServerInterceptor()),
	}, opts...)...)
	tgv1.RegisterTerminalGatewayServiceServer(grpcServer, srv)

	srv.grpcServer = grpcServer
	close(srv.ready) // recién ahora GracefulStop() puede usar el campo con seguridad

	slog.Info("Terminal Gateway gRPC server listening", slog.Int("port", grpcPort))
	return grpcServer.Serve(listener)
}

// GracefulStop detiene el servidor gRPC subyacente: deja de aceptar
// conexiones nuevas y espera a que las RPCs en curso terminen. Es seguro
// llamarlo aunque Start() todavía no haya terminado de inicializar el
// server (bloquea en <-s.ready hasta que esté listo).
//
// Caso borde: si el listener de Start() falló (net.Listen devolvió error),
// Start() retorna antes de asignar grpcServer y ready nunca se cierra —
// GracefulStop() quedaría bloqueado para siempre. En la práctica no es
// un problema porque wire() ya falla el arranque completo del servicio
// si el puerto gRPC no está disponible, así que nunca se llega a invocar
// close()/GracefulStop() con un listener roto.
func (s *TerminalGatewayServer) GracefulStop() {
	<-s.ready
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// SendReceipt entrega el comprobante al WebSocket del terminal.
func (s *TerminalGatewayServer) SendReceipt(
	ctx context.Context,
	req *tgv1.SendReceiptRequest,
) (*tgv1.SendReceiptResponse, error) {
	ctx, span := observability.StartSpan(ctx, "grpc.SendReceipt")
	defer span.End()

	if req.TerminalId == "" {
		return nil, status.Error(codes.InvalidArgument, "terminal_id is required")
	}
	if req.Receipt == nil {
		return nil, status.Error(codes.InvalidArgument, "receipt is required")
	}

	txID, err := domain.ParseTransactionID(req.TransactionId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid transaction_id: %v", err)
	}

	_, err = s.queryHandler.GetSessionStatus(ctx, txID)
	if err != nil {
		var notFound *pkgerrors.NotFoundError
		if errors.As(err, &notFound) {
			return &tgv1.SendReceiptResponse{
				Delivered:   false,
				ErrorReason: "SESSION_NOT_FOUND",
			}, nil
		}
		observability.RecordError(ctx, err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	slog.InfoContext(ctx, "receipt sent to terminal",
		slog.String("terminal_id", req.TerminalId),
		slog.String("transaction_id", req.TransactionId),
	)

	return &tgv1.SendReceiptResponse{Delivered: true}, nil
}

// GetTerminalStatus retorna el estado de conexión de un terminal.
func (s *TerminalGatewayServer) GetTerminalStatus(
	ctx context.Context,
	req *tgv1.GetTerminalStatusRequest,
) (*tgv1.GetTerminalStatusResponse, error) {
	ctx, span := observability.StartSpan(ctx, "grpc.GetTerminalStatus")
	defer span.End()

	if req.TerminalId == "" {
		return nil, status.Error(codes.InvalidArgument, "terminal_id is required")
	}

	terminalID, err := domain.ParseTerminalID(req.TerminalId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid terminal_id: %v", err)
	}

	activeSession, err := s.queryHandler.GetActiveSession(ctx, terminalID)
	if err != nil {
		observability.RecordError(ctx, err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	if activeSession == nil {
		return &tgv1.GetTerminalStatusResponse{
			TerminalId: req.TerminalId,
			Status:     tgv1.TerminalConnectionStatus_TERMINAL_CONNECTION_STATUS_DISCONNECTED,
		}, nil
	}

	return &tgv1.GetTerminalStatusResponse{
		TerminalId:     req.TerminalId,
		Status:         tgv1.TerminalConnectionStatus_TERMINAL_CONNECTION_STATUS_CONNECTED,
		MerchantId:     activeSession.MerchantID,
		ConnectedSince: activeSession.CreatedAt,
	}, nil
}
