// Package client contiene el cliente gRPC del BC Notification hacia Terminal Gateway.
// Es el único adaptador gRPC cliente real del sistema.
// Implementa domain/service.TerminalNotifier.
package client

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/juantevez/go-posnet/context/notification/domain/aggregate"
	"github.com/juantevez/go-posnet/pkg/observability"
	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TerminalGatewayClient implementa domain/service.TerminalNotifier.
// Llama al servidor gRPC de Terminal Gateway para entregar el comprobante
// al WebSocket del terminal, sin que Notification sepa nada de WebSockets.
type TerminalGatewayClient struct {
	client tgv1.TerminalGatewayServiceClient
	conn   *grpc.ClientConn
}

// NewTerminalGatewayClient construye el cliente con el interceptor de trace. Sin tlsCfg, la
// conexión no usa TLS: la configuración solo lo permite en desarrollo.
func NewTerminalGatewayClient(target string, tlsCfg *tls.Config) (*TerminalGatewayClient, error) {
	creds := insecure.NewCredentials()
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	}
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(creds),
		grpc.WithUnaryInterceptor(observability.GRPCUnaryClientInterceptor()),
	)
	if err != nil {
		return nil, fmt.Errorf("nt grpc client: dial %q: %w", target, err)
	}
	return &TerminalGatewayClient{
		client: tgv1.NewTerminalGatewayServiceClient(conn),
		conn:   conn,
	}, nil
}

// Close cierra la conexión gRPC. Llamar con defer en wire.go.
func (c *TerminalGatewayClient) Close() error {
	return c.conn.Close()
}

// SendReceipt implementa domain/service.TerminalNotifier.
// Entrega el comprobante al WebSocket del terminal vía gRPC.
func (c *TerminalGatewayClient) SendReceipt(
	ctx context.Context,
	n *aggregate.Notification,
) (bool, string, error) {
	ctx, span := observability.StartSpan(ctx, "grpc_client.SendReceipt")
	defer span.End()

	receipt := n.Receipt()

	resp, err := c.client.SendReceipt(ctx, &tgv1.SendReceiptRequest{
		TerminalId:    receipt.TerminalCode,
		TransactionId: receipt.TransactionID,
		Receipt: &tgv1.Receipt{
			TransactionId:   receipt.TransactionID,
			MerchantName:    receipt.MerchantName,
			MerchantAddress: receipt.MerchantAddress,
			TerminalCode:    receipt.TerminalCode,
			Result:          toProtoResult(receipt.Result),
			AuthCode:        receipt.AuthCode,
			RejectionCode:   receipt.RejectionCode,
			RejectionReason: receipt.RejectionReason,
			AmountCents:     receipt.AmountCents,
			Currency:        receipt.Currency,
			CardLast4:       receipt.CardLast4,
			CardNetwork:     receipt.CardNetwork,
			EntryMode:       receipt.EntryMode,
		},
	})
	if err != nil {
		observability.RecordError(ctx, err)
		return false, "", fmt.Errorf("nt grpc client: SendReceipt: %w", err)
	}

	return resp.Delivered, resp.ErrorReason, nil
}

func toProtoResult(r string) tgv1.ReceiptResult {
	switch r {
	case "APPROVED":
		return tgv1.ReceiptResult_RECEIPT_RESULT_APPROVED
	case "REJECTED":
		return tgv1.ReceiptResult_RECEIPT_RESULT_REJECTED
	case "REVERSED":
		return tgv1.ReceiptResult_RECEIPT_RESULT_REVERSED
	default:
		return tgv1.ReceiptResult_RECEIPT_RESULT_UNSPECIFIED
	}
}
