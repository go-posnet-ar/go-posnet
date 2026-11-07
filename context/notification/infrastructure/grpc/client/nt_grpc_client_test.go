package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/juantevez/go-posnet/context/notification/domain/aggregate"
	"github.com/juantevez/go-posnet/context/notification/domain/valueobject"
	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// ─── fakeTerminalGatewayServiceClient ────────────────────────────────────────

type fakeTerminalGatewayServiceClient struct {
	sendReceiptResp *tgv1.SendReceiptResponse
	sendReceiptErr  error
	lastReq         *tgv1.SendReceiptRequest
}

func (f *fakeTerminalGatewayServiceClient) SendReceipt(_ context.Context, in *tgv1.SendReceiptRequest, _ ...grpc.CallOption) (*tgv1.SendReceiptResponse, error) {
	f.lastReq = in
	if f.sendReceiptErr != nil {
		return nil, f.sendReceiptErr
	}
	return f.sendReceiptResp, nil
}

func (f *fakeTerminalGatewayServiceClient) GetTerminalStatus(context.Context, *tgv1.GetTerminalStatusRequest, ...grpc.CallOption) (*tgv1.GetTerminalStatusResponse, error) {
	return nil, errors.New("not implemented")
}

// ─── builders ────────────────────────────────────────────────────────────────

func mustReceipt(t *testing.T) valueobject.ReceiptPayload {
	t.Helper()
	r, err := valueobject.NewReceiptPayload(
		"tx-1", "Merchant Inc", "TERM-001",
		"APPROVED", 5000, "ARS", "1234", "VISA", "CHIP",
		"2026-01-01T10:00:00Z",
	)
	if err != nil {
		t.Fatalf("NewReceiptPayload() error = %v", err)
	}
	r.AuthCode = "AB1234"
	return r
}

func newTestNotification(t *testing.T) *aggregate.Notification {
	t.Helper()
	n, err := aggregate.NewNotification(domain.NewTransactionID(), domain.NewMerchantID(), valueobject.ChannelTerminalWebSocket, mustReceipt(t))
	if err != nil {
		t.Fatalf("NewNotification() error = %v", err)
	}
	return n
}

// ─── SendReceipt ─────────────────────────────────────────────────────────────

func TestSendReceipt_Success(t *testing.T) {
	fake := &fakeTerminalGatewayServiceClient{
		sendReceiptResp: &tgv1.SendReceiptResponse{Delivered: true, ErrorReason: ""},
	}
	c := &TerminalGatewayClient{client: fake}
	n := newTestNotification(t)

	delivered, reason, err := c.SendReceipt(context.Background(), n)
	if err != nil {
		t.Fatalf("SendReceipt() error = %v", err)
	}
	if !delivered {
		t.Error("delivered = false, want true")
	}
	if reason != "" {
		t.Errorf("reason = %q, want empty", reason)
	}

	req := fake.lastReq
	if req == nil {
		t.Fatal("SendReceipt was not called with a request")
	}
	receipt := n.Receipt()
	if req.TerminalId != receipt.TerminalCode {
		t.Errorf("TerminalId = %q, want %q", req.TerminalId, receipt.TerminalCode)
	}
	if req.TransactionId != receipt.TransactionID {
		t.Errorf("TransactionId = %q, want %q", req.TransactionId, receipt.TransactionID)
	}
	if req.Receipt.MerchantName != receipt.MerchantName {
		t.Errorf("Receipt.MerchantName = %q, want %q", req.Receipt.MerchantName, receipt.MerchantName)
	}
	if req.Receipt.Result != tgv1.ReceiptResult_RECEIPT_RESULT_APPROVED {
		t.Errorf("Receipt.Result = %v, want %v", req.Receipt.Result, tgv1.ReceiptResult_RECEIPT_RESULT_APPROVED)
	}
	if req.Receipt.AuthCode != receipt.AuthCode {
		t.Errorf("Receipt.AuthCode = %q, want %q", req.Receipt.AuthCode, receipt.AuthCode)
	}
	if req.Receipt.AmountCents != receipt.AmountCents {
		t.Errorf("Receipt.AmountCents = %d, want %d", req.Receipt.AmountCents, receipt.AmountCents)
	}
}

func TestSendReceipt_NotDelivered(t *testing.T) {
	fake := &fakeTerminalGatewayServiceClient{
		sendReceiptResp: &tgv1.SendReceiptResponse{Delivered: false, ErrorReason: "terminal offline"},
	}
	c := &TerminalGatewayClient{client: fake}

	delivered, reason, err := c.SendReceipt(context.Background(), newTestNotification(t))
	if err != nil {
		t.Fatalf("SendReceipt() error = %v", err)
	}
	if delivered {
		t.Error("delivered = true, want false")
	}
	if reason != "terminal offline" {
		t.Errorf("reason = %q, want %q", reason, "terminal offline")
	}
}

func TestSendReceipt_ClientError(t *testing.T) {
	fake := &fakeTerminalGatewayServiceClient{sendReceiptErr: errors.New("connection refused")}
	c := &TerminalGatewayClient{client: fake}

	delivered, reason, err := c.SendReceipt(context.Background(), newTestNotification(t))
	if err == nil || !strings.Contains(err.Error(), "nt grpc client: SendReceipt") {
		t.Fatalf("error = %v, want it to contain %q", err, "nt grpc client: SendReceipt")
	}
	if delivered {
		t.Error("delivered = true, want false")
	}
	if reason != "" {
		t.Errorf("reason = %q, want empty", reason)
	}
}

// ─── toProtoResult ────────────────────────────────────────────────────────────

func TestToProtoResult(t *testing.T) {
	tests := []struct {
		in   string
		want tgv1.ReceiptResult
	}{
		{"APPROVED", tgv1.ReceiptResult_RECEIPT_RESULT_APPROVED},
		{"REJECTED", tgv1.ReceiptResult_RECEIPT_RESULT_REJECTED},
		{"REVERSED", tgv1.ReceiptResult_RECEIPT_RESULT_REVERSED},
		{"BOGUS", tgv1.ReceiptResult_RECEIPT_RESULT_UNSPECIFIED},
		{"", tgv1.ReceiptResult_RECEIPT_RESULT_UNSPECIFIED},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := toProtoResult(tc.in); got != tc.want {
				t.Errorf("toProtoResult(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ─── NewTerminalGatewayClient / Close ────────────────────────────────────────

func TestNewTerminalGatewayClient_SuccessAndClose(t *testing.T) {
	// grpc.NewClient no marca (no dial real) — solo valida y parsea el target,
	// así que esto debe funcionar sin un servidor gRPC real escuchando.
	c, err := NewTerminalGatewayClient("localhost:50051", nil)
	if err != nil {
		t.Fatalf("NewTerminalGatewayClient() error = %v", err)
	}
	if c == nil {
		t.Fatal("NewTerminalGatewayClient() = nil, want non-nil")
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}
