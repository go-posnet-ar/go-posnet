package server_test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/infrastructure/grpc/server"
)

func TestNewSettlementServer(t *testing.T) {
	srv := server.NewSettlementServer()
	if srv == nil {
		t.Fatal("NewSettlementServer() = nil, want non-nil")
	}
}

func TestStart_ListenError(t *testing.T) {
	// Ocupar un puerto libre primero para forzar el error de bind en Start().
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer occupied.Close()

	port := occupied.Addr().(*net.TCPAddr).Port

	srv := server.NewSettlementServer()
	err = server.Start(srv, port)
	if err == nil {
		t.Fatal("Start() error = nil, want bind error")
	}
	if !strings.Contains(err.Error(), "listen on port") {
		t.Errorf("error = %q, want it to contain %q", err.Error(), "listen on port")
	}
}

func TestStop_EndsServing(t *testing.T) {
	srv := server.NewSettlementServer()

	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(srv, 0) }()

	select {
	case err := <-errCh:
		t.Fatalf("Start() returned early with error = %v, want it to keep serving", err)
	case <-time.After(200 * time.Millisecond):
	}

	srv.Stop()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start() error = %v, want nil after Stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not return after Stop")
	}
}

func TestStop_BeforeStartPreventsServing(t *testing.T) {
	srv := server.NewSettlementServer()
	srv.Stop()

	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(srv, 0) }()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() kept serving after Stop")
	}
}
