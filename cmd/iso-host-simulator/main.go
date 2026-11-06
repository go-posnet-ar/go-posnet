// Comando iso-host-simulator: simula el host del emisor por ISO 8583, para desarrollo.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/juantevez/go-posnet/context/authorization/infrastructure/iso8583/hostsim"
)

func main() {
	address := os.Getenv("ISO_HOST_ADDRESS")
	if address == "" {
		address = ":9101"
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		slog.Error("cannot listen", slog.String("error", err.Error()))
		os.Exit(1)
	}
	slog.Info("ISO 8583 host simulator listening", slog.String("address", ln.Addr().String()))
	if err := hostsim.New().Serve(ctx, ln); err != nil {
		slog.Error("server stopped", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
