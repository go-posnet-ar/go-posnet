package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TLSOption arma las credenciales del servidor. Con clientCAPath exige mTLS: solo acepta
// clientes con un certificado firmado por esa CA. Sin certPath devuelve nil, y el servidor no
// usa TLS: la configuración solo lo permite en desarrollo.
func TLSOption(certPath, keyPath, clientCAPath string) (grpc.ServerOption, error) {
	if certPath == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("grpc server: load TLS certificate: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if clientCAPath != "" {
		pem, err := os.ReadFile(clientCAPath)
		if err != nil {
			return nil, fmt.Errorf("grpc server: read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("grpc server: client CA %q has no PEM certificates", clientCAPath)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return grpc.Creds(credentials.NewTLS(cfg)), nil
}
