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
// clientes con un certificado firmado por esa CA y, si allowedClients no está vacío, cuyo CN o
// algún SAN DNS sea uno de esos nombres. Sin certPath devuelve nil, y el servidor no usa TLS: la
// configuración solo lo permite en desarrollo.
func TLSOption(certPath, keyPath, clientCAPath string, allowedClients []string) (grpc.ServerOption, error) {
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
		if len(allowedClients) > 0 {
			cfg.VerifyConnection = verifyClientName(allowedClients)
		}
	}
	return grpc.Creds(credentials.NewTLS(cfg)), nil
}

// verifyClientName rechaza un certificado válido de la CA que no sea de un cliente permitido:
// sin esto, cualquier servicio con un certificado de la misma CA podría llamar.
func verifyClientName(allowed []string) func(tls.ConnectionState) error {
	names := make(map[string]bool, len(allowed))
	for _, n := range allowed {
		names[n] = true
	}
	return func(cs tls.ConnectionState) error {
		if len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
			return fmt.Errorf("grpc server: client certificate not verified")
		}
		leaf := cs.VerifiedChains[0][0]
		if names[leaf.Subject.CommonName] {
			return nil
		}
		for _, dns := range leaf.DNSNames {
			if names[dns] {
				return nil
			}
		}
		return fmt.Errorf("grpc server: client %q is not allowed", leaf.Subject.CommonName)
	}
}
