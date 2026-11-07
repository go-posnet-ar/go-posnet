package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig arma la configuración TLS hacia Terminal Gateway: caPath verifica al servidor, y
// certPath y keyPath son el certificado del cliente para mTLS. serverName reemplaza al host del
// target cuando el certificado del servidor no lo incluye. Sin caPath devuelve nil, y la
// conexión no usa TLS: la configuración solo lo permite en desarrollo.
func TLSConfig(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	if caPath == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("nt grpc client: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("nt grpc client: CA %q has no PEM certificates", caPath)
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
	if certPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("nt grpc client: load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
