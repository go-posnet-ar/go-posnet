package server_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/infrastructure/grpc/server"
	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
)

// testPKI son los certificados de prueba: una CA, el servidor (localhost) y un cliente firmados
// por ella, y un cliente de otra CA.
type testPKI struct {
	caPath, serverCert, serverKey, clientCert, clientKey, otherClientCert, otherClientKey string
	// otro servicio, con un certificado válido de la misma CA
	sameCACert, sameCAKey string
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	write := func(name, typ string, der []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		return path
	}
	newKey := func() *ecdsa.PrivateKey {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("GenerateKey() error = %v", err)
		}
		return key
	}
	serial := int64(1)
	newCA := func(name string) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
		key := newKey()
		serial++
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatalf("CreateCertificate(CA) error = %v", err)
		}
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatalf("ParseCertificate() error = %v", err)
		}
		return cert, key, der
	}
	issue := func(name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, usage x509.ExtKeyUsage, server bool) (string, string) {
		key := newKey()
		serial++
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		}
		if server {
			tmpl.DNSNames = []string{"localhost"}
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("CreateCertificate(%s) error = %v", name, err)
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("MarshalECPrivateKey() error = %v", err)
		}
		return write(name+".pem", "CERTIFICATE", der), write(name+".key", "EC PRIVATE KEY", keyDER)
	}

	ca, caKey, caDER := newCA("posnet-test-ca")
	other, otherKey, _ := newCA("other-ca")
	p := testPKI{caPath: write("ca.pem", "CERTIFICATE", caDER)}
	p.serverCert, p.serverKey = issue("terminal-gateway", ca, caKey, x509.ExtKeyUsageServerAuth, true)
	p.clientCert, p.clientKey = issue("notification", ca, caKey, x509.ExtKeyUsageClientAuth, false)
	p.otherClientCert, p.otherClientKey = issue("intruder", other, otherKey, x509.ExtKeyUsageClientAuth, false)
	p.sameCACert, p.sameCAKey = issue("fraud-detection", ca, caKey, x509.ExtKeyUsageClientAuth, false)
	return p
}

// startTLSServer arranca el servidor con mTLS y devuelve su dirección.
func startTLSServer(t *testing.T, p testPKI) string {
	t.Helper()
	opt, err := server.TLSOption(p.serverCert, p.serverKey, p.caPath, []string{"notification"})
	if err != nil {
		t.Fatalf("TLSOption() error = %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	srv := server.NewTerminalGatewayServer(nil, query.NewSessionQueryHandler(&fakeSessionRepo{}))
	go func() { _ = server.Start(srv, port, opt) }()
	t.Cleanup(srv.GracefulStop)
	return net.JoinHostPort("localhost", itoa(port))
}

func itoa(n int) string { return (&big.Int{}).SetInt64(int64(n)).String() }

// call hace una llamada que el servidor rechaza por el argumento: si llega a eso, el handshake
// funcionó. Un handshake rechazado da Unavailable.
func call(t *testing.T, addr string, creds credentials.TransportCredentials) grpccodes.Code {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer conn.Close()
	var code grpccodes.Code
	for i := 0; i < 20; i++ { // el servidor arranca en otra goroutine
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = tgv1.NewTerminalGatewayServiceClient(conn).GetTerminalStatus(ctx, &tgv1.GetTerminalStatusRequest{})
		cancel()
		code = grpcstatus.Code(err)
		if code != grpccodes.Unavailable {
			return code
		}
		time.Sleep(50 * time.Millisecond)
	}
	return code
}

func clientTLS(t *testing.T, p testPKI, certPath, keyPath string) credentials.TransportCredentials {
	t.Helper()
	caPEM, err := os.ReadFile(p.caPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if certPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			t.Fatalf("LoadX509KeyPair() error = %v", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(cfg)
}

func TestTLSOption_RequiresClientCertificate(t *testing.T) {
	p := newTestPKI(t)
	addr := startTLSServer(t, p)

	if code := call(t, addr, clientTLS(t, p, p.clientCert, p.clientKey)); code != grpccodes.InvalidArgument {
		t.Fatalf("cliente con certificado de la CA: code = %v, want InvalidArgument (handshake aceptado)", code)
	}
	rejected := map[string]credentials.TransportCredentials{
		"sin TLS":                      insecure.NewCredentials(),
		"sin certificado":              clientTLS(t, p, "", ""),
		"certificado de otra CA":       clientTLS(t, p, p.otherClientCert, p.otherClientKey),
		"otro servicio de la misma CA": clientTLS(t, p, p.sameCACert, p.sameCAKey),
	}
	for name, creds := range rejected {
		t.Run(name, func(t *testing.T) {
			if code := call(t, addr, creds); code != grpccodes.Unavailable {
				t.Errorf("code = %v, want Unavailable (handshake rechazado)", code)
			}
		})
	}
}

func TestTLSOption_Errors(t *testing.T) {
	p := newTestPKI(t)
	if opt, err := server.TLSOption("", "", "", nil); opt != nil || err != nil {
		t.Errorf("TLSOption(sin certificado) = %v, %v, want nil, nil", opt, err)
	}
	if _, err := server.TLSOption(p.serverCert, p.caPath, "", nil); err == nil {
		t.Error("TLSOption(clave inválida) error = nil")
	}
	if _, err := server.TLSOption(p.serverCert, p.serverKey, p.serverKey, nil); err == nil {
		t.Error("TLSOption(CA sin certificados) error = nil")
	}
}
