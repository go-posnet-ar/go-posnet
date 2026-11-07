package client

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
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	tgv1 "github.com/juantevez/go-posnet/pkg/proto/terminalgateway/v1"
)

// testPKI son los certificados de prueba: una CA, el servidor (localhost) y un cliente firmados
// por ella, y un cliente de otra CA.
type testPKI struct {
	caPath, serverCert, serverKey, clientCert, clientKey, otherClientCert, otherClientKey string
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
	return p
}

type receiptServer struct {
	tgv1.UnimplementedTerminalGatewayServiceServer
}

func (receiptServer) SendReceipt(context.Context, *tgv1.SendReceiptRequest) (*tgv1.SendReceiptResponse, error) {
	return &tgv1.SendReceiptResponse{Delivered: true}, nil
}

// startMTLSServer simula a Terminal Gateway con mTLS y devuelve su puerto.
func startMTLSServer(t *testing.T, p testPKI) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatalf("LoadX509KeyPair() error = %v", err)
	}
	caPEM, err := os.ReadFile(p.caPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	})))
	tgv1.RegisterTerminalGatewayServiceServer(srv, receiptServer{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

func TestTerminalGatewayClient_MTLS(t *testing.T) {
	p := newTestPKI(t)
	port := startMTLSServer(t, p)

	send := func(target string, cfg *tls.Config) error {
		c, err := NewTerminalGatewayClient(target, cfg)
		if err != nil {
			t.Fatalf("NewTerminalGatewayClient() error = %v", err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err = c.client.SendReceipt(ctx, &tgv1.SendReceiptRequest{TerminalId: "t", TransactionId: "x"})
		return err
	}

	cfg, err := TLSConfig(p.caPath, p.clientCert, p.clientKey, "")
	if err != nil {
		t.Fatalf("TLSConfig() error = %v", err)
	}
	if err := send("localhost:"+port, cfg); err != nil {
		t.Fatalf("con mTLS: %v", err)
	}

	// El certificado del servidor no incluye este nombre: hay que declararlo.
	if err := send("127.0.0.2:"+port, cfg); err == nil {
		t.Error("con un host que no está en el certificado: error = nil")
	}
	named, err := TLSConfig(p.caPath, p.clientCert, p.clientKey, "localhost")
	if err != nil {
		t.Fatalf("TLSConfig() error = %v", err)
	}
	if err := send("127.0.0.1:"+port, named); err != nil {
		t.Errorf("con GRPC_CLIENT_TLS_SERVER_NAME: %v", err)
	}

	withoutCert, err := TLSConfig(p.caPath, "", "", "")
	if err != nil {
		t.Fatalf("TLSConfig() error = %v", err)
	}
	if err := send("localhost:"+port, withoutCert); err == nil {
		t.Error("sin certificado de cliente: error = nil, want rechazado")
	}
	if err := send("localhost:"+port, nil); err == nil {
		t.Error("sin TLS: error = nil, want rechazado")
	}
}

func TestTLSConfig_Errors(t *testing.T) {
	p := newTestPKI(t)
	if cfg, err := TLSConfig("", "", "", ""); cfg != nil || err != nil {
		t.Errorf("TLSConfig(sin CA) = %v, %v, want nil, nil", cfg, err)
	}
	if _, err := TLSConfig(filepath.Join(t.TempDir(), "no.pem"), "", "", ""); err == nil {
		t.Error("TLSConfig(CA inexistente) error = nil")
	}
	if _, err := TLSConfig(p.clientKey, "", "", ""); err == nil {
		t.Error("TLSConfig(CA sin certificados) error = nil")
	}
	if _, err := TLSConfig(p.caPath, p.clientCert, p.caPath, ""); err == nil {
		t.Error("TLSConfig(clave inválida) error = nil")
	}
}
