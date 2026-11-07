package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/config"
)

func TestTerminalTokens(t *testing.T) {
	cfgFor := func(environment, path string) *config.Config {
		cfg := &config.Config{TerminalTokensFile: path}
		cfg.OTEL.Environment = environment
		return cfg
	}

	t.Run("sin archivo en desarrollo, la API queda abierta", func(t *testing.T) {
		tokens, err := terminalTokens(cfgFor("development", ""))
		if err != nil || tokens.Configured() {
			t.Errorf("terminalTokens() = %v, %v, want abierta", tokens, err)
		}
	})

	for _, env := range []string{"production", "staging", ""} {
		t.Run("sin archivo en "+env+", no arranca", func(t *testing.T) {
			if _, err := terminalTokens(cfgFor(env, "")); err == nil {
				t.Error("terminalTokens() error = nil, want error")
			}
		})
	}

	t.Run("con archivo, la API exige token", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.json")
		if err := os.WriteFile(path, []byte(`[]`), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		tokens, err := terminalTokens(cfgFor("production", path))
		if err != nil || !tokens.Configured() {
			t.Errorf("terminalTokens() = %v, %v, want configurado", tokens, err)
		}
	})

	t.Run("archivo inválido, no arranca", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.json")
		if err := os.WriteFile(path, []byte(`{`), 0o600); err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}
		if _, err := terminalTokens(cfgFor("development", path)); err == nil {
			t.Error("terminalTokens() error = nil, want error")
		}
	})
}

// writeSelfSignedCert genera un certificado de prueba y devuelve sus paths.
func writeSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey() error = %v", err)
	}
	dir := t.TempDir()
	certPath, keyPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return certPath, keyPath
}

func TestConfigureHTTPTLS(t *testing.T) {
	t.Run("sin certificado, HTTP", func(t *testing.T) {
		srv := &http.Server{}
		if err := configureHTTPTLS(srv, &config.Config{}, true); err != nil || srv.TLSConfig != nil {
			t.Errorf("configureHTTPTLS() = %v, TLSConfig = %v, want HTTP", err, srv.TLSConfig)
		}
	})

	t.Run("con certificado, solo HTTPS desde TLS 1.2", func(t *testing.T) {
		certPath, keyPath := writeSelfSignedCert(t)
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})}
		if err := configureHTTPTLS(srv, &config.Config{HTTPTLSCert: certPath, HTTPTLSKey: keyPath}, true); err != nil {
			t.Fatalf("configureHTTPTLS() error = %v", err)
		}
		if srv.TLSConfig == nil || srv.TLSConfig.MinVersion != tls.VersionTLS12 {
			t.Fatalf("TLSConfig = %+v, want TLS 1.2 mínimo", srv.TLSConfig)
		}

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Listen() error = %v", err)
		}
		go func() { _ = srv.ServeTLS(ln, "", "") }()
		t.Cleanup(func() { _ = srv.Close() })
		url := "https://" + ln.Addr().String() + "/"

		client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // certificado de prueba
		}}
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET https: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("status = %d, want 204", resp.StatusCode)
		}

		// En texto plano, el token no puede viajar.
		plain := &http.Client{Timeout: 5 * time.Second}
		if resp, err := plain.Get("http://" + ln.Addr().String() + "/"); err == nil {
			resp.Body.Close()
			if resp.StatusCode < 400 {
				t.Errorf("HTTP en claro: status = %d, want rechazado", resp.StatusCode)
			}
		}
	})

	t.Run("certificado ilegible, no arranca", func(t *testing.T) {
		cfg := &config.Config{HTTPTLSCert: filepath.Join(t.TempDir(), "no.pem"), HTTPTLSKey: "no.pem"}
		if err := configureHTTPTLS(&http.Server{}, cfg, true); err == nil {
			t.Error("configureHTTPTLS() error = nil, want error")
		}
	})
}

func TestPayTokens(t *testing.T) {
	t.Run("sin clave en desarrollo, usa una efímera", func(t *testing.T) {
		cfg := &config.Config{}
		cfg.OTEL.Environment = "development"
		p, err := payTokens(cfg)
		if err != nil || p == nil || p.Required() {
			t.Fatalf("payTokens() = %v, %v, want un emisor que no exige el token", p, err)
		}
		if other, _ := payTokens(cfg); other.Issue("x") == p.Issue("x") {
			t.Error("dos claves efímeras firman igual")
		}
	})
	t.Run("con clave, firma siempre igual", func(t *testing.T) {
		cfg := &config.Config{PayTokenKey: "0123456789abcdef0123456789abcdef", PayTokenRequired: true}
		a, _ := payTokens(cfg)
		b, _ := payTokens(cfg)
		if a.Issue("x") != b.Issue("x") || !a.Required() {
			t.Error("dos instancias con la misma clave tienen que emitir el mismo token")
		}
	})
}
