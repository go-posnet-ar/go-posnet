// Package proxy contiene el reverse proxy y las rutas del API Gateway.
package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/juantevez/go-posnet/gateway/internal/middleware"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// Timeouts hacia los servicios. Sin ellos, un servicio colgado retiene conexiones del gateway.
const (
	upstreamDialTimeout           = 5 * time.Second
	upstreamResponseHeaderTimeout = 30 * time.Second
)

// NewTransport arma el transporte hacia los servicios. caPath verifica a los que usan HTTPS con
// un certificado propio; sin él, se usan las CA del sistema.
func NewTransport(caPath string) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: upstreamDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = upstreamResponseHeaderTimeout
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caPath != "" {
		pem, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read upstream CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("upstream CA %q has no PEM certificates", caPath)
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return transport, nil
}

// NewUpstream arma el proxy hacia un servicio. Conserva el path del pedido, y le envía la IP
// del cliente en X-Forwarded-For: el servicio la usa si declara al gateway en TRUSTED_PROXIES.
func NewUpstream(name, rawURL string, transport http.RoundTripper, clientIP middleware.ClientIP) (http.Handler, error) {
	target, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("upstream %s: %w", name, err)
	}
	return &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			// Rewrite descarta el X-Forwarded-For entrante. Solo se conserva si lo escribió un
			// proxy de confianza: si no, el cliente podría declarar otra IP.
			if clientIP.Trusted(middleware.Remote(pr.In)) {
				pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"]
			}
			pr.SetXForwarded()
			otel.GetTextMapPropagator().Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header))
		},
		ModifyResponse: func(resp *http.Response) error {
			// CORS lo responde el gateway: dos políticas en la misma respuesta las anula.
			for key := range resp.Header {
				if strings.HasPrefix(key, "Access-Control-") {
					resp.Header.Del(key)
				}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			observability.RecordError(r.Context(), err)
			observability.FromContext(r.Context()).Error("upstream request failed",
				slog.String("upstream", name), slog.String("error", err.Error()))
			var maxBytes *http.MaxBytesError
			switch {
			case errors.As(err, &maxBytes):
				middleware.WriteError(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "request body too large")
			case errors.Is(err, context.DeadlineExceeded):
				middleware.WriteError(w, http.StatusGatewayTimeout, "UPSTREAM_TIMEOUT", "the service did not respond in time")
			default:
				middleware.WriteError(w, http.StatusBadGateway, "UPSTREAM_UNAVAILABLE", "the service is unavailable")
			}
		},
	}, nil
}
