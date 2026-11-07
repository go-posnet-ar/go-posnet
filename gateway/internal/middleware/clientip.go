// Package middleware contiene los middlewares HTTP del API Gateway.
package middleware

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// ClientIP resuelve la IP del cliente. Sin proxies de confianza, es la de la conexión:
// X-Forwarded-For lo puede escribir cualquiera.
type ClientIP struct {
	TrustedProxies []netip.Prefix
}

// Remote devuelve la IP de la conexión.
func Remote(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.IPv4Unspecified()
	}
	return ip.Unmap()
}

// Of devuelve la IP del cliente: la de la conexión o, si viene de un proxy de confianza, la
// última dirección de X-Forwarded-For que no sea otro proxy de confianza.
func (c ClientIP) Of(r *http.Request) netip.Addr {
	ip := Remote(r)
	if !c.Trusted(ip) {
		return ip
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip
		}
		hop = hop.Unmap()
		if !c.Trusted(hop) {
			return hop
		}
		ip = hop
	}
	return ip
}

// Trusted indica si ip es un proxy de confianza.
func (c ClientIP) Trusted(ip netip.Addr) bool {
	for _, p := range c.TrustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
