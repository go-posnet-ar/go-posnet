package middleware

import (
	"encoding/json"
	"math"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

// RateLimiter limita los pedidos por cliente con un balde de fichas: perMinute fichas por minuto,
// con ráfagas de hasta perMinute. Vive en memoria, por instancia.
type RateLimiter struct {
	mu        sync.Mutex
	perMinute float64
	buckets   map[netip.Prefix]*bucket
	clientIP  ClientIP
	now       func() time.Time
	maxKeys   int
}

type bucket struct {
	tokens float64
	last   time.Time
}

// NewRateLimiter crea el límite.
func NewRateLimiter(perMinute int, clientIP ClientIP) *RateLimiter {
	return &RateLimiter{
		perMinute: float64(perMinute),
		buckets:   map[netip.Prefix]*bucket{},
		clientIP:  clientIP,
		now:       time.Now,
		maxKeys:   100_000,
	}
}

// clientKey agrupa IPv6 por /64: un solo host suele disponer de toda esa red.
func clientKey(ip netip.Addr) netip.Prefix {
	bits := 32
	if ip.Is6() {
		bits = 64
	}
	key, err := ip.Prefix(bits)
	if err != nil {
		return netip.PrefixFrom(ip, ip.BitLen())
	}
	return key
}

// allow consume una ficha. Si no hay, devuelve cuánto falta para la próxima.
func (l *RateLimiter) allow(ip netip.Addr) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	key := clientKey(ip)
	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= l.maxKeys {
			l.purge(now)
		}
		// Sin lugar, se deja pasar: bloquear a todos sería una forma de cortar el servicio.
		if len(l.buckets) >= l.maxKeys {
			return 0, true
		}
		b = &bucket{tokens: l.perMinute, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.perMinute, b.tokens+now.Sub(b.last).Minutes()*l.perMinute)
	b.last = now
	if b.tokens < 1 {
		wait := time.Duration((1 - b.tokens) / l.perMinute * float64(time.Minute))
		return wait, false
	}
	b.tokens--
	return 0, true
}

// purge borra los baldes llenos: un cliente que no volvió no necesita recordarse.
func (l *RateLimiter) purge(now time.Time) {
	for key, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Minutes()*l.perMinute >= l.perMinute {
			delete(l.buckets, key)
		}
	}
}

// Limit aplica el límite a next.
func (l *RateLimiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if wait, ok := l.allow(l.clientIP.Of(r)); !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
			WriteError(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS", "too many requests; try again later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WriteError responde un error con la misma forma que los servicios: {"error", "message"}.
func WriteError(w http.ResponseWriter, code int, errCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": errCode, "message": message})
}
