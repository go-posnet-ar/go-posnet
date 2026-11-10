package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juantevez/go-posnet/pkg/observability"
)

// Role indica qué puede hacer un token. Cada rol incluye a los anteriores.
type Role int

const (
	// RoleReader consulta lotes, revisiones y zonas.
	RoleReader Role = iota + 1
	// RoleOperator además cierra, reenvía y reabre lotes.
	RoleOperator
	// RoleSupervisor además toma las decisiones financieras: resolve-dispute, settle,
	// revisiones de reversas y zonas horarias.
	RoleSupervisor
)

func parseRole(s string) (Role, bool) {
	switch s {
	case "reader":
		return RoleReader, true
	case "operator":
		return RoleOperator, true
	case "supervisor":
		return RoleSupervisor, true
	}
	return 0, false
}

// Principal es quien hizo el pedido, según su token.
type Principal struct {
	Name string
	Role Role
	// Verified es false con SETTLEMENT_ADMIN_API_KEY: el token no identifica a nadie, y
	// operator_id sigue siendo lo que declara quien llama.
	Verified bool
}

type principalKey struct{}

func principalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// ─── Tokens ───────────────────────────────────────────────────────────────────

type tokenEntry struct {
	principal Principal
	hash      [sha256.Size]byte
	expiresAt time.Time // Cero = no vence
}

// TokenStore valida tokens. Los nombrados se leen de un archivo, que solo guarda sus
// hashes y se puede cambiar sin reiniciar el servicio.
type TokenStore struct {
	path    string
	legacy  *tokenEntry
	entries atomic.Pointer[[]tokenEntry]
	raw     []byte // Último contenido leído; solo lo usa Run
	now     func() time.Time
}

// NewTokenStore crea el almacén. Un archivo inválido es un error: con otro criterio, el
// servicio arrancaría sin los tokens que el operador cree tener.
func NewTokenStore(legacyKey, path string) (*TokenStore, error) {
	s := &TokenStore{path: path, now: time.Now}
	if legacyKey != "" {
		s.legacy = &tokenEntry{
			principal: Principal{Name: "legacy-admin-key", Role: RoleSupervisor},
			hash:      sha256.Sum256([]byte(legacyKey)),
		}
	}
	s.entries.Store(&[]tokenEntry{})
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read tokens file: %w", err)
	}
	entries, err := parseTokens(raw)
	if err != nil {
		return nil, fmt.Errorf("parse tokens file: %w", err)
	}
	s.entries.Store(&entries)
	s.raw = raw
	return s, nil
}

func parseTokens(raw []byte) ([]tokenEntry, error) {
	var file []struct {
		Name      string     `json:"name"`
		Role      string     `json:"role"`
		SHA256    string     `json:"sha256"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, err
	}
	names := map[string]bool{}
	hashes := map[[sha256.Size]byte]bool{}
	entries := make([]tokenEntry, 0, len(file))
	for i, t := range file {
		if t.Name == "" {
			return nil, fmt.Errorf("token %d: name cannot be empty", i)
		}
		if names[t.Name] {
			return nil, fmt.Errorf("token %q: duplicated name", t.Name)
		}
		names[t.Name] = true
		role, ok := parseRole(t.Role)
		if !ok {
			return nil, fmt.Errorf("token %q: role must be reader, operator or supervisor", t.Name)
		}
		sum, err := hex.DecodeString(t.SHA256)
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("token %q: sha256 must have 64 hex characters", t.Name)
		}
		e := tokenEntry{principal: Principal{Name: t.Name, Role: role, Verified: true}}
		copy(e.hash[:], sum)
		if hashes[e.hash] {
			return nil, fmt.Errorf("token %q: duplicated sha256", t.Name)
		}
		hashes[e.hash] = true
		if t.ExpiresAt != nil {
			e.expiresAt = *t.ExpiresAt
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Configured indica si hay algún token con el que entrar.
func (s *TokenStore) Configured() bool {
	return s != nil && (s.legacy != nil || len(*s.entries.Load()) > 0)
}

// UsesLegacyKey indica si sigue habilitada SETTLEMENT_ADMIN_API_KEY.
func (s *TokenStore) UsesLegacyKey() bool { return s != nil && s.legacy != nil }

// Authenticate busca el token. Compara hashes contra todas las entradas, sin cortar, para no
// revelar por el tiempo de respuesta si existe ni cuál es.
func (s *TokenStore) Authenticate(token string) (Principal, bool) {
	got := sha256.Sum256([]byte(token))
	now := s.now()
	var (
		found Principal
		ok    bool
	)
	match := func(e *tokenEntry) {
		if subtle.ConstantTimeCompare(got[:], e.hash[:]) == 1 && (e.expiresAt.IsZero() || now.Before(e.expiresAt)) {
			found, ok = e.principal, true
		}
	}
	entries := *s.entries.Load()
	for i := range entries {
		match(&entries[i])
	}
	if s.legacy != nil {
		match(s.legacy)
	}
	return found, ok
}

// Reload vuelve a leer el archivo. Si falla, se conservan los tokens vigentes: un archivo
// a medio escribir no debe dejar afuera a todos.
func (s *TokenStore) Reload() (changed bool, err error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, err
	}
	if bytes.Equal(raw, s.raw) {
		return false, nil
	}
	entries, err := parseTokens(raw)
	if err != nil {
		return false, err
	}
	s.entries.Store(&entries)
	s.raw = raw
	return true, nil
}

// Run relee el archivo periódicamente hasta que termine ctx.
func (s *TokenStore) Run(ctx context.Context, interval time.Duration) {
	if s.path == "" {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := s.Reload()
			if err != nil {
				slog.Error("admin tokens file not reloaded — keeping the current tokens", slog.String("error", err.Error()))
			} else if changed {
				slog.Warn("admin tokens reloaded", slog.Int("tokens", len(*s.entries.Load())))
			}
		}
	}
}

// ─── Límite de intentos ───────────────────────────────────────────────────────

const (
	authMaxFailures = 10
	authWindow      = time.Minute
	authBlock       = 5 * time.Minute
	authMaxClients  = 10_000
)

type authClient struct {
	failures     int
	windowStart  time.Time
	blockedUntil time.Time
}

// authLimiter frena los intentos fallidos por cliente. Vive en memoria, por instancia.
type authLimiter struct {
	mu      sync.Mutex
	clients map[netip.Prefix]*authClient
	now     func() time.Time
}

func newAuthLimiter() *authLimiter {
	return &authLimiter{clients: map[netip.Prefix]*authClient{}, now: time.Now}
}

// clientKey agrupa IPv6 por /64: un solo host suele disponer de toda esa red.
func clientKey(ip netip.Addr) netip.Prefix {
	ip = ip.Unmap()
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

// blocked devuelve cuánto falta para que el cliente pueda volver a intentar.
func (l *authLimiter) blocked(ip netip.Addr) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.clients[clientKey(ip)]
	if !ok {
		return 0, false
	}
	remaining := c.blockedUntil.Sub(l.now())
	return remaining, remaining > 0
}

func (l *authLimiter) fail(ip netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	key := clientKey(ip)
	c, ok := l.clients[key]
	if !ok {
		if len(l.clients) >= authMaxClients {
			l.purge(now)
		}
		// Sin lugar, no se registra: el token sigue siendo imposible de adivinar, y frenar a
		// todos por esto le daría a un atacante una forma de cortar el servicio.
		if len(l.clients) >= authMaxClients {
			return
		}
		c = &authClient{windowStart: now}
		l.clients[key] = c
	}
	if now.Sub(c.windowStart) > authWindow {
		c.failures, c.windowStart = 0, now
	}
	c.failures++
	if c.failures >= authMaxFailures {
		c.blockedUntil = now.Add(authBlock)
		c.failures, c.windowStart = 0, now
	}
}

func (l *authLimiter) succeed(ip netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.clients, clientKey(ip))
}

func (l *authLimiter) purge(now time.Time) {
	for key, c := range l.clients {
		if now.After(c.blockedUntil) && now.Sub(c.windowStart) > authWindow {
			delete(l.clients, key)
		}
	}
}

// ─── Middleware ───────────────────────────────────────────────────────────────

// Security reúne lo que necesita la autenticación de la API.
type Security struct {
	Tokens *TokenStore
	// TrustedProxies son los proxies cuyo X-Forwarded-For se acepta. Sin esto, el encabezado
	// se ignora: cualquiera podría declarar otra IP para evitar el límite de intentos.
	TrustedProxies []netip.Prefix
}

// clientIP devuelve la IP del cliente: la conexión o, si viene de un proxy de confianza, la
// última dirección de X-Forwarded-For que no sea otro proxy de confianza.
func (s Security) clientIP(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.IPv4Unspecified()
	}
	ip = ip.Unmap()
	if !s.trusted(ip) {
		return ip
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip
		}
		hop = hop.Unmap()
		if !s.trusted(hop) {
			return hop
		}
		ip = hop
	}
	return ip
}

func (s Security) trusted(ip netip.Addr) bool {
	for _, p := range s.TrustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// publicPaths son las rutas sin token: las usan Docker y Prometheus, y no exponen datos de
// comercios. Son exactas: una ruta nueva queda protegida por defecto.
var publicPaths = map[string]bool{"/healthz": true, "/readyz": true, "/metrics": true}

// requiredRole indica el rol que exige un pedido. Lo que no está previsto exige el máximo.
func requiredRole(r *http.Request) Role {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return RoleReader
	}
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/batches/") {
		for _, action := range []string{"/force-close", "/resubmit", "/reopen"} {
			if strings.HasSuffix(r.URL.Path, action) {
				return RoleOperator
			}
		}
	}
	// Reprocesar sigue el camino normal del evento, con su idempotencia. Descartarlo lo pierde:
	// queda en el rol máximo.
	if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/dead-letters/") && strings.HasSuffix(r.URL.Path, "/replay") {
		return RoleOperator
	}
	return RoleSupervisor
}

func authMiddleware(sec Security, next http.Handler) http.Handler {
	limiter := newAuthLimiter()
	return authMiddlewareWith(sec, limiter, next)
}

func authMiddlewareWith(sec Security, limiter *authLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) && publicPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if !sec.Tokens.Configured() {
			writeJSON(w, http.StatusServiceUnavailable,
				errResp("ADMIN_API_DISABLED", "no admin tokens are configured"))
			return
		}

		ip := sec.clientIP(r)
		log := observability.FromContext(r.Context()).With(
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("client_ip", ip.String()),
		)
		if remaining, blocked := limiter.blocked(ip); blocked {
			log.Warn("rejected admin request — too many failed attempts")
			w.Header().Set("Retry-After", strconv.Itoa(int(remaining.Seconds())+1))
			writeJSON(w, http.StatusTooManyRequests, errResp("TOO_MANY_ATTEMPTS", "too many failed attempts; try again later"))
			return
		}

		token, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		principal, ok := Principal{}, false
		if hasBearer {
			principal, ok = sec.Tokens.Authenticate(token)
		}
		if !ok {
			// Sin token no es un intento de adivinarlo: no cuenta para el límite.
			if hasBearer && token != "" {
				limiter.fail(ip)
			}
			log.Warn("rejected admin request")
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, errResp("UNAUTHORIZED", "missing or invalid admin token"))
			return
		}
		limiter.succeed(ip)

		if principal.Role < requiredRole(r) {
			log.Warn("rejected admin request — role not allowed", slog.String("token", principal.Name))
			writeJSON(w, http.StatusForbidden, errResp("FORBIDDEN", "this token cannot perform this operation"))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, principal)))
	})
}

// errOperatorMismatch indica que el body declara otro operador que el del token.
var errOperatorMismatch = errors.New("operator_id does not match the token")

// resolveOperator devuelve el operador de la auditoría: el del token, si lo identifica. Un
// operator_id distinto se rechaza: aceptarlo dejaría creer que quedó registrado ese nombre.
func resolveOperator(ctx context.Context, declared string) (string, error) {
	p, ok := principalFrom(ctx)
	if !ok || !p.Verified {
		return declared, nil
	}
	if declared != "" && declared != p.Name {
		return "", errOperatorMismatch
	}
	return p.Name, nil
}

// operator escribe el error si el operador no es válido.
func operator(w http.ResponseWriter, r *http.Request, declared string) (string, bool) {
	name, err := resolveOperator(r.Context(), declared)
	if err != nil {
		writeJSON(w, http.StatusForbidden, errResp("OPERATOR_MISMATCH", err.Error()))
		return "", false
	}
	return name, true
}
