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

	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// TerminalTokens autentica a los terminales. El archivo solo guarda el SHA-256 de cada token: si
// se filtra, no alcanza para entrar. Un terminal puede tener varios tokens, para rotarlos, y el
// archivo se puede cambiar sin reiniciar el servicio.
type TerminalTokens struct {
	path    string
	entries atomic.Pointer[[]terminalToken]
	raw     []byte // Último contenido leído; solo lo usa Run
	now     func() time.Time
}

type terminalToken struct {
	terminalID domain.TerminalID
	hash       [sha256.Size]byte
	expiresAt  time.Time // Cero = no vence
}

// LoadTerminalTokens lee el archivo de tokens. Sin path no hay tokens, y la API queda abierta:
// el wiring solo lo permite en desarrollo. Un archivo inválido es un error, para no arrancar sin
// los tokens que el operador cree tener.
func LoadTerminalTokens(path string) (*TerminalTokens, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read terminal tokens file: %w", err)
	}
	entries, err := parseTerminalTokens(raw)
	if err != nil {
		return nil, fmt.Errorf("parse terminal tokens file: %w", err)
	}
	t := &TerminalTokens{path: path, raw: raw, now: time.Now}
	t.entries.Store(&entries)
	return t, nil
}

// Reload vuelve a leer el archivo. Si falla, se conservan los tokens vigentes: un archivo a
// medio escribir no debe dejar afuera a todos los terminales.
func (t *TerminalTokens) Reload() (changed bool, err error) {
	raw, err := os.ReadFile(t.path)
	if err != nil {
		return false, err
	}
	if bytes.Equal(raw, t.raw) {
		return false, nil
	}
	entries, err := parseTerminalTokens(raw)
	if err != nil {
		return false, err
	}
	t.entries.Store(&entries)
	t.raw = raw
	return true, nil
}

// Run relee el archivo periódicamente hasta que termine ctx. Así, un token revocado deja de
// valer en interval, sin reiniciar el servicio.
func (t *TerminalTokens) Run(ctx context.Context, interval time.Duration) {
	if t == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			changed, err := t.Reload()
			if err != nil {
				slog.Error("terminal tokens file not reloaded — keeping the current tokens", slog.String("error", err.Error()))
			} else if changed {
				slog.Warn("terminal tokens reloaded", slog.Int("tokens", t.Count()))
			}
		}
	}
}

func parseTerminalTokens(raw []byte) ([]terminalToken, error) {
	var file []struct {
		TerminalID string     `json:"terminal_id"`
		SHA256     string     `json:"sha256"`
		ExpiresAt  *time.Time `json:"expires_at"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, err
	}
	hashes := map[[sha256.Size]byte]bool{}
	entries := make([]terminalToken, 0, len(file))
	for i, t := range file {
		id, err := domain.ParseTerminalID(t.TerminalID)
		if err != nil {
			return nil, fmt.Errorf("token %d: terminal_id must be a UUID", i)
		}
		sum, err := hex.DecodeString(t.SHA256)
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("token %d: sha256 must have 64 hex characters", i)
		}
		e := terminalToken{terminalID: id}
		copy(e.hash[:], sum)
		if hashes[e.hash] {
			return nil, fmt.Errorf("token %d: duplicated sha256", i)
		}
		hashes[e.hash] = true
		if t.ExpiresAt != nil {
			e.expiresAt = *t.ExpiresAt
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Configured indica si la API exige token. Un archivo sin tokens también cuenta: deja todo
// cerrado, en lugar de abrirlo.
func (t *TerminalTokens) Configured() bool { return t != nil }

// Count devuelve cuántos tokens hay.
func (t *TerminalTokens) Count() int {
	if t == nil {
		return 0
	}
	return len(*t.entries.Load())
}

// Authenticate devuelve el terminal del token. Compara contra todas las entradas, sin cortar,
// para no revelar por el tiempo de respuesta si el token existe.
func (t *TerminalTokens) Authenticate(token string) (domain.TerminalID, bool) {
	got := sha256.Sum256([]byte(token))
	now := t.now()
	var (
		found domain.TerminalID
		ok    bool
	)
	entries := *t.entries.Load()
	for i := range entries {
		e := &entries[i]
		if subtle.ConstantTimeCompare(got[:], e.hash[:]) == 1 && (e.expiresAt.IsZero() || now.Before(e.expiresAt)) {
			found, ok = e.terminalID, true
		}
	}
	return found, ok
}

type terminalKey struct{}

// authenticatedTerminal devuelve el terminal autenticado. Sin tokens configurados no hay ninguno,
// y los handlers usan lo que envía el cliente, como antes.
func authenticatedTerminal(ctx context.Context) (domain.TerminalID, bool) {
	id, ok := ctx.Value(terminalKey{}).(domain.TerminalID)
	return id, ok
}

// TerminalAuth reúne lo que necesita la autenticación de los terminales.
type TerminalAuth struct {
	tokens *TerminalTokens
	// trustedProxies son los proxies cuyo X-Forwarded-For se acepta. Sin esto, el encabezado se
	// ignora: cualquiera podría declarar otra IP para evitar el límite de intentos.
	trustedProxies []netip.Prefix
	limiter        *authLimiter
}

// NewTerminalAuth arma la autenticación. Sin tokens devuelve nil, y la API queda abierta.
func NewTerminalAuth(tokens *TerminalTokens, trustedProxies []netip.Prefix) *TerminalAuth {
	if !tokens.Configured() {
		return nil
	}
	return &TerminalAuth{tokens: tokens, trustedProxies: trustedProxies, limiter: newAuthLimiter()}
}

// Errores de Verify.
var (
	ErrInvalidTerminalToken = errors.New("missing or invalid terminal token")
	ErrTooManyAttempts      = errors.New("too many failed attempts")
)

// Verify autentica el token de un terminal que llega en el pedido r, con el límite de intentos
// por IP. Con ErrTooManyAttempts, retryAfter es cuánto falta para volver a intentar. Lo usan la
// API HTTP y el WebSocket, para que el límite sea uno solo.
func (a *TerminalAuth) Verify(r *http.Request, token string) (id domain.TerminalID, retryAfter time.Duration, err error) {
	ip := a.clientIP(r)
	if remaining, blocked := a.limiter.blocked(ip); blocked {
		return domain.TerminalID{}, remaining, ErrTooManyAttempts
	}
	if token == "" {
		// Sin token no es un intento de adivinarlo: no cuenta para el límite.
		return domain.TerminalID{}, 0, ErrInvalidTerminalToken
	}
	id, ok := a.tokens.Authenticate(token)
	if !ok {
		a.limiter.fail(ip)
		return domain.TerminalID{}, 0, ErrInvalidTerminalToken
	}
	a.limiter.succeed(ip)
	return id, 0, nil
}

// requireTerminal exige un token de terminal válido. Sin autenticación configurada no exige nada.
func (a *TerminalAuth) requireTerminal(next http.HandlerFunc) http.HandlerFunc {
	if a == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		log := observability.FromContext(r.Context()).With(
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("client_ip", a.clientIP(r).String()),
		)
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			token = ""
		}
		id, retryAfter, err := a.Verify(r, token)
		switch {
		case errors.Is(err, ErrTooManyAttempts):
			log.Warn("rejected terminal request — too many failed attempts")
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
			writeJSON(w, http.StatusTooManyRequests, errResp("TOO_MANY_ATTEMPTS", "too many failed attempts; try again later"))
			return
		case err != nil:
			log.Warn("rejected terminal request")
			w.Header().Set("WWW-Authenticate", `Bearer realm="terminal-gateway"`)
			writeJSON(w, http.StatusUnauthorized, errResp("UNAUTHORIZED", "missing or invalid terminal token"))
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), terminalKey{}, id)))
	}
}

// clientIP devuelve la IP del cliente: la de la conexión o, si viene de un proxy de confianza,
// la última dirección de X-Forwarded-For que no sea otro proxy de confianza.
func (a *TerminalAuth) clientIP(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.IPv4Unspecified()
	}
	ip = ip.Unmap()
	if !a.trusted(ip) {
		return ip
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip
		}
		hop = hop.Unmap()
		if !a.trusted(hop) {
			return hop
		}
		ip = hop
	}
	return ip
}

func (a *TerminalAuth) trusted(ip netip.Addr) bool {
	for _, p := range a.trustedProxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
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

// authLimiter frena los intentos fallidos por cliente. Vive en memoria, por instancia. Un
// intento exitoso desde la misma IP borra los fallos: un terminal mal configurado no bloquea a
// los demás de su local mientras alguno se autentique.
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

// writeForbidden responde que el terminal autenticado no puede operar sobre otro terminal.
func writeForbidden(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, errResp("FORBIDDEN", "terminal_id does not match the authenticated terminal"))
}
