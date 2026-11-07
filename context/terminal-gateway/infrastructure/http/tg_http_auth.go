package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// TerminalTokens autentica a los terminales. El archivo solo guarda el SHA-256 de cada token: si
// se filtra, no alcanza para entrar. Un terminal puede tener varios tokens, para rotarlos.
type TerminalTokens struct {
	entries []terminalToken
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
	return &TerminalTokens{entries: entries, now: time.Now}, nil
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
	return len(t.entries)
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
	for i := range t.entries {
		e := &t.entries[i]
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

// requireTerminal exige un token de terminal válido. Sin tokens configurados no exige nada.
func (t *TerminalTokens) requireTerminal(next http.HandlerFunc) http.HandlerFunc {
	if !t.Configured() {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		token, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		var (
			id domain.TerminalID
			ok bool
		)
		if hasBearer && token != "" {
			id, ok = t.Authenticate(token)
		}
		if !ok {
			observability.FromContext(r.Context()).Warn("rejected terminal request",
				slog.String("method", r.Method), slog.String("path", r.URL.Path))
			w.Header().Set("WWW-Authenticate", `Bearer realm="terminal-gateway"`)
			writeJSON(w, http.StatusUnauthorized, errResp("UNAUTHORIZED", "missing or invalid terminal token"))
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), terminalKey{}, id)))
	}
}

// writeForbidden responde que el terminal autenticado no puede operar sobre otro terminal.
func writeForbidden(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, errResp("FORBIDDEN", "terminal_id does not match the authenticated terminal"))
}
