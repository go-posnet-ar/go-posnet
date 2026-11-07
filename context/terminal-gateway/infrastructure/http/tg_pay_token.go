package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
)

// SessionTokenHeader es el encabezado con el token de la sesión, en /pay y /status.
const SessionTokenHeader = "X-Session-Token"

// PayTokens emite y verifica el token de una sesión: lo que prueba que quien llama a /pay o
// /status vio el QR, y no solo adivinó o leyó el ID. Es un HMAC del ID, así que no se guarda:
// se recalcula para verificarlo, y vale mientras exista la sesión.
type PayTokens struct {
	keys     [][]byte // La primera firma; las demás solo verifican, para rotar la clave
	required bool
}

// NewPayTokens crea el emisor. Con required, /pay y /status rechazan un pedido sin token; sin
// él, lo aceptan, para que los clientes que todavía no lo envían sigan funcionando. Un token
// inválido se rechaza siempre.
func NewPayTokens(key []byte, previous []byte, required bool) *PayTokens {
	keys := [][]byte{key}
	if len(previous) > 0 {
		keys = append(keys, previous)
	}
	return &PayTokens{keys: keys, required: required}
}

// Required indica si /pay y /status exigen el token.
func (p *PayTokens) Required() bool { return p != nil && p.required }

func sign(key []byte, txID string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("posnet-pay-token:v1:" + txID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Issue devuelve el token de la sesión txID.
func (p *PayTokens) Issue(txID string) string { return sign(p.keys[0], txID) }

func (p *PayTokens) valid(txID, token string) bool {
	ok := false
	for _, key := range p.keys {
		// Se comparan todas, sin cortar, para no revelar por el tiempo cuál coincidió.
		if hmac.Equal([]byte(sign(key, txID)), []byte(token)) {
			ok = true
		}
	}
	return ok
}

// requireSessionToken verifica el token de la sesión {id}. Sin PayTokens no verifica nada.
func (p *PayTokens) requireSessionToken(next http.HandlerFunc) http.HandlerFunc {
	if p == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(SessionTokenHeader)
		if token == "" {
			if !p.required {
				next(w, r)
				return
			}
			writeJSON(w, http.StatusUnauthorized, errResp("SESSION_TOKEN_REQUIRED", "the session token from the QR is required"))
			return
		}
		if !p.valid(r.PathValue("id"), token) {
			writeJSON(w, http.StatusUnauthorized, errResp("INVALID_SESSION_TOKEN", "invalid session token"))
			return
		}
		next(w, r)
	}
}
