package http

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/juantevez/go-posnet/pkg/observability"
)

// adminAuthMiddleware exige un token en todo lo que no sea una lectura. Aplica por
// método y no por ruta, para que una operación nueva quede protegida por defecto. Sin
// token configurado, las escrituras quedan deshabilitadas, no abiertas.
func adminAuthMiddleware(apiKey string, next http.Handler) http.Handler {
	expected := sha256.Sum256([]byte(apiKey))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		if apiKey == "" {
			writeJSON(w, http.StatusServiceUnavailable,
				errResp("ADMIN_API_DISABLED", "SETTLEMENT_ADMIN_API_KEY is not configured"))
			return
		}

		// Se comparan hashes para no revelar la longitud del token en el tiempo de respuesta.
		token, hasBearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		got := sha256.Sum256([]byte(token))
		if !hasBearer || subtle.ConstantTimeCompare(got[:], expected[:]) != 1 {
			observability.FromContext(r.Context()).Warn("rejected admin request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("remote_addr", r.RemoteAddr),
			)
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, errResp("UNAUTHORIZED", "missing or invalid admin token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				stack := debug.Stack()
				observability.FromContext(r.Context()).Error("panic recovered",
					slog.Any("panic", rec),
					slog.String("stack", string(stack)),
					slog.String("path", r.URL.Path),
				)
				observability.RecordError(r.Context(), fmt.Errorf("panic: %v", rec))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"error": "INTERNAL_ERROR", "message": "unexpected error",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
