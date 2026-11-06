package http

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/juantevez/go-posnet/pkg/observability"
)

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
