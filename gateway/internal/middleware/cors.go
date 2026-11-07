package middleware

import "net/http"

// CORS deja que un navegador llame a la API desde los orígenes permitidos. "*" permite
// cualquiera. El gateway es el único que responde CORS: el proxy descarta los encabezados de
// los servicios, para que el navegador no reciba dos políticas.
func CORS(allowedOrigins []string, next http.Handler) http.Handler {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	anyOrigin := allowed["*"]

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		ok := origin != "" && (anyOrigin || allowed[origin])
		if ok {
			if anyOrigin {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Terminal-ID, X-Session-Token")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		// Un preflight no llega a los servicios: no tienen rutas OPTIONS.
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// SecurityHeaders agrega encabezados que no dependen del servicio.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

// MaxBody limita el cuerpo de cada pedido: más allá, el servicio recibe un error de lectura.
func MaxBody(limit int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > limit {
			WriteError(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "request body too large")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}
