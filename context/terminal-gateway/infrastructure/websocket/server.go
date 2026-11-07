package websocket

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/observability"
)

// Plazos de la conexión. Son variables para poder acortarlos en los tests.
var (
	authTimeout  = 10 * time.Second // Para enviar el mensaje auth
	writeTimeout = 5 * time.Second  // Para escribir un mensaje
	pingEvery    = 30 * time.Second // Un terminal que no responde el ping se desconecta
)

// maxReadBytes limita lo que envía el terminal: solo manda auth, así que algo más grande es un abuso.
const maxReadBytes = 4096

// Authenticator verifica el token de un terminal. Lo implementa la autenticación de la API HTTP,
// para que el límite de intentos sea uno solo. retryAfter es mayor que cero solo si se superó ese
// límite.
type Authenticator interface {
	Verify(r *http.Request, token string) (domain.TerminalID, time.Duration, error)
}

// Server acepta las conexiones de los terminales en /ws.
type Server struct {
	hub *Hub
	// auth es nil sin tokens configurados, solo en desarrollo: el terminal se identifica con su ID.
	auth           Authenticator
	originPatterns []string

	// ctx corta todas las conexiones al apagar: http.Server.Shutdown no cierra las que ya
	// pasaron a WebSocket.
	ctx    context.Context
	cancel context.CancelFunc
}

// NewServer crea el servidor. allowedOrigins son los orígenes de CORS: un navegador solo se
// conecta desde ellos.
func NewServer(hub *Hub, auth Authenticator, allowedOrigins []string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{hub: hub, auth: auth, originPatterns: originPatterns(allowedOrigins), ctx: ctx, cancel: cancel}
}

// Close corta todas las conexiones abiertas.
func (s *Server) Close() { s.cancel() }

// originPatterns convierte orígenes como http://192.168.0.2:5173 en el host que compara la
// librería. "*" permite cualquiera.
func originPatterns(origins []string) []string {
	var patterns []string
	for _, o := range origins {
		if o == "*" {
			return []string{"*"}
		}
		if u, err := url.Parse(o); err == nil && u.Host != "" {
			patterns = append(patterns, u.Host)
		}
	}
	return patterns
}

// Handler devuelve el handler HTTP de /ws.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handle)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","connections":` + strconv.Itoa(s.hub.Connections()) + `}`))
	})
	return mux
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.originPatterns})
	if err != nil {
		// Accept ya respondió el error, por ejemplo un origen no permitido.
		slog.WarnContext(r.Context(), "websocket upgrade rejected", slog.String("error", err.Error()))
		return
	}
	conn.SetReadLimit(maxReadBytes)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	terminalID, ok := s.authenticate(ctx, r, conn)
	if !ok {
		return
	}
	log := observability.FromContext(r.Context()).With(slog.String("terminal_id", terminalID.String()))

	var dropOnce sync.Once
	c := &client{
		terminalID:  terminalID,
		connectedAt: time.Now().UTC(),
		send:        make(chan []byte, sendBuffer),
		drop: func() {
			dropOnce.Do(func() {
				log.Warn("terminal is not reading its messages — disconnecting")
				cancel()
			})
		},
	}
	s.hub.register(c)
	defer s.hub.unregister(c)
	log.Info("terminal connected", slog.Int("connections", s.hub.Connections()))

	if err := write(ctx, conn, authOKMessage{Type: TypeAuthOK, TerminalID: terminalID.String()}); err != nil {
		return
	}

	// El terminal no envía nada más: leer solo detecta el cierre y responde los pings. Usa su
	// propio contexto: si se cancelara con ctx, la librería cortaría la conexión sin el cierre
	// ordenado.
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(pingEvery)
	defer ticker.Stop()
	for {
		select {
		case <-readerDone:
			log.Info("terminal disconnected")
			return
		case <-ctx.Done():
			// Apagado, o un terminal que no lee sus mensajes.
			_ = conn.Close(websocket.StatusGoingAway, "")
			log.Info("terminal disconnected by the server")
			return
		case msg := <-c.send:
			if err := writeRaw(ctx, conn, msg); err != nil {
				log.Info("terminal disconnected", slog.String("reason", err.Error()))
				return
			}
		case <-ticker.C:
			pingCtx, cancelPing := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pingCtx)
			cancelPing()
			if err != nil {
				log.Info("terminal did not answer the ping — disconnecting")
				_ = conn.Close(websocket.StatusPolicyViolation, "ping timeout")
				return
			}
		}
	}
}

// authenticate lee el primer mensaje y resuelve el terminal. Si falla, cierra la conexión.
func (s *Server) authenticate(ctx context.Context, r *http.Request, conn *websocket.Conn) (domain.TerminalID, bool) {
	fail := func(code, message string) (domain.TerminalID, bool) {
		_ = write(ctx, conn, errorMessage{Type: TypeError, Code: code, Message: message})
		_ = conn.Close(websocket.StatusPolicyViolation, code)
		slog.WarnContext(r.Context(), "websocket authentication failed", slog.String("code", code))
		return domain.TerminalID{}, false
	}

	// Un temporizador y no el contexto de Read: la librería cortaría la conexión sin decir por qué.
	timer := time.AfterFunc(authTimeout, func() {
		_ = conn.Close(websocket.StatusPolicyViolation, "auth timeout")
	})
	_, data, err := conn.Read(ctx)
	if !timer.Stop() || err != nil {
		_ = conn.CloseNow()
		return domain.TerminalID{}, false
	}
	var msg authMessage
	if err := json.Unmarshal(data, &msg); err != nil || msg.Type != TypeAuth {
		return fail("AUTH_REQUIRED", `the first message must be {"type":"auth"}`)
	}

	if s.auth == nil {
		id, err := domain.ParseTerminalID(msg.TerminalID)
		if err != nil {
			return fail("INVALID_TERMINAL_ID", "terminal_id must be a UUID")
		}
		return id, true
	}
	id, retryAfter, err := s.auth.Verify(r, msg.Token)
	switch {
	case err != nil && retryAfter > 0:
		return fail("TOO_MANY_ATTEMPTS", "too many failed attempts; try again later")
	case err != nil:
		return fail("UNAUTHORIZED", "missing or invalid terminal token")
	}
	return id, true
}

func write(ctx context.Context, conn *websocket.Conn, v any) error {
	return writeRaw(ctx, conn, encode(v))
}

func writeRaw(ctx context.Context, conn *websocket.Conn, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(ctx, websocket.MessageText, msg)
}
