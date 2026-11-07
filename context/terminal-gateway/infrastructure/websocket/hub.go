package websocket

import (
	"context"
	"sync"
	"time"

	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// sendBuffer son los mensajes que puede tener pendientes una conexión. Si se llena, el terminal
// no está leyendo, y se lo desconecta en lugar de frenar a los demás.
const sendBuffer = 32

// Hub registra las conexiones abiertas de cada terminal y les envía los mensajes. Un terminal
// puede tener varias, por ejemplo dos pestañas del cajero: todas reciben lo mismo.
//
// Vive en memoria, por instancia: con varias instancias, un terminal solo recibe lo que procesa la
// instancia a la que está conectado.
type Hub struct {
	mu    sync.RWMutex
	conns map[domain.TerminalID]map[*client]struct{}
}

// client es una conexión abierta.
type client struct {
	terminalID  domain.TerminalID
	connectedAt time.Time
	send        chan []byte
	// drop corta la conexión; lo llama el hub si el terminal no lee sus mensajes.
	drop func()
}

var (
	_ service.TerminalNotifier    = (*Hub)(nil)
	_ service.TerminalConnections = (*Hub)(nil)
)

// NewHub crea el registro vacío.
func NewHub() *Hub {
	return &Hub{conns: map[domain.TerminalID]map[*client]struct{}{}}
}

func (h *Hub) register(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns[c.terminalID] == nil {
		h.conns[c.terminalID] = map[*client]struct{}{}
	}
	h.conns[c.terminalID][c] = struct{}{}
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns[c.terminalID], c)
	if len(h.conns[c.terminalID]) == 0 {
		delete(h.conns, c.terminalID)
	}
}

// broadcast encola msg en cada conexión del terminal. Devuelve ErrTerminalNotConnected si no
// tiene ninguna. Encolar no garantiza que el terminal lo reciba: puede cortarse antes.
func (h *Hub) broadcast(terminalID domain.TerminalID, msg []byte) error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.conns[terminalID]) == 0 {
		return service.ErrTerminalNotConnected
	}
	for c := range h.conns[terminalID] {
		select {
		case c.send <- msg:
		default:
			c.drop()
		}
	}
	return nil
}

// NotifySessionCreated implementa service.TerminalNotifier.
func (h *Hub) NotifySessionCreated(_ context.Context, s *aggregate.PaymentSession) error {
	return h.broadcast(s.TerminalID(), encode(sessionCreatedMessage{
		Type:          TypeSessionCreated,
		TransactionID: s.ID().String(),
		AmountCents:   s.Amount().Cents(),
		Currency:      s.Amount().Currency().String(),
		Channel:       s.Channel().String(),
		ExpiresAt:     s.ExpiresAt().UTC().Format(time.RFC3339),
	}))
}

// NotifyResult implementa service.TerminalNotifier.
func (h *Hub) NotifyResult(_ context.Context, s *aggregate.PaymentSession) error {
	return h.broadcast(s.TerminalID(), encode(newSessionResult(s)))
}

// NotifySessionExpired implementa service.TerminalNotifier.
func (h *Hub) NotifySessionExpired(_ context.Context, terminalID domain.TerminalID, txID domain.TransactionID) error {
	return h.broadcast(terminalID, encode(sessionExpiredMessage{Type: TypeSessionExpired, TransactionID: txID.String()}))
}

// SendReceipt implementa service.TerminalConnections.
func (h *Hub) SendReceipt(_ context.Context, terminalID domain.TerminalID, r service.Receipt) error {
	return h.broadcast(terminalID, encode(newReceipt(r)))
}

// ConnectedSince implementa service.TerminalConnections: la conexión más antigua que sigue abierta.
func (h *Hub) ConnectedSince(terminalID domain.TerminalID) (time.Time, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var since time.Time
	for c := range h.conns[terminalID] {
		if since.IsZero() || c.connectedAt.Before(since) {
			since = c.connectedAt
		}
	}
	return since, !since.IsZero()
}

// Connections devuelve cuántas conexiones abiertas hay.
func (h *Hub) Connections() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	for _, cs := range h.conns {
		n += len(cs)
	}
	return n
}
