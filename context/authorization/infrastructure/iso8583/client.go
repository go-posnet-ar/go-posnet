package iso8583

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	moov "github.com/moov-io/iso8583"
	"go.opentelemetry.io/otel/metric"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/pkg/observability"
)

const (
	dialTimeout  = 3 * time.Second
	writeTimeout = 5 * time.Second
	echoTimeout  = 5 * time.Second
	maxBackoff   = 30 * time.Second
)

var errConnectionLost = errors.New("iso8583: connection lost while waiting for the response")

// Config configura la conexión con el host.
type Config struct {
	Address    string
	AcquirerID string
	EchoEvery  time.Duration
}

// Client implementa service.AcquirerGateway. Usa una sola conexión TCP multiplexada: las
// respuestas se correlacionan por tipo de mensaje y RRN, sin esperar turno.
type Client struct {
	cfg   Config
	cards service.CardDataProvider
	log   *slog.Logger

	mu      sync.Mutex
	conn    net.Conn
	pending map[string][]chan *moov.Message

	writeMu   sync.Mutex
	connected atomic.Bool
	echoSTAN  atomic.Int64
}

var _ service.AcquirerGateway = (*Client)(nil)

// NewClient crea el cliente. La conexión empieza con Run.
func NewClient(cfg Config, cards service.CardDataProvider) *Client {
	return &Client{
		cfg:     cfg,
		cards:   cards,
		log:     slog.Default().With(slog.String("component", "iso8583.client")),
		pending: map[string][]chan *moov.Message{},
	}
}

// Connected indica si hay conexión con el host.
func (c *Client) Connected() bool { return c.connected.Load() }

// RegisterConnectedGauge publica el estado de la conexión.
func (c *Client) RegisterConnectedGauge() error {
	meter := observability.Meter("posnet.authorization")
	gauge, err := meter.Int64ObservableGauge("posnet_acquirer_connected",
		metric.WithDescription("Conexión con el host adquirente: 1 conectado, 0 caído."))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var value int64
		if c.Connected() {
			value = 1
		}
		o.ObserveInt64(gauge, value)
		return nil
	}, gauge)
	return err
}

// Run mantiene la conexión hasta que termine ctx, reconectando con una espera creciente.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		conn, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", c.cfg.Address)
		if err != nil {
			c.log.Warn("acquirer host unreachable", slog.String("address", c.cfg.Address), slog.String("error", err.Error()), slog.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = time.Second
		c.serve(ctx, conn)
	}
}

// serve usa la conexión hasta que falle.
func (c *Client) serve(ctx context.Context, conn net.Conn) {
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	c.connected.Store(true)
	c.log.Info("acquirer host connected", slog.String("address", c.cfg.Address))

	serveCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Con ctx cancelado, cerrar la conexión destraba la lectura.
		<-serveCtx.Done()
		_ = conn.Close()
	}()
	go func() {
		defer wg.Done()
		c.echoLoop(serveCtx, conn)
	}()

	c.readLoop(conn)
	stop()
	wg.Wait()

	c.connected.Store(false)
	c.mu.Lock()
	c.conn = nil
	lost := c.pending
	c.pending = map[string][]chan *moov.Message{}
	c.mu.Unlock()
	for _, waiters := range lost {
		for _, ch := range waiters {
			close(ch)
		}
	}
	if ctx.Err() == nil {
		c.log.Warn("acquirer host disconnected", slog.String("address", c.cfg.Address))
	}
}

func (c *Client) readLoop(conn net.Conn) {
	for {
		packed, err := ReadFrame(conn)
		if err != nil {
			return
		}
		msg := moov.NewMessage(Spec)
		// Los errores no incluyen el mensaje: lleva el PAN.
		if err := msg.Unpack(packed); err != nil {
			c.log.Error("acquirer sent an unreadable message — closing the connection")
			return
		}
		key, err := responseKey(msg)
		if err != nil {
			c.log.Error("acquirer sent a message without correlation data — ignoring it")
			continue
		}
		c.deliver(key, msg)
	}
}

func responseKey(msg *moov.Message) (string, error) {
	mti, err := msg.GetMTI()
	if err != nil {
		return "", err
	}
	id := DERRN
	if mti == MTINetworkResponse {
		id = DESTAN
	}
	value, err := msg.GetString(id)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.New("empty correlation field")
	}
	return mti + ":" + value, nil
}

func (c *Client) deliver(key string, msg *moov.Message) {
	c.mu.Lock()
	waiters := c.pending[key]
	if len(waiters) == 0 {
		c.mu.Unlock()
		// Llegó después del timeout: la transacción ya la tomó el conciliador.
		c.log.Warn("late acquirer response ignored", slog.String("key", key))
		return
	}
	ch := waiters[0]
	if len(waiters) == 1 {
		delete(c.pending, key)
	} else {
		c.pending[key] = waiters[1:]
	}
	c.mu.Unlock()
	ch <- msg
}

func (c *Client) echoLoop(ctx context.Context, conn net.Conn) {
	if c.cfg.EchoEvery <= 0 {
		return
	}
	ticker := time.NewTicker(c.cfg.EchoEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := c.echo(ctx, now); err != nil && ctx.Err() == nil {
				c.log.Warn("acquirer echo failed — reconnecting", slog.String("error", err.Error()))
				_ = conn.Close()
				return
			}
		}
	}
}

func (c *Client) echo(ctx context.Context, now time.Time) error {
	stan := int(c.echoSTAN.Add(1))
	msg, err := BuildEcho(stan, now)
	if err != nil {
		return err
	}
	echoCtx, cancel := context.WithTimeout(ctx, echoTimeout)
	defer cancel()
	_, err = c.roundTrip(echoCtx, msg, MTINetworkResponse+":"+fmt.Sprintf("%06d", stan%1000000))
	return err
}

// roundTrip envía el mensaje y espera su respuesta. Si no salió ningún byte, el error incluye
// service.ErrNotSent: el host seguro no lo recibió.
func (c *Client) roundTrip(ctx context.Context, msg *moov.Message, key string) (*moov.Message, error) {
	packed, err := msg.Pack()
	if err != nil {
		// No incluye err: la librería puede citar valores de los campos.
		return nil, fmt.Errorf("%w: iso8583: message cannot be packed", service.ErrNotSent)
	}

	ch := make(chan *moov.Message, 1)
	c.mu.Lock()
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: iso8583: no connection to the host", service.ErrNotSent)
	}
	c.pending[key] = append(c.pending[key], ch)
	c.mu.Unlock()

	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	written, err := WriteFrame(conn, packed)
	c.writeMu.Unlock()
	if err != nil {
		c.forget(key, ch)
		if written == 0 {
			return nil, fmt.Errorf("%w: iso8583: write: %w", service.ErrNotSent, err)
		}
		// Salió una parte: el host no puede interpretarla, pero la conexión quedó inservible.
		_ = conn.Close()
		return nil, fmt.Errorf("iso8583: partial write: %w", err)
	}

	select {
	case response, ok := <-ch:
		if !ok {
			return nil, errConnectionLost
		}
		return response, nil
	case <-ctx.Done():
		c.forget(key, ch)
		return nil, fmt.Errorf("iso8583: no response: %w", ctx.Err())
	}
}

func (c *Client) forget(key string, ch chan *moov.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	waiters := c.pending[key]
	for i, w := range waiters {
		if w == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(c.pending, key)
	} else {
		c.pending[key] = waiters
	}
}

// Authorize envía un 0100 y espera el 0110.
func (c *Client) Authorize(ctx context.Context, tx *aggregate.Transaction) (service.AcquirerResponse, error) {
	card, err := c.cards.Resolve(ctx, tx.CardToken())
	if err != nil {
		return service.AcquirerResponse{}, fmt.Errorf("%w: resolve card: %w", service.ErrNotSent, err)
	}
	msg, err := BuildAuthorization(tx, card, c.cfg.AcquirerID)
	if err != nil {
		return service.AcquirerResponse{}, fmt.Errorf("%w: %w", service.ErrNotSent, err)
	}
	response, err := c.roundTrip(ctx, msg, MTIAuthResponse+":"+RRN(tx.ID()))
	if err != nil {
		return service.AcquirerResponse{}, err
	}
	code, err := response.GetString(DEResponseCode)
	if err != nil || code == "" {
		return service.AcquirerResponse{}, errors.New("iso8583: response without DE-39")
	}
	authCode, _ := response.GetString(DEAuthCode)
	return service.AcquirerResponse{ResponseCode: code, AuthCode: authCode, RespondedAt: time.Now().UTC()}, nil
}

// Reverse envía un 0400 y espera el 0410. Si el host no conoce la operación, no hay nada que
// revertir, y es un éxito: pudo no haber llegado nunca.
func (c *Client) Reverse(ctx context.Context, tx *aggregate.Transaction) error {
	card, err := c.cards.Resolve(ctx, tx.CardToken())
	if err != nil {
		return fmt.Errorf("%w: resolve card: %w", service.ErrNotSent, err)
	}
	msg, err := BuildReversal(tx, card, c.cfg.AcquirerID)
	if err != nil {
		return fmt.Errorf("%w: %w", service.ErrNotSent, err)
	}
	response, err := c.roundTrip(ctx, msg, MTIReversalResponse+":"+RRN(tx.ID()))
	if err != nil {
		return err
	}
	code, err := response.GetString(DEResponseCode)
	if err != nil {
		return errors.New("iso8583: response without DE-39")
	}
	switch code {
	case ResponseApproved, ResponseOriginalNotFound:
		return nil
	default:
		return fmt.Errorf("iso8583: reversal declined with code %s", code)
	}
}
