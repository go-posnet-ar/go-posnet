// Package hostsim simula el host del emisor por ISO 8583, hasta que exista el real. Solo sirve
// para desarrollo: acepta solo tarjetas de prueba y guarda lo que autorizó en memoria.
package hostsim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	moov "github.com/moov-io/iso8583"

	"github.com/juantevez/go-posnet/context/authorization/infrastructure/iso8583"
)

// Montos con un comportamiento especial, en centavos. Son los mismos del mock de Authorization.
const (
	AmountDecline      = 770051 // Responde 51, fondos insuficientes
	AmountCaptureCard  = 770041 // Responde 41, tarjeta perdida
	AmountError        = 770091 // Cierra la conexión sin responder
	AmountHang         = 770098 // No responde
	AmountReverseFails = 770092 // Aprueba, y responde 96 a las dos primeras reversas
)

const (
	codeInvalidCard     = "14"
	codeSystemFailure   = "96"
	codeExpiredCard     = "54"
	codeLostCard        = "41"
	codeNoFunds         = "51"
	reverseFailuresWant = 2
)

// testBINs son los prefijos que acepta. Rechazar el resto evita que una tarjeta real viaje por acá.
var testBINs = []string{"424242", "555555", "400005"}

// Host atiende conexiones. Es seguro para uso concurrente.
type Host struct {
	log *slog.Logger
	now func() time.Time

	mu              sync.Mutex
	approved        map[string]bool
	reversed        map[string]bool
	reverseAttempts map[string]int
	authorizations  map[string]int
	seen            map[string]int
}

// New crea un host sin operaciones.
func New() *Host {
	return &Host{
		log:             slog.Default().With(slog.String("component", "hostsim")),
		now:             time.Now,
		approved:        map[string]bool{},
		reversed:        map[string]bool{},
		reverseAttempts: map[string]int{},
		authorizations:  map[string]int{},
		seen:            map[string]int{},
	}
}

// Seen devuelve cuántos mensajes de un tipo recibió.
func (h *Host) Seen(mti string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[mti]
}

// Reversed indica si revirtió una operación aprobada.
func (h *Host) Reversed(rrn string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.reversed[rrn]
}

// Authorizations devuelve cuántos 0100 recibió para un RRN.
func (h *Host) Authorizations(rrn string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.authorizations[rrn]
}

// Serve atiende hasta que termine ctx o se cierre el listener.
func (h *Host) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.handle(ctx, conn)
		}()
	}
}

func (h *Host) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	for {
		packed, err := iso8583.ReadFrame(conn)
		if err != nil {
			return
		}
		request := moov.NewMessage(iso8583.Spec)
		if err := request.Unpack(packed); err != nil {
			h.log.Error("unreadable message — closing the connection")
			return
		}
		response, closeConn := h.respond(request)
		if closeConn {
			return
		}
		if response == nil {
			continue
		}
		out, err := response.Pack()
		if err != nil {
			h.log.Error("response cannot be packed")
			return
		}
		if _, err := iso8583.WriteFrame(conn, out); err != nil {
			return
		}
	}
}

// respond devuelve la respuesta. Sin respuesta, el cliente vence por timeout; con closeConn, se
// cae la conexión.
func (h *Host) respond(request *moov.Message) (response *moov.Message, closeConn bool) {
	mti, err := request.GetMTI()
	if err != nil {
		return nil, true
	}
	h.mu.Lock()
	h.seen[mti]++
	h.mu.Unlock()

	switch mti {
	case iso8583.MTINetworkRequest:
		return reply(request, iso8583.MTINetworkResponse, iso8583.ResponseApproved, "", iso8583.DETransmission, iso8583.DESTAN, iso8583.DENetworkCode), false
	case iso8583.MTIAuthRequest:
		return h.authorize(request)
	case iso8583.MTIReversalRequest:
		return h.reverse(request), false
	}
	h.log.Warn("unknown message type", slog.String("mti", mti))
	return nil, false
}

var echoedFinancialFields = []int{
	iso8583.DEProcessingCode, iso8583.DEAmount, iso8583.DETransmission, iso8583.DESTAN,
	iso8583.DERRN, iso8583.DETerminalID, iso8583.DEMerchantID, iso8583.DECurrency,
}

func (h *Host) authorize(request *moov.Message) (*moov.Message, bool) {
	rrn, _ := request.GetString(iso8583.DERRN)
	amount, err := iso8583.Amount(request)
	if err != nil {
		return reply(request, iso8583.MTIAuthResponse, "30", "", echoedFinancialFields...), false
	}
	h.mu.Lock()
	h.authorizations[rrn]++
	h.mu.Unlock()

	switch amount {
	case AmountError:
		return nil, true
	case AmountHang:
		return nil, false
	}

	code := h.decide(request, amount)
	authCode := ""
	if code == iso8583.ResponseApproved {
		authCode = authCodeFor(rrn)
		h.mu.Lock()
		h.approved[rrn] = true
		h.mu.Unlock()
	}
	return reply(request, iso8583.MTIAuthResponse, code, authCode, echoedFinancialFields...), false
}

func (h *Host) decide(request *moov.Message, amount int64) string {
	pan, _ := request.GetString(iso8583.DEPAN)
	if !isTestCard(pan) {
		return codeInvalidCard
	}
	expiry, _ := request.GetString(iso8583.DEExpiry)
	if len(expiry) != 4 || expiry < h.now().UTC().Format("0601") {
		return codeExpiredCard
	}
	switch amount {
	case AmountDecline:
		return codeNoFunds
	case AmountCaptureCard:
		return codeLostCard
	}
	return iso8583.ResponseApproved
}

func (h *Host) reverse(request *moov.Message) *moov.Message {
	rrn, _ := request.GetString(iso8583.DERRN)
	amount, _ := iso8583.Amount(request)

	h.mu.Lock()
	defer h.mu.Unlock()
	if amount == AmountReverseFails && h.reverseAttempts[rrn] < reverseFailuresWant {
		h.reverseAttempts[rrn]++
		return reply(request, iso8583.MTIReversalResponse, codeSystemFailure, "", echoedFinancialFields...)
	}
	if !h.approved[rrn] && !h.reversed[rrn] {
		return reply(request, iso8583.MTIReversalResponse, iso8583.ResponseOriginalNotFound, "", echoedFinancialFields...)
	}
	h.approved[rrn] = false
	h.reversed[rrn] = true
	return reply(request, iso8583.MTIReversalResponse, iso8583.ResponseApproved, "", echoedFinancialFields...)
}

// reply copia los campos indicados. Nunca devuelve el PAN: la correlación usa DE-37.
func reply(request *moov.Message, mti, code, authCode string, echo ...int) *moov.Message {
	response := moov.NewMessage(iso8583.Spec)
	response.MTI(mti)
	for _, id := range echo {
		if value, err := request.GetString(id); err == nil && value != "" {
			_ = response.Field(id, value)
		}
	}
	_ = response.Field(iso8583.DEResponseCode, code)
	if authCode != "" {
		_ = response.Field(iso8583.DEAuthCode, authCode)
	}
	return response
}

func authCodeFor(rrn string) string {
	sum := sha256.Sum256([]byte(rrn))
	return strings.ToUpper(hex.EncodeToString(sum[:]))[:6]
}

func isTestCard(pan string) bool {
	for _, bin := range testBINs {
		if strings.HasPrefix(pan, bin) {
			return luhn(pan)
		}
	}
	return false
}

func luhn(pan string) bool {
	if len(pan) < 12 {
		return false
	}
	sum, double := 0, false
	for i := len(pan) - 1; i >= 0; i-- {
		d := int(pan[i] - '0')
		if d < 0 || d > 9 {
			return false
		}
		if double {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
