package iso8583_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	moov "github.com/moov-io/iso8583"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/carddata"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/iso8583"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/iso8583/hostsim"
	"github.com/juantevez/go-posnet/pkg/domain"
)

const testPAN = "4242424242424242"

func newTx(t *testing.T, cents int64) *aggregate.Transaction {
	t.Helper()
	money, err := domain.NewMoney(cents, domain.ARS)
	if err != nil {
		t.Fatalf("NewMoney() error = %v", err)
	}
	stan, _ := domain.NewSTAN(4711)
	pan, _ := domain.NewPAN("4242", domain.NetworkVisa)
	tx, err := aggregate.NewTransaction(domain.NewTransactionID(), domain.NewTerminalID(), domain.NewMerchantID(),
		money, stan, pan, valueobject.EntryModeChip, domain.CardToken{}, "", nil)
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}
	return tx
}

type fixture struct {
	host   *hostsim.Host
	client *iso8583.Client
	addr   string
}

func startClient(t *testing.T, addr string, cards service.CardDataProvider, echo time.Duration) (*iso8583.Client, func()) {
	t.Helper()
	client := iso8583.NewClient(iso8583.Config{Address: addr, AcquirerID: "000001", EchoEvery: echo}, cards)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		client.Run(ctx)
		close(done)
	}()
	return client, func() {
		cancel()
		<-done
	}
}

func startHost(t *testing.T, addr string) (*hostsim.Host, string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	host := hostsim.New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = host.Serve(ctx, ln)
		close(done)
	}()
	return host, ln.Addr().String(), func() {
		cancel()
		<-done
	}
}

func setup(t *testing.T) *fixture {
	t.Helper()
	host, addr, stopHost := startHost(t, "127.0.0.1:0")
	client, stopClient := startClient(t, addr, carddata.NewStub(), 0)
	t.Cleanup(func() {
		stopClient()
		stopHost()
	})
	waitConnected(t, client, true)
	return &fixture{host: host, client: client, addr: addr}
}

func waitConnected(t *testing.T, c *iso8583.Client, want bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for c.Connected() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Connected() never became %v", want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func callCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestAuthorize_Responses(t *testing.T) {
	f := setup(t)
	cases := []struct {
		name     string
		cents    int64
		code     string
		hasAuth  bool
		approved bool
	}{
		{"aprobada", 12345, "00", true, true},
		{"fondos insuficientes", hostsim.AmountDecline, "51", false, false},
		{"tarjeta perdida", hostsim.AmountCaptureCard, "41", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := f.client.Authorize(callCtx(t), newTx(t, tc.cents))
			if err != nil {
				t.Fatalf("Authorize() error = %v", err)
			}
			if resp.ResponseCode != tc.code || (resp.AuthCode != "") != tc.hasAuth || resp.IsApproved() != tc.approved {
				t.Errorf("response = %+v", resp)
			}
			if tc.hasAuth {
				if _, err := domain.NewAuthCode(resp.AuthCode); err != nil {
					t.Errorf("auth code %q is not valid for the domain: %v", resp.AuthCode, err)
				}
			}
		})
	}
}

// Sin respuesta, vence el contexto: no se sabe si llegó, así que NO es ErrNotSent.
func TestAuthorize_Timeout(t *testing.T) {
	f := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := f.client.Authorize(ctx, newTx(t, hostsim.AmountHang))
	if err == nil || errors.Is(err, service.ErrNotSent) {
		t.Fatalf("error = %v, want an ambiguous error", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want DeadlineExceeded", err)
	}
	if f.host.Seen(iso8583.MTIAuthRequest) != 1 {
		t.Errorf("0100 received = %d, want 1", f.host.Seen(iso8583.MTIAuthRequest))
	}
}

// La conexión se cae esperando: el mensaje salió, así que NO es ErrNotSent. Después reconecta.
func TestAuthorize_ConnectionLostIsAmbiguousAndReconnects(t *testing.T) {
	f := setup(t)

	_, err := f.client.Authorize(callCtx(t), newTx(t, hostsim.AmountError))
	if err == nil || errors.Is(err, service.ErrNotSent) {
		t.Fatalf("error = %v, want an ambiguous error", err)
	}

	waitConnected(t, f.client, true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := f.client.Authorize(callCtx(t), newTx(t, 100))
		if err == nil && resp.IsApproved() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("did not recover: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Sin conexión, seguro no salió: no hay nada que revertir.
func TestAuthorize_NoConnectionIsNotSent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	client, stop := startClient(t, addr, carddata.NewStub(), 0)
	defer stop()

	_, err = client.Authorize(callCtx(t), newTx(t, 100))
	if !errors.Is(err, service.ErrNotSent) {
		t.Fatalf("error = %v, want ErrNotSent", err)
	}
}

// Conecta cuando el host aparece, sin reiniciar el servicio.
func TestClient_ConnectsWhenTheHostComesUp(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	client, stop := startClient(t, addr, carddata.NewStub(), 0)
	defer stop()
	time.Sleep(50 * time.Millisecond)
	if client.Connected() {
		t.Fatal("connected without a host")
	}

	_, _, stopHost := startHost(t, addr)
	defer stopHost()
	waitConnected(t, client, true)
}

type failingCards struct{ err error }

func (f failingCards) Resolve(context.Context, domain.CardToken) (service.CardData, error) {
	return service.CardData{}, f.err
}

// Sin datos de la tarjeta, no se envía nada.
func TestAuthorize_CardDataError(t *testing.T) {
	host, addr, stopHost := startHost(t, "127.0.0.1:0")
	defer stopHost()
	client, stop := startClient(t, addr, failingCards{service.ErrCardDataNotFound}, 0)
	defer stop()
	waitConnected(t, client, true)

	_, err := client.Authorize(callCtx(t), newTx(t, 100))
	if !errors.Is(err, service.ErrNotSent) || !errors.Is(err, service.ErrCardDataNotFound) {
		t.Fatalf("error = %v, want ErrNotSent and ErrCardDataNotFound", err)
	}
	if host.Seen(iso8583.MTIAuthRequest) != 0 {
		t.Error("the message should not have been sent")
	}
}

type fixedCard struct{ card service.CardData }

func (f fixedCard) Resolve(context.Context, domain.CardToken) (service.CardData, error) {
	return f.card, nil
}

func TestAuthorize_HostValidatesTheCard(t *testing.T) {
	host, addr, stopHost := startHost(t, "127.0.0.1:0")
	defer stopHost()
	cases := []struct {
		name string
		card service.CardData
		code string
	}{
		{"fuera del rango de prueba", service.CardData{PAN: "4111111111111111", ExpiryYYMM: "3012"}, "14"},
		{"Luhn inválido", service.CardData{PAN: "4242424242424241", ExpiryYYMM: "3012"}, "14"},
		{"vencida", service.CardData{PAN: testPAN, ExpiryYYMM: "2001"}, "54"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, stop := startClient(t, addr, fixedCard{tc.card}, 0)
			defer stop()
			waitConnected(t, client, true)
			resp, err := client.Authorize(callCtx(t), newTx(t, 100))
			if err != nil || resp.ResponseCode != tc.code {
				t.Errorf("response = %+v, %v, want %s", resp, err, tc.code)
			}
		})
	}
	_ = host
}

func TestReverse(t *testing.T) {
	f := setup(t)

	t.Run("una aprobada", func(t *testing.T) {
		tx := newTx(t, 100)
		if _, err := f.client.Authorize(callCtx(t), tx); err != nil {
			t.Fatal(err)
		}
		if err := f.client.Reverse(callCtx(t), tx); err != nil {
			t.Fatalf("Reverse() error = %v", err)
		}
		if !f.host.Reversed(iso8583.RRN(tx.ID())) {
			t.Error("the host did not reverse it")
		}
		// Repetirla no tiene efecto: la usa el conciliador.
		if err := f.client.Reverse(callCtx(t), tx); err != nil {
			t.Errorf("repeated Reverse() error = %v", err)
		}
	})

	t.Run("una que el host no conoce es un éxito", func(t *testing.T) {
		if err := f.client.Reverse(callCtx(t), newTx(t, 100)); err != nil {
			t.Errorf("Reverse() error = %v", err)
		}
	})

	t.Run("rechazada por el host se reintenta", func(t *testing.T) {
		tx := newTx(t, hostsim.AmountReverseFails)
		if _, err := f.client.Authorize(callCtx(t), tx); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			if err := f.client.Reverse(callCtx(t), tx); err == nil {
				t.Errorf("attempt %d should fail", i)
			}
		}
		if err := f.client.Reverse(callCtx(t), tx); err != nil {
			t.Errorf("third Reverse() error = %v", err)
		}
	})
}

// Muchas llamadas por una conexión: cada una recibe su respuesta, aunque el host las mezcle.
func TestConcurrentCallsAreCorrelated(t *testing.T) {
	f := setup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cents, want := int64(100+i), "00"
			if i%2 == 0 {
				cents, want = hostsim.AmountDecline, "51"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := f.client.Authorize(ctx, newTx(t, cents))
			if err != nil {
				errs <- err
				return
			}
			if resp.ResponseCode != want {
				errs <- fmt.Errorf("call %d = %s, want %s", i, resp.ResponseCode, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// Un host que deja de responder se detecta por el eco, sin esperar una venta.
func TestEcho(t *testing.T) {
	t.Run("mantiene la conexión", func(t *testing.T) {
		host, addr, stopHost := startHost(t, "127.0.0.1:0")
		defer stopHost()
		client, stop := startClient(t, addr, carddata.NewStub(), 20*time.Millisecond)
		defer stop()
		waitConnected(t, client, true)
		deadline := time.Now().Add(3 * time.Second)
		for host.Seen(iso8583.MTINetworkRequest) < 3 {
			if time.Now().After(deadline) {
				t.Fatal("no echoes received")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !client.Connected() {
			t.Error("the echo should keep it connected")
		}
	})

	t.Run("una caída del host se detecta", func(t *testing.T) {
		_, addr, stopHost := startHost(t, "127.0.0.1:0")
		client, stop := startClient(t, addr, carddata.NewStub(), 20*time.Millisecond)
		defer stop()
		waitConnected(t, client, true)
		stopHost()
		waitConnected(t, client, false)
	})
}

func TestBuildAuthorization_Fields(t *testing.T) {
	tx := newTx(t, 150050)
	msg, err := iso8583.BuildAuthorization(tx, service.CardData{PAN: testPAN, ExpiryYYMM: "3012"}, "000001")
	if err != nil {
		t.Fatalf("BuildAuthorization() error = %v", err)
	}
	packed, err := msg.Pack()
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	// Los campos numéricos viajan con ceros a la izquierda, que la librería quita al leer.
	if !strings.Contains(string(packed), "000000"+"000000150050") {
		t.Errorf("DE-3 and DE-4 are not on the wire with their fixed length: %q", packed)
	}
	got := moov.NewMessage(iso8583.Spec)
	if err := got.Unpack(packed); err != nil {
		t.Fatalf("Unpack() error = %v", err)
	}
	if mti, _ := got.GetMTI(); mti != "0100" {
		t.Errorf("MTI = %s", mti)
	}
	for id, want := range map[int]string{
		iso8583.DEPAN:  testPAN,
		iso8583.DESTAN: "004711", iso8583.DEExpiry: "3012", iso8583.DEEntryMode: "051",
		iso8583.DEAcquirerID: "000001", iso8583.DERRN: iso8583.RRN(tx.ID()), iso8583.DECurrency: "032",
	} {
		if value, _ := got.GetString(id); value != want {
			t.Errorf("DE-%d = %q, want %q", id, value, want)
		}
	}
}

func TestBuildReversal_CarriesOriginalData(t *testing.T) {
	tx := newTx(t, 100)
	msg, err := iso8583.BuildReversal(tx, service.CardData{PAN: testPAN, ExpiryYYMM: "3012"}, "000001")
	if err != nil {
		t.Fatalf("BuildReversal() error = %v", err)
	}
	packed, err := msg.Pack()
	if err != nil {
		t.Fatalf("Pack() error = %v", err)
	}
	got := moov.NewMessage(iso8583.Spec)
	if err := got.Unpack(packed); err != nil {
		t.Fatalf("Unpack() error = %v", err)
	}
	if mti, _ := got.GetMTI(); mti != "0400" {
		t.Errorf("MTI = %s", mti)
	}
	original, _ := got.GetString(iso8583.DEOriginalData)
	transmission := tx.ReceivedAt().UTC().Format("0102150405")
	if len(original) != 42 || !strings.HasPrefix(original, "0100004711"+transmission) {
		t.Errorf("DE-90 = %q", original)
	}
	if rrn, _ := got.GetString(iso8583.DERRN); rrn != iso8583.RRN(tx.ID()) {
		t.Errorf("RRN = %q, want the same as the authorization", rrn)
	}
}

func TestBuildAuthorization_InvalidCurrencyMapping(t *testing.T) {
	// Toda moneda del dominio tiene su código numérico: si se agrega una, este test lo exige.
	for _, c := range []domain.Currency{domain.ARS, domain.USD, domain.EUR, domain.BRL, domain.UYU, domain.CLP} {
		money, err := domain.NewMoney(100, c)
		if err != nil {
			t.Fatal(err)
		}
		stan, _ := domain.NewSTAN(1)
		pan, _ := domain.NewPAN("4242", domain.NetworkVisa)
		tx, err := aggregate.NewTransaction(domain.NewTransactionID(), domain.NewTerminalID(), domain.NewMerchantID(), money, stan, pan, valueobject.EntryModeManual, domain.CardToken{}, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := iso8583.BuildAuthorization(tx, service.CardData{PAN: testPAN, ExpiryYYMM: "3012"}, "1"); err != nil {
			t.Errorf("%s: %v", c, err)
		}
	}
}

// El PAN nunca aparece en los logs, ni siquiera con el host rechazando o cayéndose.
func TestPANNeverReachesTheLogs(t *testing.T) {
	var logs safeBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)

	f := setup(t)
	for _, cents := range []int64{100, hostsim.AmountDecline, hostsim.AmountError} {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		tx := newTx(t, cents)
		_, _ = f.client.Authorize(ctx, tx)
		_ = f.client.Reverse(ctx, tx)
		cancel()
	}
	if out := logs.String(); strings.Contains(out, testPAN) {
		t.Errorf("the logs expose the PAN:\n%s", out)
	}
}

type safeBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
