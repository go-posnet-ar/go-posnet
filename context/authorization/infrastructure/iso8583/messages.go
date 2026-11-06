// Package iso8583 implementa el adquirente sobre ISO 8583:1987 en ASCII, por TCP.
//
// Los mensajes llevan el PAN: nunca se guardan ni se escriben en los logs. El campo iso8583_raw
// de la transacción no se usa para esto.
package iso8583

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	moov "github.com/moov-io/iso8583"
	"github.com/moov-io/iso8583/encoding"
	"github.com/moov-io/iso8583/field"
	"github.com/moov-io/iso8583/prefix"
	"github.com/moov-io/iso8583/specs"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

// Tipos de mensaje.
const (
	MTIAuthRequest      = "0100"
	MTIAuthResponse     = "0110"
	MTIReversalRequest  = "0400"
	MTIReversalResponse = "0410"
	MTINetworkRequest   = "0800"
	MTINetworkResponse  = "0810"
)

// Campos usados, por número de ISO 8583.
const (
	DEPAN              = 2
	DEProcessingCode   = 3
	DEAmount           = 4
	DETransmission     = 7
	DESTAN             = 11
	DELocalTime        = 12
	DELocalDate        = 13
	DEExpiry           = 14
	DEEntryMode        = 22
	DEConditionCode    = 25
	DEAcquirerID       = 32
	DERRN              = 37
	DEAuthCode         = 38
	DEResponseCode     = 39
	DETerminalID       = 41
	DEMerchantID       = 42
	DECurrency         = 49
	DEICCData          = 55
	DENetworkCode      = 70
	DEOriginalData     = 90
	processingPurchase = "000000"
	networkCodeEcho    = "301"
)

// ResponseApproved es el DE-39 de una aprobación o de una reversa aceptada.
const ResponseApproved = "00"

// ResponseOriginalNotFound es el DE-39 de una reversa de algo que el host no conoce. Para una
// reversa es un éxito: no hay nada que revertir.
const ResponseOriginalNotFound = "25"

// maxFrame limita un mensaje: lo que declare un largo mayor es un error de protocolo.
const maxFrame = 4096

// errFrameTooLarge indica un largo imposible.
var errFrameTooLarge = errors.New("iso8583: frame too large")

// Spec es el formato de los mensajes: 1987 en ASCII, más el DE-70 de los mensajes de red, que el
// spec de la librería no define.
var Spec = newSpec()

func newSpec() *moov.MessageSpec {
	fields := make(map[int]field.Field, len(specs.Spec87ASCII.Fields)+1)
	for id, f := range specs.Spec87ASCII.Fields {
		fields[id] = f
	}
	fields[DENetworkCode] = field.NewString(&field.Spec{
		Length:      3,
		Description: "Network Management Information Code",
		Enc:         encoding.ASCII,
		Pref:        prefix.ASCII.Fixed,
	})
	return &moov.MessageSpec{Name: "posnet ISO 8583:1987 ASCII", Fields: fields}
}

// currencyCodes traduce a ISO 4217 numérico, como pide DE-49.
var currencyCodes = map[domain.Currency]string{
	domain.ARS: "032", domain.USD: "840", domain.EUR: "978",
	domain.BRL: "986", domain.UYU: "858", domain.CLP: "152",
}

// entryModes traduce el modo de captura a DE-22.
var entryModes = map[valueobject.EntryMode]string{
	valueobject.EntryModeManual:      "012",
	valueobject.EntryModeChip:        "051",
	valueobject.EntryModeContactless: "071",
	valueobject.EntryModeMagstripe:   "901",
}

// RRN deriva el DE-37 del ID de la transacción: la reversa y un reintento usan el mismo, sin
// tener que guardarlo.
func RRN(id domain.TransactionID) string { return digest(id.String(), 12) }

func digest(value string, length int) string {
	sum := sha256.Sum256([]byte(value))
	return strings.ToUpper(hex.EncodeToString(sum[:]))[:length]
}

// transmission es el DE-7 de una transacción. Sale de su fecha de recepción, para que la reversa
// pueda repetirlo en los datos originales (DE-90).
func transmission(tx *aggregate.Transaction) string {
	return tx.ReceivedAt().UTC().Format("0102150405")
}

type fieldValue struct {
	id    int
	value string
}

func newMessage(mti string, fields []fieldValue) (*moov.Message, error) {
	msg := moov.NewMessage(Spec)
	msg.MTI(mti)
	for _, f := range fields {
		if f.value == "" {
			continue
		}
		if err := msg.Field(f.id, f.value); err != nil {
			// No incluye el valor: puede ser el PAN.
			return nil, fmt.Errorf("iso8583: set field %d: invalid value", f.id)
		}
	}
	return msg, nil
}

func financialFields(tx *aggregate.Transaction, card service.CardData, acquirerID string) ([]fieldValue, error) {
	currency, ok := currencyCodes[tx.Amount().Currency()]
	if !ok {
		return nil, fmt.Errorf("iso8583: currency %s has no numeric code", tx.Amount().Currency())
	}
	entryMode, ok := entryModes[tx.EntryMode()]
	if !ok {
		return nil, fmt.Errorf("iso8583: entry mode %s has no code", tx.EntryMode())
	}
	received := tx.ReceivedAt().UTC()
	return []fieldValue{
		{DEPAN, card.PAN},
		{DEProcessingCode, processingPurchase},
		{DEAmount, fmt.Sprintf("%012d", tx.Amount().Cents())},
		{DETransmission, transmission(tx)},
		{DESTAN, tx.STAN().String()},
		{DELocalTime, received.Format("150405")},
		{DELocalDate, received.Format("0102")},
		{DEExpiry, card.ExpiryYYMM},
		{DEEntryMode, entryMode},
		{DEConditionCode, "00"},
		{DEAcquirerID, acquirerID},
		{DERRN, RRN(tx.ID())},
		{DETerminalID, digest(tx.TerminalID().String(), 8)},
		{DEMerchantID, digest(tx.MerchantID().String(), 15)},
		{DECurrency, currency},
	}, nil
}

// BuildAuthorization arma un 0100.
func BuildAuthorization(tx *aggregate.Transaction, card service.CardData, acquirerID string) (*moov.Message, error) {
	fields, err := financialFields(tx, card, acquirerID)
	if err != nil {
		return nil, err
	}
	return newMessage(MTIAuthRequest, append(fields, fieldValue{DEICCData, tx.EMVDataBase64()}))
}

// BuildReversal arma un 0400, con los datos del 0100 original en DE-90.
func BuildReversal(tx *aggregate.Transaction, card service.CardData, acquirerID string) (*moov.Message, error) {
	fields, err := financialFields(tx, card, acquirerID)
	if err != nil {
		return nil, err
	}
	original := MTIAuthRequest + tx.STAN().String() + transmission(tx) +
		fmt.Sprintf("%011s", acquirerID) + strings.Repeat("0", 11)
	fields = append(fields, fieldValue{DEOriginalData, original})
	if tx.AuthCode() != nil {
		fields = append(fields, fieldValue{DEAuthCode, tx.AuthCode().String()})
	}
	return newMessage(MTIReversalRequest, fields)
}

// BuildEcho arma un 0800 de eco, que mantiene viva la conexión y detecta una caída.
func BuildEcho(stan int, now time.Time) (*moov.Message, error) {
	return newMessage(MTINetworkRequest, []fieldValue{
		{DETransmission, now.UTC().Format("0102150405")},
		{DESTAN, fmt.Sprintf("%06d", stan%1000000)},
		{DENetworkCode, networkCodeEcho},
	})
}

// Amount lee DE-4 en centavos.
func Amount(msg *moov.Message) (int64, error) {
	raw, err := msg.GetString(DEAmount)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(raw, 10, 64)
}

// WriteFrame escribe un mensaje con su largo en 2 bytes, en una sola escritura. Devuelve los bytes
// escritos: con 0, se sabe que el mensaje no salió.
func WriteFrame(w io.Writer, packed []byte) (int, error) {
	if len(packed) > maxFrame {
		return 0, errFrameTooLarge
	}
	frame := make([]byte, 2+len(packed))
	binary.BigEndian.PutUint16(frame, uint16(len(packed)))
	copy(frame[2:], packed)
	return w.Write(frame)
}

// ReadFrame lee un mensaje escrito por WriteFrame.
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint16(header[:])
	if int(length) > maxFrame {
		return nil, errFrameTooLarge
	}
	packed := make([]byte, length)
	if _, err := io.ReadFull(r, packed); err != nil {
		return nil, err
	}
	return packed, nil
}
