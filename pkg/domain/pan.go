package domain

import (
	"fmt"
	"regexp"
)

// CardNetwork identifica la red de la tarjeta.
type CardNetwork string

const (
	NetworkVisa       CardNetwork = "VISA"
	NetworkMastercard CardNetwork = "MASTERCARD"
	NetworkAmex       CardNetwork = "AMEX"
	NetworkCabal      CardNetwork = "CABAL"
	NetworkNaranja    CardNetwork = "NARANJA"
	NetworkUnknown    CardNetwork = "UNKNOWN"
)

var validNetworks = map[CardNetwork]struct{}{
	NetworkVisa: {}, NetworkMastercard: {}, NetworkAmex: {},
	NetworkCabal: {}, NetworkNaranja: {}, NetworkUnknown: {},
}

func (n CardNetwork) IsValid() bool {
	_, ok := validNetworks[n]
	return ok
}

// ParseCardNetwork parsea un string y devuelve error si no es una red conocida.
// Función exportada — usada por los repositorios al reconstruir aggregates desde Postgres.
func ParseCardNetwork(s string) (CardNetwork, error) {
	n := CardNetwork(s)
	if !n.IsValid() {
		return NetworkUnknown, fmt.Errorf("unknown card network %q", s)
	}
	return n, nil
}

var reFourDigits = regexp.MustCompile(`^\d{4}$`)

// PAN representa el número de tarjeta de forma segura.
// INVARIANTE CRÍTICA: solo almacena los últimos 4 dígitos.
// El PAN completo no se guarda ni viaja por eventos: solo el adaptador del adquirente lo tiene en
// memoria, mientras arma un mensaje (ver service.CardDataProvider en Authorization).
type PAN struct {
	last4   string
	network CardNetwork
}

// NewPAN crea un PAN validando que last4 sean exactamente 4 dígitos.
func NewPAN(last4 string, network CardNetwork) (PAN, error) {
	if !reFourDigits.MatchString(last4) {
		return PAN{}, fmt.Errorf("pan: last4 must be exactly 4 digits, got %q", last4)
	}
	if !network.IsValid() {
		return PAN{}, fmt.Errorf("pan: unknown card network %q", network)
	}
	return PAN{last4: last4, network: network}, nil
}

// Last4 devuelve los últimos 4 dígitos.
func (p PAN) Last4() string { return p.last4 }

// Network devuelve la red de la tarjeta.
func (p PAN) Network() CardNetwork { return p.network }

// Masked devuelve la representación de comprobante: "**** **** **** 1234"
func (p PAN) Masked() string {
	return fmt.Sprintf("**** **** **** %s", p.last4)
}

// String implementa fmt.Stringer — usa la representación enmascarada.
func (p PAN) String() string { return p.Masked() }
