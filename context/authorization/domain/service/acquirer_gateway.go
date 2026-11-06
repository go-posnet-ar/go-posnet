// Package service contiene los Domain Services y puertos de salida del BC Authorization.
package service

import (
	"errors"
	"context"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
)

// AcquirerGateway es el puerto de salida hacia el Host Adquirente externo.
// El adaptador en infrastructure/postgres implementa esta interface
// usando ISO 8583 sobre TCP/TLS.
//
// Esta interface vive en el dominio porque el dominio la necesita,
// pero su implementación está en infraestructura.
//
// ErrNotSent distingue un error en el que seguro el mensaje no salió, y no hay nada que revertir.
// Cualquier otro error es ambiguo: la transacción pasa a INDETERMINATE y se revierte.
type AcquirerGateway interface {
	// Authorize envía el mensaje de autorización al adquirente y espera la respuesta.
	// El timeout es responsabilidad del implementador (típicamente 30s).
	// Retorna ErrTimeout si el adquirente no responde.
	Authorize(ctx context.Context, tx *aggregate.Transaction) (AcquirerResponse, error)

	// Reverse envía un mensaje de reversión (MTI 0400/0420) al adquirente.
	// El conciliador lo usa también cuando no sabe si la autorización llegó, y puede repetirlo:
	// debe dar éxito si el host no conoce la operación o si ya estaba revertida.
	Reverse(ctx context.Context, tx *aggregate.Transaction) error
}

// ErrNotSent indica que la autorización seguro no llegó al host.
var ErrNotSent = errors.New("acquirer: request was not sent")

// AcquirerResponse encapsula la respuesta del host adquirente.
type AcquirerResponse struct {
	// ResponseCode es el código ISO 8583 (DE-39): "00" = aprobado.
	ResponseCode string

	// AuthCode es el código de autorización (DE-38). Solo presente si ResponseCode == "00".
	AuthCode string

	// ARPCBase64 es el criptograma de respuesta del emisor para el chip EMV.
	ARPCBase64 string

	// RespondedAt es el timestamp de la respuesta del adquirente.
	RespondedAt time.Time
}

// IsApproved indica si la respuesta es una aprobación.
func (r AcquirerResponse) IsApproved() bool { return r.ResponseCode == valueobject.ISO_APPROVED }

// ToRejectionCode convierte el ResponseCode a un RejectionCode del dominio.
func (r AcquirerResponse) ToRejectionCode() (valueobject.RejectionCode, error) {
	return valueobject.NewRejectionFromISO(r.ResponseCode)
}
