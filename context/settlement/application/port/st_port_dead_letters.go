package port

import (
	"context"
	"time"
)

// DeadLetter es un evento que agotó sus entregas y quedó en la DLQ de Settlement
// (posnet.dlq.settlement.<subject> en el stream POSNET_DLQ).
type DeadLetter struct {
	Sequence   uint64    `json:"sequence"` // Secuencia en POSNET_DLQ: identifica al evento en la API
	Subject    string    `json:"subject"`  // Subject original, como posnet.auth.approved.v1
	EventID    string    `json:"event_id"`
	Error      string    `json:"error"` // Último error, recortado
	Deliveries int       `json:"deliveries"`
	FailedAt   time.Time `json:"failed_at"`
}

// DeadLetterQueue lista y reprocesa los eventos de la DLQ de Settlement.
type DeadLetterQueue interface {
	// List devuelve hasta limit eventos, del más antiguo al más nuevo.
	List(ctx context.Context, limit int) ([]DeadLetter, error)

	// Replay procesa el evento con el mismo código que el consumer y, si sale bien, lo borra de
	// la DLQ. NotFoundError si no está; con un error permanente o transitorio, queda en la DLQ.
	Replay(ctx context.Context, sequence uint64) error

	// Discard borra el evento de la DLQ sin procesarlo. NotFoundError si no está.
	Discard(ctx context.Context, sequence uint64) error

	// Count devuelve cuántos eventos hay en la DLQ.
	Count(ctx context.Context) (int, error)
}
