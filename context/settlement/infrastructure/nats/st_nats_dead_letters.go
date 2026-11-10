package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	natsclient "github.com/nats-io/nats.go"

	"github.com/juantevez/go-posnet/context/settlement/application/port"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// La DLQ de Settlement vive en POSNET_DLQ, que comparten todos los BC, bajo su propio prefijo.
// Está en NATS y no en Postgres porque la causa habitual de agotar las entregas es la base caída.
const (
	dlqStream = "POSNET_DLQ"
	dlqPrefix = "posnet.dlq.settlement."

	hdrDLQSubject    = "Posnet-Dlq-Subject"
	hdrDLQEventID    = "Posnet-Dlq-Event-Id"
	hdrDLQError      = "Posnet-Dlq-Error"
	hdrDLQDeliveries = "Posnet-Dlq-Deliveries"
	hdrDLQFailedAt   = "Posnet-Dlq-Failed-At"

	// dlqErrorMax recorta el error guardado: un header no admite valores arbitrariamente largos.
	dlqErrorMax = 1024
	// dlqScanMax acota cuántas secuencias recorre List.
	dlqScanMax = 10000
)

// deadLetter publica el mensaje en la DLQ, con los headers originales (la traza, entre ellos) y
// los datos del fallo. El Nats-Msg-Id deriva del evento: si el mensaje se vuelve a agotar antes
// de que se confirme, JetStream descarta el duplicado dentro de su ventana.
func (s *Subscriber) deadLetter(ctx context.Context, msg *natsclient.Msg, cause error, eventID string, delivered uint64) error {
	dl := natsclient.NewMsg(dlqPrefix + msg.Subject)
	dl.Data = msg.Data
	for k, v := range msg.Header {
		dl.Header[k] = v
	}
	errText := cause.Error()
	if len(errText) > dlqErrorMax {
		errText = errText[:dlqErrorMax]
	}
	dl.Header.Set(hdrDLQSubject, msg.Subject)
	dl.Header.Set(hdrDLQEventID, eventID)
	dl.Header.Set(hdrDLQError, strings.ReplaceAll(errText, "\n", " "))
	dl.Header.Set(hdrDLQDeliveries, strconv.FormatUint(delivered, 10))
	dl.Header.Set(hdrDLQFailedAt, time.Now().UTC().Format(time.RFC3339))
	msgID := "dlq-settlement-" + eventID
	if eventID == "" {
		if meta, err := msg.Metadata(); err == nil {
			msgID = fmt.Sprintf("dlq-settlement-%s-%d", msg.Subject, meta.Sequence.Stream)
		}
	}
	dl.Header.Set(natsclient.MsgIdHdr, msgID)

	if _, err := s.js.PublishMsg(dl, natsclient.Context(ctx)); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}

// DeadLetterQueue implementa port.DeadLetterQueue sobre POSNET_DLQ.
type DeadLetterQueue struct {
	js      natsclient.JetStreamContext
	process func(ctx context.Context, subject string, data []byte) (string, error)
}

// NewDeadLetterQueue reprocesa con el mismo código que los consumers de s.
func NewDeadLetterQueue(js natsclient.JetStreamContext, s *Subscriber) *DeadLetterQueue {
	return &DeadLetterQueue{js: js, process: s.process}
}

var _ port.DeadLetterQueue = (*DeadLetterQueue)(nil)

// Count cuenta los eventos de Settlement en la DLQ.
func (q *DeadLetterQueue) Count(ctx context.Context) (int, error) {
	info, err := q.js.StreamInfo(dlqStream, natsclient.Context(ctx), &natsclient.StreamInfoRequest{SubjectsFilter: dlqPrefix + ">"})
	if err != nil {
		return 0, fmt.Errorf("dead letters: stream info: %w", err)
	}
	var n uint64
	for _, c := range info.State.Subjects {
		n += c
	}
	return int(n), nil
}

// List recorre el stream desde la primera secuencia. Los otros BC no publican en POSNET_DLQ, así
// que casi todo lo que hay es de Settlement.
func (q *DeadLetterQueue) List(ctx context.Context, limit int) ([]port.DeadLetter, error) {
	info, err := q.js.StreamInfo(dlqStream, natsclient.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("dead letters: stream info: %w", err)
	}
	letters := []port.DeadLetter{}
	if info.State.Msgs == 0 {
		return letters, nil
	}
	last := info.State.LastSeq
	if last-info.State.FirstSeq >= dlqScanMax {
		last = info.State.FirstSeq + dlqScanMax - 1
	}
	for seq := info.State.FirstSeq; seq <= last && len(letters) < limit; seq++ {
		raw, err := q.js.GetMsg(dlqStream, seq, natsclient.Context(ctx))
		if errors.Is(err, natsclient.ErrMsgNotFound) {
			continue // borrado: ya se reprocesó o descartó
		}
		if err != nil {
			return nil, fmt.Errorf("dead letters: get %d: %w", seq, err)
		}
		if strings.HasPrefix(raw.Subject, dlqPrefix) {
			letters = append(letters, toDeadLetter(raw))
		}
	}
	return letters, nil
}

// Replay procesa el evento y, si sale bien, lo borra. Un evento ya registrado sale bien: la
// idempotencia por event_id lo descarta, y se borra igual.
func (q *DeadLetterQueue) Replay(ctx context.Context, sequence uint64) error {
	raw, err := q.get(ctx, sequence)
	if err != nil {
		return err
	}
	subject := raw.Header.Get(hdrDLQSubject)
	if subject == "" {
		subject = strings.TrimPrefix(raw.Subject, dlqPrefix)
	}
	if _, err := q.process(ctx, subject, raw.Data); err != nil {
		return fmt.Errorf("dead letter %d: %w", sequence, err)
	}
	if err := q.js.DeleteMsg(dlqStream, sequence, natsclient.Context(ctx)); err != nil && !errors.Is(err, natsclient.ErrMsgNotFound) {
		// Ya se procesó: otro intento lo descartaría por idempotencia.
		return fmt.Errorf("dead letter %d processed, but not deleted: %w", sequence, err)
	}
	return nil
}

// Discard borra el evento sin procesarlo.
func (q *DeadLetterQueue) Discard(ctx context.Context, sequence uint64) error {
	if _, err := q.get(ctx, sequence); err != nil {
		return err
	}
	if err := q.js.DeleteMsg(dlqStream, sequence, natsclient.Context(ctx)); err != nil {
		if errors.Is(err, natsclient.ErrMsgNotFound) {
			return pkgerrors.NewNotFoundError("DeadLetter", strconv.FormatUint(sequence, 10))
		}
		return fmt.Errorf("dead letter %d: delete: %w", sequence, err)
	}
	return nil
}

// get lee un evento de Settlement de la DLQ, o NotFoundError si no está o es de otro BC.
func (q *DeadLetterQueue) get(ctx context.Context, sequence uint64) (*natsclient.RawStreamMsg, error) {
	raw, err := q.js.GetMsg(dlqStream, sequence, natsclient.Context(ctx))
	if errors.Is(err, natsclient.ErrMsgNotFound) || (err == nil && !strings.HasPrefix(raw.Subject, dlqPrefix)) {
		return nil, pkgerrors.NewNotFoundError("DeadLetter", strconv.FormatUint(sequence, 10))
	}
	if err != nil {
		return nil, fmt.Errorf("dead letter %d: get: %w", sequence, err)
	}
	return raw, nil
}

func toDeadLetter(raw *natsclient.RawStreamMsg) port.DeadLetter {
	d := port.DeadLetter{
		Sequence: raw.Sequence,
		Subject:  raw.Header.Get(hdrDLQSubject),
		EventID:  raw.Header.Get(hdrDLQEventID),
		Error:    raw.Header.Get(hdrDLQError),
		FailedAt: raw.Time.UTC(),
	}
	if d.Subject == "" {
		d.Subject = strings.TrimPrefix(raw.Subject, dlqPrefix)
	}
	d.Deliveries, _ = strconv.Atoi(raw.Header.Get(hdrDLQDeliveries))
	if t, err := time.Parse(time.RFC3339, raw.Header.Get(hdrDLQFailedAt)); err == nil {
		d.FailedAt = t
	}
	return d
}
