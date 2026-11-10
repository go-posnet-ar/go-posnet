package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natsclient "github.com/nats-io/nats.go"

	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/events"
)

// fakeDLQ es un POSNET_DLQ en memoria.
type fakeDLQ struct {
	natsclient.JetStreamContext

	msgs      map[uint64]*natsclient.RawStreamMsg
	first     uint64
	last      uint64
	deleted   []uint64
	deleteErr error
}

func newFakeDLQ(msgs ...*natsclient.RawStreamMsg) *fakeDLQ {
	f := &fakeDLQ{msgs: map[uint64]*natsclient.RawStreamMsg{}}
	for _, m := range msgs {
		f.msgs[m.Sequence] = m
		if f.first == 0 || m.Sequence < f.first {
			f.first = m.Sequence
		}
		if m.Sequence > f.last {
			f.last = m.Sequence
		}
	}
	return f
}

func (f *fakeDLQ) StreamInfo(stream string, opts ...natsclient.JSOpt) (*natsclient.StreamInfo, error) {
	subjects := map[string]uint64{}
	for _, m := range f.msgs {
		if len(m.Subject) > len(dlqPrefix) && m.Subject[:len(dlqPrefix)] == dlqPrefix {
			subjects[m.Subject]++
		}
	}
	return &natsclient.StreamInfo{State: natsclient.StreamState{
		Msgs: uint64(len(f.msgs)), FirstSeq: f.first, LastSeq: f.last, Subjects: subjects,
	}}, nil
}

func (f *fakeDLQ) GetMsg(_ string, seq uint64, _ ...natsclient.JSOpt) (*natsclient.RawStreamMsg, error) {
	m, ok := f.msgs[seq]
	if !ok {
		return nil, natsclient.ErrMsgNotFound
	}
	return m, nil
}

func (f *fakeDLQ) DeleteMsg(_ string, seq uint64, _ ...natsclient.JSOpt) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if _, ok := f.msgs[seq]; !ok {
		return natsclient.ErrMsgNotFound
	}
	delete(f.msgs, seq)
	f.deleted = append(f.deleted, seq)
	return nil
}

func deadLetterMsg(seq uint64, subject string) *natsclient.RawStreamMsg {
	return &natsclient.RawStreamMsg{
		Subject: dlqPrefix + subject, Sequence: seq, Data: []byte(`{}`), Time: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC),
		Header: natsclient.Header{
			hdrDLQSubject: []string{subject}, hdrDLQEventID: []string{"evt-1"}, hdrDLQError: []string{"db down"},
			hdrDLQDeliveries: []string{"5"}, hdrDLQFailedAt: []string{"2026-11-10T12:00:01Z"},
		},
	}
}

func TestDeadLetterQueue_ListSkipsDeletedAndOtherBCs(t *testing.T) {
	other := deadLetterMsg(3, "x")
	other.Subject = "posnet.dlq.notification.x"
	js := newFakeDLQ(deadLetterMsg(2, events.SubjectAuthApproved), other, deadLetterMsg(5, events.SubjectBatchCloseRequested))
	q := &DeadLetterQueue{js: js}

	got, err := q.List(context.Background(), 10)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 2 || got[0].Sequence != 2 || got[1].Sequence != 5 {
		t.Fatalf("List() = %+v, want sequences 2 and 5", got)
	}
	want := time.Date(2026, 11, 10, 12, 0, 1, 0, time.UTC)
	if got[0].Subject != events.SubjectAuthApproved || got[0].EventID != "evt-1" || got[0].Error != "db down" ||
		got[0].Deliveries != 5 || !got[0].FailedAt.Equal(want) {
		t.Errorf("List()[0] = %+v", got[0])
	}

	if got, _ := q.List(context.Background(), 1); len(got) != 1 {
		t.Errorf("List(limit 1) = %d letters, want 1", len(got))
	}
	if n, err := q.Count(context.Background()); err != nil || n != 2 {
		t.Errorf("Count() = %d, %v, want 2", n, err)
	}
}

func TestDeadLetterQueue_Replay(t *testing.T) {
	tests := []struct {
		name        string
		processErr  error
		wantDeleted bool
		wantErr     bool
	}{
		{"processed: deleted", nil, true, false},
		{"still failing: kept", errors.New("db down"), false, true},
		{"permanent error: kept", pkgerrors.NewValidationError("invalid terminal_id"), false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newFakeDLQ(deadLetterMsg(7, events.SubjectAuthApproved))
			var gotSubject string
			q := &DeadLetterQueue{js: js, process: func(_ context.Context, subject string, _ []byte) (string, error) {
				gotSubject = subject
				return "evt-1", tc.processErr
			}}

			err := q.Replay(context.Background(), 7)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Replay() error = %v, wantErr %v", err, tc.wantErr)
			}
			if gotSubject != events.SubjectAuthApproved {
				t.Errorf("processed subject = %q, want the original", gotSubject)
			}
			if (len(js.deleted) == 1) != tc.wantDeleted {
				t.Errorf("deleted = %v, want deleted: %v", js.deleted, tc.wantDeleted)
			}
		})
	}
}

func TestDeadLetterQueue_NotFound(t *testing.T) {
	other := deadLetterMsg(3, "x")
	other.Subject = "posnet.dlq.notification.x"
	q := &DeadLetterQueue{js: newFakeDLQ(other), process: func(context.Context, string, []byte) (string, error) {
		t.Fatal("process called for a missing dead letter")
		return "", nil
	}}
	var notFound *pkgerrors.NotFoundError
	for _, seq := range []uint64{3, 99} {
		if err := q.Replay(context.Background(), seq); !errors.As(err, &notFound) {
			t.Errorf("Replay(%d) error = %v, want NotFoundError", seq, err)
		}
		if err := q.Discard(context.Background(), seq); !errors.As(err, &notFound) {
			t.Errorf("Discard(%d) error = %v, want NotFoundError", seq, err)
		}
	}
}

func TestDeadLetterQueue_Discard(t *testing.T) {
	js := newFakeDLQ(deadLetterMsg(4, events.SubjectReversalCompleted))
	q := &DeadLetterQueue{js: js}

	if err := q.Discard(context.Background(), 4); err != nil {
		t.Fatalf("Discard() error = %v", err)
	}
	if len(js.deleted) != 1 || js.deleted[0] != 4 {
		t.Errorf("deleted = %v, want [4]", js.deleted)
	}
}

// El reproceso usa el mismo código que el consumer: un evento malformado es un error permanente.
func TestProcess_MalformedEventIsPermanent(t *testing.T) {
	s := NewSubscriber(&fakeJetStream{}, newTestBatchHandler(nil, &fakeBatchRepo{}, &fakeDomainPublisher{}, &fakeProcessor{}))
	for _, data := range []string{"not-json", `{"event_id":"e","data":12345}`} {
		if _, err := s.process(context.Background(), events.SubjectAuthApproved, []byte(data)); !isPermanent(err) {
			t.Errorf("process(%s) error = %v, want a permanent error", data, err)
		}
	}
	if _, err := s.process(context.Background(), "posnet.other.v1", []byte(`{"event_id":"e","data":{}}`)); !isPermanent(err) {
		t.Errorf("process(unexpected subject) error = %v, want a permanent error", err)
	}
}
