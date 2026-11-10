package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

type fakeDeadLetters struct {
	letters    []port.DeadLetter
	listErr    error
	replayErr  error
	discardErr error
	replayed   []uint64
	discarded  []uint64
	limit      int
}

func (f *fakeDeadLetters) List(_ context.Context, limit int) ([]port.DeadLetter, error) {
	f.limit = limit
	return f.letters, f.listErr
}
func (f *fakeDeadLetters) Replay(_ context.Context, seq uint64) error {
	f.replayed = append(f.replayed, seq)
	return f.replayErr
}
func (f *fakeDeadLetters) Discard(_ context.Context, seq uint64) error {
	f.discarded = append(f.discarded, seq)
	return f.discardErr
}
func (f *fakeDeadLetters) Count(context.Context) (int, error) { return len(f.letters), nil }

func deadLettersRouter(t *testing.T, dlq port.DeadLetterQueue) http.Handler {
	t.Helper()
	repo := &fakeBatchRepo{}
	tokens, err := NewTokenStore(testAdminKey, "")
	if err != nil {
		t.Fatalf("NewTokenStore() error = %v", err)
	}
	return NewRouter(query.NewBatchQueryHandler(repo), command.NewAdminHandler(repo, &fakeProcessor{}), &fakePool{},
		Security{Tokens: tokens}, nil, nil, dlq)
}

func doDeadLetters(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testAdminKey)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestListDeadLetters(t *testing.T) {
	dlq := &fakeDeadLetters{letters: []port.DeadLetter{{Sequence: 3, Subject: "posnet.auth.approved.v1", EventID: "evt-1"}}}
	h := deadLettersRouter(t, dlq)

	rec := doDeadLetters(h, http.MethodGet, "/dead-letters", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"event_id":"evt-1"`) || !strings.Contains(rec.Body.String(), `"count":1`) {
		t.Fatalf("GET /dead-letters = %d %s", rec.Code, rec.Body)
	}
	if dlq.limit != deadLettersDefaultLimit {
		t.Errorf("limit = %d, want %d", dlq.limit, deadLettersDefaultLimit)
	}
	for _, bad := range []string{"0", "501", "x"} {
		if rec := doDeadLetters(h, http.MethodGet, "/dead-letters?limit="+bad, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("limit=%s = %d, want 400", bad, rec.Code)
		}
	}
	dlq.listErr = errors.New("nats down")
	if rec := doDeadLetters(h, http.MethodGet, "/dead-letters", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("list error = %d, want 503", rec.Code)
	}
}

func TestReplayDeadLetter(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		err  error
		want int
	}{
		{"replayed", "/dead-letters/7/replay", `{"operator_id":"op-1"}`, nil, http.StatusOK},
		{"not found", "/dead-letters/7/replay", `{"operator_id":"op-1"}`, pkgerrors.NewNotFoundError("DeadLetter", "7"), http.StatusNotFound},
		{"permanent error", "/dead-letters/7/replay", `{"operator_id":"op-1"}`, pkgerrors.NewValidationError("invalid terminal_id"), http.StatusUnprocessableEntity},
		{"still failing", "/dead-letters/7/replay", `{"operator_id":"op-1"}`, errors.New("db down"), http.StatusServiceUnavailable},
		{"invalid sequence", "/dead-letters/x/replay", `{"operator_id":"op-1"}`, nil, http.StatusBadRequest},
		{"zero sequence", "/dead-letters/0/replay", `{"operator_id":"op-1"}`, nil, http.StatusBadRequest},
		{"no operator", "/dead-letters/7/replay", `{}`, nil, http.StatusBadRequest},
		{"bad body", "/dead-letters/7/replay", `{`, nil, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dlq := &fakeDeadLetters{replayErr: tc.err}
			rec := doDeadLetters(deadLettersRouter(t, dlq), http.MethodPost, tc.path, tc.body)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d (%s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestDiscardDeadLetter(t *testing.T) {
	dlq := &fakeDeadLetters{}
	h := deadLettersRouter(t, dlq)

	if rec := doDeadLetters(h, http.MethodPost, "/dead-letters/7/discard", `{"operator_id":"op-1"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("without reason = %d, want 400", rec.Code)
	}
	rec := doDeadLetters(h, http.MethodPost, "/dead-letters/7/discard", `{"operator_id":"op-1","reason":"duplicado de prueba"}`)
	if rec.Code != http.StatusOK || len(dlq.discarded) != 1 || dlq.discarded[0] != 7 {
		t.Errorf("discard = %d %s, discarded %v", rec.Code, rec.Body, dlq.discarded)
	}
	dlq.discardErr = pkgerrors.NewNotFoundError("DeadLetter", "7")
	if rec := doDeadLetters(h, http.MethodPost, "/dead-letters/7/discard", `{"operator_id":"op-1","reason":"x"}`); rec.Code != http.StatusNotFound {
		t.Errorf("missing = %d, want 404", rec.Code)
	}
}

func TestDeadLettersDisabled(t *testing.T) {
	if rec := doDeadLetters(deadLettersRouter(t, nil), http.MethodGet, "/dead-letters", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
}
