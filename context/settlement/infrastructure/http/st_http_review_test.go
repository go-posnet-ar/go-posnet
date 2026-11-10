package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/query"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

const reviewTxID = "22222222-2222-4222-8222-222222222222"

type fakeReviewLog struct {
	listed  []repository.StoredReversalReview
	listErr error
	stored  *repository.StoredReversalReview
	closed  int
}

func (f *fakeReviewLog) FindPurchaseTx(context.Context, pgx.Tx, string) (*repository.ReversalPurchase, error) {
	return nil, nil
}
func (f *fakeReviewLog) InsertReversalReviewTx(context.Context, pgx.Tx, repository.ReversalReview) error {
	return nil
}
func (f *fakeReviewLog) ReconcileReversalReviewTx(context.Context, pgx.Tx, string, string, repository.ReversalPurchase) (repository.ReviewReconciliation, error) {
	return repository.ReviewReconciliation{}, nil
}
func (f *fakeReviewLog) InsertReversalTx(context.Context, pgx.Tx, string, string, int64, string) error {
	return nil
}
func (f *fakeReviewLog) CountOpenReversalReviews(context.Context, string) (int, error) { return 0, nil }
func (f *fakeReviewLog) FindReleasableHeldBatchIDs(context.Context) ([]string, error) {
	return nil, nil
}
func (f *fakeReviewLog) ListOpenReversalReviews(context.Context, int) ([]repository.StoredReversalReview, error) {
	return f.listed, f.listErr
}
func (f *fakeReviewLog) LockReversalReviewTx(context.Context, pgx.Tx, string) (*repository.StoredReversalReview, error) {
	return f.stored, nil
}
func (f *fakeReviewLog) UpdateReversalAmountTx(context.Context, pgx.Tx, string, string, int64) error {
	return nil
}
func (f *fakeReviewLog) DeleteReversalTx(context.Context, pgx.Tx, string, string) error { return nil }
func (f *fakeReviewLog) CloseReversalReviewTx(context.Context, pgx.Tx, string, string, string, string) error {
	f.closed++
	return nil
}

type fakeAudit struct{}

func (fakeAudit) InsertAuditTx(context.Context, pgx.Tx, repository.BatchAuditEntry) error { return nil }

type fakeTimezoneStore struct {
	timezone string
	err      error
	setErr   error
}

func (f *fakeTimezoneStore) MerchantTimezone(context.Context, string) (string, error) {
	return f.timezone, f.err
}
func (f *fakeTimezoneStore) MerchantTimezoneTx(context.Context, pgx.Tx, string) (string, error) {
	return f.timezone, f.err
}
func (f *fakeTimezoneStore) SetMerchantTimezone(context.Context, string, string, string, string) ([]repository.RedatedBatch, error) {
	return nil, f.setErr
}

func reviewMux(t *testing.T, reviews *fakeReviewLog, timezones *fakeTimezoneStore, expectTx bool) *http.ServeMux {
	t.Helper()
	pool, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	t.Cleanup(pool.Close)
	if expectTx {
		pool.MatchExpectationsInOrder(false)
		pool.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		pool.ExpectCommit()
		pool.ExpectRollback()
	}

	repo := &fakeBatchRepo{}
	admin := command.NewAdminHandler(repo, &fakeProcessor{})
	admin.ConfigureSettlement(pool, fakeAudit{}, nil, outbox.NewStore("settlement"), 2.5)
	admin.SetReversalReviews(reviews)
	admin.SetMerchantTimezones(timezones)
	q := query.NewBatchQueryHandler(repo)
	q.ConfigureReviews(reviews)
	q.ConfigureTimezones(timezones, "America/Argentina/Buenos_Aires")
	return newTestMux(NewHandler(q, admin, &fakePool{}))
}

func serve(mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestListReversalReviews(t *testing.T) {
	reviews := &fakeReviewLog{listed: []repository.StoredReversalReview{{
		ReversalReview: repository.ReversalReview{
			TransactionID: reviewTxID, Reason: repository.ReversalReviewAmountMismatch, AmountCents: 500, Currency: "ARS",
			Purchase: &repository.ReversalPurchase{MerchantID: "m-1", AmountCents: 900, Currency: "ARS"},
		},
		BatchState: "DISPUTED", CreatedAt: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
	}}}
	rec := serve(reviewMux(t, reviews, &fakeTimezoneStore{}, false), http.MethodGet, "/reversal-reviews", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	body := decodeJSON(t, rec.Body.Bytes())
	if body["count"] != float64(1) {
		t.Errorf("count = %v", body["count"])
	}
	first := body["reviews"].([]any)[0].(map[string]any)
	if first["transaction_id"] != reviewTxID || first["purchase_amount"] != float64(900) || first["batch_state"] != "DISPUTED" || first["created_at"] != "2026-11-05T12:00:00Z" {
		t.Errorf("review = %v", first)
	}
}

func TestListReversalReviews_Error(t *testing.T) {
	rec := serve(reviewMux(t, &fakeReviewLog{listErr: errors.New("db down")}, &fakeTimezoneStore{}, false), http.MethodGet, "/reversal-reviews", "")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestResolveReversalReview_HTTP(t *testing.T) {
	open := &repository.StoredReversalReview{ReversalReview: repository.ReversalReview{TransactionID: reviewTxID}, BatchState: "OPEN"}
	valid := `{"operator_id":"op","resolution":"ticket","action":"ACCEPT"}`
	path := "/reversal-reviews/" + reviewTxID + "/resolve"
	cases := []struct {
		name     string
		path     string
		body     string
		stored   *repository.StoredReversalReview
		expectTx bool
		want     int
		wantBody string
	}{
		{"resuelve", path, valid, open, true, 200, "resolved"},
		{"no existe", path, valid, nil, true, 404, "reversal review not found"},
		{"id inválido", "/reversal-reviews/x/resolve", valid, open, false, 400, "INVALID_ID"},
		{"cuerpo inválido", path, "{", open, false, 400, "INVALID_BODY"},
		{"sin resolución", path, `{"operator_id":"op","action":"ACCEPT"}`, open, false, 400, "VALIDATION_ERROR"},
		{"acción inválida", path, `{"operator_id":"op","resolution":"t","action":"X"}`, open, false, 400, "VALIDATION_ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reviews := &fakeReviewLog{stored: tc.stored}
			rec := serve(reviewMux(t, reviews, &fakeTimezoneStore{}, tc.expectTx), http.MethodPost, tc.path, tc.body)
			if rec.Code != tc.want || !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("got %d %s, want %d with %q", rec.Code, rec.Body, tc.want, tc.wantBody)
			}
		})
	}
}

func TestGetMerchantTimezone(t *testing.T) {
	merchant := domain.NewMerchantID().String()
	cases := []struct {
		name  string
		id    string
		store *fakeTimezoneStore
		want  int
		body  string
	}{
		{"propia", merchant, &fakeTimezoneStore{timezone: "Asia/Tokyo"}, 200, `"source":"merchant"`},
		{"general", merchant, &fakeTimezoneStore{}, 200, `"timezone":"America/Argentina/Buenos_Aires"`},
		{"comercio inválido", "x", &fakeTimezoneStore{}, 400, "VALIDATION_ERROR"},
		{"error de base", merchant, &fakeTimezoneStore{err: errors.New("db down")}, 500, "INTERNAL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(reviewMux(t, &fakeReviewLog{}, tc.store, false), http.MethodGet, "/merchants/"+tc.id+"/timezone", "")
			if rec.Code != tc.want || !strings.Contains(rec.Body.String(), tc.body) {
				t.Errorf("got %d %s, want %d with %q", rec.Code, rec.Body, tc.want, tc.body)
			}
		})
	}
}

func TestSetMerchantTimezone_HTTP(t *testing.T) {
	merchant := domain.NewMerchantID().String()
	cases := []struct {
		name   string
		body   string
		setErr error
		want   int
	}{
		{"válida", `{"timezone":"Asia/Tokyo","operator_id":"op"}`, nil, 200},
		{"zona inválida", `{"timezone":"Mars/Base","operator_id":"op"}`, nil, 400},
		{"sin operador", `{"timezone":"Asia/Tokyo"}`, nil, 400},
		{"cuerpo inválido", `{`, nil, 400},
		{"choca con un lote abierto", `{"timezone":"Asia/Tokyo","operator_id":"op"}`, repository.ErrOpenBatchConflict, 409},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(reviewMux(t, &fakeReviewLog{}, &fakeTimezoneStore{setErr: tc.setErr}, false), http.MethodPut, "/merchants/"+merchant+"/timezone", tc.body)
			if rec.Code != tc.want {
				t.Errorf("got %d %s, want %d", rec.Code, rec.Body, tc.want)
			}
		})
	}
}

// Una escritura nueva queda protegida por el token sin agregarla a ninguna lista.
func TestNewWritesRequireAdminToken(t *testing.T) {
	tokens, err := NewTokenStore(strings.Repeat("k", 32), "")
	if err != nil {
		t.Fatalf("NewTokenStore() error = %v", err)
	}
	handler := authMiddleware(Security{Tokens: tokens}, reviewMux(t, &fakeReviewLog{}, &fakeTimezoneStore{}, false))
	merchant := domain.NewMerchantID().String()
	for _, r := range []struct{ method, path string }{
		{http.MethodPost, "/reversal-reviews/" + reviewTxID + "/resolve"},
		{http.MethodPut, "/merchants/" + merchant + "/timezone"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(r.method, r.path, strings.NewReader("{}")))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s = %d, want 401", r.method, r.path, rec.Code)
		}
	}
}
