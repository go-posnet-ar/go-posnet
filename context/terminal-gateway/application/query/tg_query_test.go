package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/terminal-gateway/application/query"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/aggregate"
	"github.com/juantevez/go-posnet/context/terminal-gateway/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
)

// ─── GetSessionStatus ───────────────────────────────────────────────────────────

func TestGetSessionStatus_Success(t *testing.T) {
	session := newSession(t, valueobject.StateAwaitingPayment)
	repo := &fakeSessionRepo{findByIDResult: session}
	h := query.NewSessionQueryHandler(repo)

	result, err := h.GetSessionStatus(context.Background(), session.ID())
	if err != nil {
		t.Fatalf("GetSessionStatus() error = %v", err)
	}
	if result.TransactionID != session.ID().String() {
		t.Errorf("TransactionID = %q, want %q", result.TransactionID, session.ID().String())
	}
	if result.TerminalID != session.TerminalID().String() {
		t.Errorf("TerminalID = %q, want %q", result.TerminalID, session.TerminalID().String())
	}
	if result.MerchantID != session.MerchantID().String() {
		t.Errorf("MerchantID = %q, want %q", result.MerchantID, session.MerchantID().String())
	}
	if result.State != string(valueobject.StateAwaitingPayment) {
		t.Errorf("State = %q, want %q", result.State, valueobject.StateAwaitingPayment)
	}
	if result.Channel != valueobject.ChannelNFC.String() {
		t.Errorf("Channel = %q, want %q", result.Channel, valueobject.ChannelNFC.String())
	}
	if result.AmountCents != session.Amount().Cents() {
		t.Errorf("AmountCents = %d, want %d", result.AmountCents, session.Amount().Cents())
	}
	if result.Currency != session.Amount().Currency().String() {
		t.Errorf("Currency = %q, want %q", result.Currency, session.Amount().Currency().String())
	}
	if result.CreatedAt != session.CreatedAt().Format("2006-01-02T15:04:05Z") {
		t.Errorf("CreatedAt = %q, want %q", result.CreatedAt, session.CreatedAt().Format("2006-01-02T15:04:05Z"))
	}
	if result.ExpiresAt != session.ExpiresAt().Format("2006-01-02T15:04:05Z") {
		t.Errorf("ExpiresAt = %q, want %q", result.ExpiresAt, session.ExpiresAt().Format("2006-01-02T15:04:05Z"))
	}
	if result.AuthCode != "" {
		t.Errorf("AuthCode = %q, want empty (state != APPROVED)", result.AuthCode)
	}
	if result.RejectionCode != "" || result.RejectionReason != "" {
		t.Errorf("RejectionCode/Reason = %q/%q, want empty (state != REJECTED)", result.RejectionCode, result.RejectionReason)
	}
}

func TestGetSessionStatus_Approved_IncludesAuthCode(t *testing.T) {
	session := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:         domain.NewTransactionID(),
		TerminalID: domain.NewTerminalID(),
		MerchantID: domain.NewMerchantID(),
		Amount:     mustMoney(t),
		STAN:       mustSTAN(t),
		Channel:    valueobject.ChannelNFC,
		State:      valueobject.StateApproved,
		AuthCode:   "AUTH123",
	})
	repo := &fakeSessionRepo{findByIDResult: session}
	h := query.NewSessionQueryHandler(repo)

	result, err := h.GetSessionStatus(context.Background(), session.ID())
	if err != nil {
		t.Fatalf("GetSessionStatus() error = %v", err)
	}
	if result.AuthCode != "AUTH123" {
		t.Errorf("AuthCode = %q, want %q", result.AuthCode, "AUTH123")
	}
	if result.RejectionCode != "" || result.RejectionReason != "" {
		t.Errorf("RejectionCode/Reason = %q/%q, want empty", result.RejectionCode, result.RejectionReason)
	}
}

func TestGetSessionStatus_Rejected_IncludesRejectionFields(t *testing.T) {
	session := aggregate.Reconstitute(aggregate.ReconstituteParams{
		ID:              domain.NewTransactionID(),
		TerminalID:      domain.NewTerminalID(),
		MerchantID:      domain.NewMerchantID(),
		Amount:          mustMoney(t),
		STAN:            mustSTAN(t),
		Channel:         valueobject.ChannelNFC,
		State:           valueobject.StateRejected,
		RejectionCode:   "05",
		RejectionReason: "Do not honor",
	})
	repo := &fakeSessionRepo{findByIDResult: session}
	h := query.NewSessionQueryHandler(repo)

	result, err := h.GetSessionStatus(context.Background(), session.ID())
	if err != nil {
		t.Fatalf("GetSessionStatus() error = %v", err)
	}
	if result.RejectionCode != "05" {
		t.Errorf("RejectionCode = %q, want %q", result.RejectionCode, "05")
	}
	if result.RejectionReason != "Do not honor" {
		t.Errorf("RejectionReason = %q, want %q", result.RejectionReason, "Do not honor")
	}
	if result.AuthCode != "" {
		t.Errorf("AuthCode = %q, want empty", result.AuthCode)
	}
}

func TestGetSessionStatus_RepoError(t *testing.T) {
	repo := &fakeSessionRepo{findByIDErr: errors.New("connection reset")}
	h := query.NewSessionQueryHandler(repo)

	_, err := h.GetSessionStatus(context.Background(), domain.NewTransactionID())
	if err == nil || !strings.Contains(err.Error(), "GetSessionStatus") {
		t.Fatalf("error = %v, want it to contain %q", err, "GetSessionStatus")
	}
}

func TestGetSessionStatus_NotFound(t *testing.T) {
	repo := &fakeSessionRepo{}
	h := query.NewSessionQueryHandler(repo)

	_, err := h.GetSessionStatus(context.Background(), domain.NewTransactionID())

	var notFound *pkgerrors.NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("error = %v, want *pkgerrors.NotFoundError", err)
	}
}

// ─── GetActiveSession ───────────────────────────────────────────────────────────

func TestGetActiveSession_Success(t *testing.T) {
	session := newSession(t, valueobject.StateProcessing)
	repo := &fakeSessionRepo{findActiveResult: session}
	h := query.NewSessionQueryHandler(repo)

	result, err := h.GetActiveSession(context.Background(), session.TerminalID())
	if err != nil {
		t.Fatalf("GetActiveSession() error = %v", err)
	}
	if result == nil {
		t.Fatal("result = nil, want non-nil")
	}
	if result.TransactionID != session.ID().String() {
		t.Errorf("TransactionID = %q, want %q", result.TransactionID, session.ID().String())
	}
	if result.State != string(valueobject.StateProcessing) {
		t.Errorf("State = %q, want %q", result.State, valueobject.StateProcessing)
	}
}

func TestGetActiveSession_NoActiveSession(t *testing.T) {
	repo := &fakeSessionRepo{}
	h := query.NewSessionQueryHandler(repo)

	result, err := h.GetActiveSession(context.Background(), domain.NewTerminalID())
	if err != nil {
		t.Fatalf("GetActiveSession() error = %v, want nil", err)
	}
	if result != nil {
		t.Errorf("result = %v, want nil", result)
	}
}

func TestGetActiveSession_RepoError(t *testing.T) {
	repo := &fakeSessionRepo{findActiveErr: errors.New("connection reset")}
	h := query.NewSessionQueryHandler(repo)

	_, err := h.GetActiveSession(context.Background(), domain.NewTerminalID())
	if err == nil || !strings.Contains(err.Error(), "GetActiveSession") {
		t.Fatalf("error = %v, want it to contain %q", err, "GetActiveSession")
	}
}

// La orden de retener la tarjeta se deriva del código de rechazo guardado: el del emisor (04,
// 41, 43) o el de la blocklist.
func TestGetSessionStatus_CaptureCard(t *testing.T) {
	cases := []struct {
		state valueobject.SessionState
		code  string
		want  bool
	}{
		{valueobject.StateRejected, "41", true},
		{valueobject.StateRejected, "CARD_BLOCKED", true},
		{valueobject.StateRejected, "51", false},
		{valueobject.StateRejected, "FRAUD_REJECTED", false},
		{valueobject.StateApproved, "", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.state)+" "+tc.code, func(t *testing.T) {
			session := aggregate.Reconstitute(aggregate.ReconstituteParams{
				ID:            domain.NewTransactionID(),
				TerminalID:    domain.NewTerminalID(),
				MerchantID:    domain.NewMerchantID(),
				Amount:        mustMoney(t),
				STAN:          mustSTAN(t),
				Channel:       valueobject.ChannelQR,
				State:         tc.state,
				RejectionCode: tc.code,
			})
			result, err := query.NewSessionQueryHandler(&fakeSessionRepo{findByIDResult: session}).GetSessionStatus(context.Background(), session.ID())
			if err != nil {
				t.Fatalf("GetSessionStatus() error = %v", err)
			}
			if result.CaptureCard != tc.want {
				t.Errorf("CaptureCard = %v, want %v", result.CaptureCard, tc.want)
			}
		})
	}
}
