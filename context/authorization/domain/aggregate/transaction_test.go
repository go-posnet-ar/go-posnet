package aggregate_test

import (
	"strings"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/event"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
)

func TestNewTransaction_Success(t *testing.T) {
	id := domain.NewTransactionID()
	tid := domain.NewTerminalID()
	mid := domain.NewMerchantID()
	amount := mustMoney(t, 5000)
	stan := mustSTAN(t, 42)
	pan := mustPAN(t)

	before := time.Now().UTC()
	tx, err := aggregate.NewTransaction(id, tid, mid, amount, stan, pan, valueobject.EntryModeChip, domain.CardToken{}, "emv-data", []byte{0xAA})
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("NewTransaction() error = %v", err)
	}

	if !tx.ID().Equals(id) {
		t.Errorf("ID() = %v, want %v", tx.ID(), id)
	}
	if !tx.TerminalID().Equals(tid) {
		t.Errorf("TerminalID() = %v, want %v", tx.TerminalID(), tid)
	}
	if !tx.MerchantID().Equals(mid) {
		t.Errorf("MerchantID() = %v, want %v", tx.MerchantID(), mid)
	}
	if !tx.Amount().Equals(amount) {
		t.Errorf("Amount() = %v, want %v", tx.Amount(), amount)
	}
	if !tx.STAN().Equals(stan) {
		t.Errorf("STAN() = %v, want %v", tx.STAN(), stan)
	}
	if tx.PAN() != pan {
		t.Errorf("PAN() = %v, want %v", tx.PAN(), pan)
	}
	if tx.EntryMode() != valueobject.EntryModeChip {
		t.Errorf("EntryMode() = %v, want %v", tx.EntryMode(), valueobject.EntryModeChip)
	}
	if tx.State() != valueobject.StateReceived {
		t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateReceived)
	}
	if tx.EMVDataBase64() != "emv-data" {
		t.Errorf("EMVDataBase64() = %q, want %q", tx.EMVDataBase64(), "emv-data")
	}
	if string(tx.ISO8583Raw()) != string([]byte{0xAA}) {
		t.Errorf("ISO8583Raw() = %v, want %v", tx.ISO8583Raw(), []byte{0xAA})
	}
	if tx.ReceivedAt().Before(before) || tx.ReceivedAt().After(after) {
		t.Errorf("ReceivedAt() = %v, want between %v and %v", tx.ReceivedAt(), before, after)
	}
	if tx.AuthorizedAt() != nil {
		t.Errorf("AuthorizedAt() = %v, want nil", tx.AuthorizedAt())
	}
	if tx.RejectedAt() != nil {
		t.Errorf("RejectedAt() = %v, want nil", tx.RejectedAt())
	}
	if tx.AuthCode() != nil {
		t.Errorf("AuthCode() = %v, want nil", tx.AuthCode())
	}
	if tx.RejectionCode() != nil {
		t.Errorf("RejectionCode() = %v, want nil", tx.RejectionCode())
	}

	events := tx.DomainEvents()
	if len(events) != 1 {
		t.Fatalf("len(DomainEvents()) = %d, want 1", len(events))
	}
	created, ok := events[0].(event.TransactionCreated)
	if !ok {
		t.Fatalf("DomainEvents()[0] type = %T, want event.TransactionCreated", events[0])
	}
	if !created.TransactionID.Equals(id) {
		t.Errorf("TransactionCreated.TransactionID = %v, want %v", created.TransactionID, id)
	}
}

func TestNewTransaction_ValidationErrors(t *testing.T) {
	validAmount := mustMoney(t, 100)
	var zeroMoney domain.Money

	tests := []struct {
		name       string
		id         domain.TransactionID
		terminalID domain.TerminalID
		merchantID domain.MerchantID
		amount     domain.Money
		wantErr    string
	}{
		{
			name:       "zero transaction id",
			id:         domain.TransactionID{},
			terminalID: domain.NewTerminalID(),
			merchantID: domain.NewMerchantID(),
			amount:     validAmount,
			wantErr:    "id cannot be zero",
		},
		{
			name:       "zero terminal id",
			id:         domain.NewTransactionID(),
			terminalID: domain.TerminalID{},
			merchantID: domain.NewMerchantID(),
			amount:     validAmount,
			wantErr:    "terminal_id cannot be zero",
		},
		{
			name:       "zero merchant id",
			id:         domain.NewTransactionID(),
			terminalID: domain.NewTerminalID(),
			merchantID: domain.MerchantID{},
			amount:     validAmount,
			wantErr:    "merchant_id cannot be zero",
		},
		{
			name:       "non-positive amount",
			id:         domain.NewTransactionID(),
			terminalID: domain.NewTerminalID(),
			merchantID: domain.NewMerchantID(),
			amount:     zeroMoney,
			wantErr:    "amount must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := aggregate.NewTransaction(
				tc.id, tc.terminalID, tc.merchantID, tc.amount,
				mustSTAN(t, 1), mustPAN(t), valueobject.EntryModeChip, domain.CardToken{}, "emv", nil,
			)
			if err == nil {
				t.Fatalf("NewTransaction() error = nil, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("NewTransaction() error = %q, want it to contain %q", err.Error(), tc.wantErr)
			}
			if tx != nil {
				t.Errorf("NewTransaction() tx = %v, want nil", tx)
			}
		})
	}
}

func TestTransaction_StartFraudCheck(t *testing.T) {
	t.Run("success from received", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		if tx.State() != valueobject.StateFraudChecking {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateFraudChecking)
		}
		events := tx.DomainEvents()
		last := events[len(events)-1]
		if last.EventType() != "fraud.check.started" {
			t.Errorf("last event type = %q, want %q", last.EventType(), "fraud.check.started")
		}
	})

	t.Run("invalid when already fraud checking", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		if err := tx.StartFraudCheck(); err == nil {
			t.Fatal("second StartFraudCheck() error = nil, want error")
		}
	})
}

func TestTransaction_ApplyFraudDecision(t *testing.T) {
	t.Run("wrong state", func(t *testing.T) {
		tx := newValidTransaction(t)
		fd, err := valueobject.NewFraudDecision(10, valueobject.FraudDecisionApprove, nil)
		if err != nil {
			t.Fatalf("NewFraudDecision() error = %v", err)
		}
		if err := tx.ApplyFraudDecision(fd); err == nil {
			t.Fatal("ApplyFraudDecision() error = nil, want error")
		}
	})

	t.Run("reject decision transitions to rejected", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		fd, err := valueobject.NewFraudDecision(90, valueobject.FraudDecisionReject, []string{"R1"})
		if err != nil {
			t.Fatalf("NewFraudDecision() error = %v", err)
		}
		if err := tx.ApplyFraudDecision(fd); err != nil {
			t.Fatalf("ApplyFraudDecision() error = %v", err)
		}
		if tx.State() != valueobject.StateRejected {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateRejected)
		}
		if tx.FraudDecision().Decision != valueobject.FraudDecisionReject {
			t.Errorf("FraudDecision().Decision = %v, want %v", tx.FraudDecision().Decision, valueobject.FraudDecisionReject)
		}
		rc := tx.RejectionCode()
		if rc == nil {
			t.Fatal("RejectionCode() = nil, want non-nil")
		}
		if rc.Source() != valueobject.SourceFraud {
			t.Errorf("RejectionCode().Source() = %v, want %v", rc.Source(), valueobject.SourceFraud)
		}
		if tx.RejectedAt() == nil {
			t.Error("RejectedAt() = nil, want non-nil")
		}
	})

	t.Run("approve decision transitions to processing", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		fd, err := valueobject.NewFraudDecision(10, valueobject.FraudDecisionApprove, nil)
		if err != nil {
			t.Fatalf("NewFraudDecision() error = %v", err)
		}
		if err := tx.ApplyFraudDecision(fd); err != nil {
			t.Fatalf("ApplyFraudDecision() error = %v", err)
		}
		if tx.State() != valueobject.StateProcessing {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateProcessing)
		}
	})

	t.Run("review decision transitions to processing", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		fd, err := valueobject.NewFraudDecision(60, valueobject.FraudDecisionReview, []string{"R2"})
		if err != nil {
			t.Fatalf("NewFraudDecision() error = %v", err)
		}
		if err := tx.ApplyFraudDecision(fd); err != nil {
			t.Fatalf("ApplyFraudDecision() error = %v", err)
		}
		if tx.State() != valueobject.StateProcessing {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateProcessing)
		}
	})
}

func TestTransaction_ExpireFraudCheck(t *testing.T) {
	t.Run("wrong state", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.ExpireFraudCheck(); err == nil {
			t.Fatal("ExpireFraudCheck() error = nil, want error")
		}
	})

	t.Run("rejects as retryable without capturing the card", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.StartFraudCheck(); err != nil {
			t.Fatalf("StartFraudCheck() error = %v", err)
		}
		if err := tx.ExpireFraudCheck(); err != nil {
			t.Fatalf("ExpireFraudCheck() error = %v", err)
		}
		if tx.State() != valueobject.StateRejected {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateRejected)
		}
		rc := tx.RejectionCode()
		if rc.Code() != valueobject.CodeFraudTimeout || rc.Source() != valueobject.SourceFraud {
			t.Errorf("RejectionCode() = %s/%s, want %s/%s", rc.Code(), rc.Source(), valueobject.CodeFraudTimeout, valueobject.SourceFraud)
		}
		if !rc.IsRetryable() || rc.RequiresCardCapture() {
			t.Errorf("IsRetryable() = %v, RequiresCardCapture() = %v, want true, false", rc.IsRetryable(), rc.RequiresCardCapture())
		}
		if tx.RejectedAt() == nil {
			t.Error("RejectedAt() = nil, want the rejection time")
		}
	})
}

func TestTransaction_Approve(t *testing.T) {
	t.Run("success from processing", func(t *testing.T) {
		tx := newProcessingTransaction(t)
		ac := mustAuthCode(t, "AB1234")

		before := time.Now().UTC()
		if err := tx.Approve(ac); err != nil {
			t.Fatalf("Approve() error = %v", err)
		}
		after := time.Now().UTC()

		if tx.State() != valueobject.StateApproved {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateApproved)
		}
		if tx.AuthCode() == nil || !tx.AuthCode().Equals(ac) {
			t.Errorf("AuthCode() = %v, want %v", tx.AuthCode(), ac)
		}
		authorizedAt := tx.AuthorizedAt()
		if authorizedAt == nil {
			t.Fatal("AuthorizedAt() = nil, want non-nil")
		}
		if authorizedAt.Before(before) || authorizedAt.After(after) {
			t.Errorf("AuthorizedAt() = %v, want between %v and %v", authorizedAt, before, after)
		}

		events := tx.DomainEvents()
		last, ok := events[len(events)-1].(event.TransactionApproved)
		if !ok {
			t.Fatalf("last event type = %T, want event.TransactionApproved", events[len(events)-1])
		}
		if !last.AuthCode.Equals(ac) {
			t.Errorf("TransactionApproved.AuthCode = %v, want %v", last.AuthCode, ac)
		}
	})

	t.Run("invalid transition from received", func(t *testing.T) {
		tx := newValidTransaction(t)
		ac := mustAuthCode(t, "AB1234")
		if err := tx.Approve(ac); err == nil {
			t.Fatal("Approve() error = nil, want error")
		}
		if tx.AuthCode() != nil {
			t.Errorf("AuthCode() = %v, want nil after failed Approve", tx.AuthCode())
		}
		if tx.State() != valueobject.StateReceived {
			t.Errorf("State() = %v, want unchanged %v", tx.State(), valueobject.StateReceived)
		}
	})
}

func TestTransaction_Reject(t *testing.T) {
	t.Run("success from received", func(t *testing.T) {
		tx := newValidTransaction(t)
		rc, err := valueobject.NewRejectionFromISO(valueobject.ISO_DO_NOT_HONOR)
		if err != nil {
			t.Fatalf("NewRejectionFromISO() error = %v", err)
		}

		if err := tx.Reject(rc); err != nil {
			t.Fatalf("Reject() error = %v", err)
		}
		if tx.State() != valueobject.StateRejected {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateRejected)
		}
		got := tx.RejectionCode()
		if got == nil || got.Code() != rc.Code() {
			t.Errorf("RejectionCode() = %v, want %v", got, rc)
		}
		if tx.RejectedAt() == nil {
			t.Error("RejectedAt() = nil, want non-nil")
		}

		events := tx.DomainEvents()
		last := events[len(events)-1]
		if last.EventType() != "transaction.rejected" {
			t.Errorf("last event type = %q, want %q", last.EventType(), "transaction.rejected")
		}
	})

	t.Run("invalid transition from approved", func(t *testing.T) {
		tx := newApprovedTransaction(t)
		rc, err := valueobject.NewRejectionFromISO(valueobject.ISO_DO_NOT_HONOR)
		if err != nil {
			t.Fatalf("NewRejectionFromISO() error = %v", err)
		}
		if err := tx.Reject(rc); err == nil {
			t.Fatal("Reject() error = nil, want error")
		}
		if tx.State() != valueobject.StateApproved {
			t.Errorf("State() = %v, want unchanged %v", tx.State(), valueobject.StateApproved)
		}
	})
}

func TestTransaction_MarkIndeterminate(t *testing.T) {
	t.Run("success from processing", func(t *testing.T) {
		tx := newProcessingTransaction(t)
		if err := tx.MarkIndeterminate(); err != nil {
			t.Fatalf("MarkIndeterminate() error = %v", err)
		}
		if tx.State() != valueobject.StateIndeterminate {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateIndeterminate)
		}
		events := tx.DomainEvents()
		last := events[len(events)-1]
		if last.EventType() != "transaction.indeterminate" {
			t.Errorf("last event type = %q, want %q", last.EventType(), "transaction.indeterminate")
		}
	})

	t.Run("invalid transition from received", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.MarkIndeterminate(); err == nil {
			t.Fatal("MarkIndeterminate() error = nil, want error")
		}
	})
}

func TestTransaction_Reverse(t *testing.T) {
	t.Run("success from approved", func(t *testing.T) {
		tx := newApprovedTransaction(t)
		if err := tx.Reverse(); err != nil {
			t.Fatalf("Reverse() error = %v", err)
		}
		if tx.State() != valueobject.StateReversed {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateReversed)
		}
		events := tx.DomainEvents()
		last := events[len(events)-1]
		if last.EventType() != "transaction.reversed" {
			t.Errorf("last event type = %q, want %q", last.EventType(), "transaction.reversed")
		}
	})

	t.Run("success from indeterminate", func(t *testing.T) {
		tx := newIndeterminateTransaction(t)
		if err := tx.Reverse(); err != nil {
			t.Fatalf("Reverse() error = %v", err)
		}
		if tx.State() != valueobject.StateReversed {
			t.Errorf("State() = %v, want %v", tx.State(), valueobject.StateReversed)
		}
	})

	t.Run("invalid transition from received", func(t *testing.T) {
		tx := newValidTransaction(t)
		if err := tx.Reverse(); err == nil {
			t.Fatal("Reverse() error = nil, want error")
		}
	})
}

func TestTransaction_ClearDomainEvents(t *testing.T) {
	tx := newValidTransaction(t)
	if len(tx.DomainEvents()) == 0 {
		t.Fatal("DomainEvents() = empty, want at least the creation event")
	}
	tx.ClearDomainEvents()
	if len(tx.DomainEvents()) != 0 {
		t.Errorf("DomainEvents() after ClearDomainEvents() = %v, want empty", tx.DomainEvents())
	}
}
