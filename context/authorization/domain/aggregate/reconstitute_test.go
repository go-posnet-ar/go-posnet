package aggregate_test

import (
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/authorization/domain/aggregate"
	"github.com/juantevez/go-posnet/context/authorization/domain/valueobject"
)

func TestReconstitute_CopiesAllFields(t *testing.T) {
	params := baseReconstituteParams(t)
	fd, err := valueobject.NewFraudDecision(20, valueobject.FraudDecisionApprove, []string{"R1"})
	if err != nil {
		t.Fatalf("NewFraudDecision() error = %v", err)
	}
	params.FraudDecision = fd
	params.State = valueobject.StateApproved
	authorizedAt := params.ReceivedAt.Add(time.Minute)
	params.AuthorizedAt = &authorizedAt

	tx := aggregate.Reconstitute(params)

	if !tx.ID().Equals(params.ID) {
		t.Errorf("ID() = %v, want %v", tx.ID(), params.ID)
	}
	if !tx.TerminalID().Equals(params.TerminalID) {
		t.Errorf("TerminalID() = %v, want %v", tx.TerminalID(), params.TerminalID)
	}
	if !tx.MerchantID().Equals(params.MerchantID) {
		t.Errorf("MerchantID() = %v, want %v", tx.MerchantID(), params.MerchantID)
	}
	if !tx.Amount().Equals(params.Amount) {
		t.Errorf("Amount() = %v, want %v", tx.Amount(), params.Amount)
	}
	if !tx.STAN().Equals(params.STAN) {
		t.Errorf("STAN() = %v, want %v", tx.STAN(), params.STAN)
	}
	if tx.PAN() != params.PAN {
		t.Errorf("PAN() = %v, want %v", tx.PAN(), params.PAN)
	}
	if tx.EntryMode() != params.EntryMode {
		t.Errorf("EntryMode() = %v, want %v", tx.EntryMode(), params.EntryMode)
	}
	if tx.State() != params.State {
		t.Errorf("State() = %v, want %v", tx.State(), params.State)
	}
	if tx.FraudDecision().Score != params.FraudDecision.Score || tx.FraudDecision().Decision != params.FraudDecision.Decision {
		t.Errorf("FraudDecision() = %+v, want %+v", tx.FraudDecision(), params.FraudDecision)
	}
	if tx.EMVDataBase64() != params.EMVDataBase64 {
		t.Errorf("EMVDataBase64() = %q, want %q", tx.EMVDataBase64(), params.EMVDataBase64)
	}
	if string(tx.ISO8583Raw()) != string(params.ISO8583Raw) {
		t.Errorf("ISO8583Raw() = %v, want %v", tx.ISO8583Raw(), params.ISO8583Raw)
	}
	if !tx.ReceivedAt().Equal(params.ReceivedAt) {
		t.Errorf("ReceivedAt() = %v, want %v", tx.ReceivedAt(), params.ReceivedAt)
	}
	if tx.AuthorizedAt() == nil || !tx.AuthorizedAt().Equal(authorizedAt) {
		t.Errorf("AuthorizedAt() = %v, want %v", tx.AuthorizedAt(), authorizedAt)
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

	// Reconstitute no debe emitir eventos de dominio.
	if len(tx.DomainEvents()) != 0 {
		t.Errorf("DomainEvents() = %v, want empty", tx.DomainEvents())
	}
}

func TestReconstitute_RejectedAt(t *testing.T) {
	params := baseReconstituteParams(t)
	params.State = valueobject.StateRejected
	rejectedAt := params.ReceivedAt.Add(30 * time.Second)
	params.RejectedAt = &rejectedAt

	tx := aggregate.Reconstitute(params)

	if tx.RejectedAt() == nil || !tx.RejectedAt().Equal(rejectedAt) {
		t.Errorf("RejectedAt() = %v, want %v", tx.RejectedAt(), rejectedAt)
	}
	if tx.AuthorizedAt() != nil {
		t.Errorf("AuthorizedAt() = %v, want nil", tx.AuthorizedAt())
	}
}

func TestReconstitute_AuthCode(t *testing.T) {
	t.Run("nil auth code stays unset", func(t *testing.T) {
		params := baseReconstituteParams(t)
		tx := aggregate.Reconstitute(params)
		if tx.AuthCode() != nil {
			t.Errorf("AuthCode() = %v, want nil", tx.AuthCode())
		}
	})

	t.Run("valid auth code is set", func(t *testing.T) {
		ac := "AB1234"
		params := baseReconstituteParams(t)
		params.AuthCode = &ac

		tx := aggregate.Reconstitute(params)

		if tx.AuthCode() == nil {
			t.Fatal("AuthCode() = nil, want non-nil")
		}
		if tx.AuthCode().String() != ac {
			t.Errorf("AuthCode().String() = %q, want %q", tx.AuthCode().String(), ac)
		}
	})

	t.Run("invalid auth code is silently discarded", func(t *testing.T) {
		bad := "not-a-valid-code"
		params := baseReconstituteParams(t)
		params.AuthCode = &bad

		tx := aggregate.Reconstitute(params)

		if tx.AuthCode() != nil {
			t.Errorf("AuthCode() = %v, want nil for invalid stored value %q", tx.AuthCode(), bad)
		}
	})
}

func TestReconstitute_RejectionCode(t *testing.T) {
	t.Run("nil rejection code stays unset", func(t *testing.T) {
		params := baseReconstituteParams(t)
		tx := aggregate.Reconstitute(params)
		if tx.RejectionCode() != nil {
			t.Errorf("RejectionCode() = %v, want nil", tx.RejectionCode())
		}
	})

	t.Run("fraud source", func(t *testing.T) {
		code := "FRAUD_REJECTED"
		source := string(valueobject.SourceFraud)
		params := baseReconstituteParams(t)
		params.RejectionCode = &code
		params.RejectionSource = &source

		tx := aggregate.Reconstitute(params)

		rc := tx.RejectionCode()
		if rc == nil {
			t.Fatal("RejectionCode() = nil, want non-nil")
		}
		if rc.Source() != valueobject.SourceFraud {
			t.Errorf("RejectionCode().Source() = %v, want %v", rc.Source(), valueobject.SourceFraud)
		}
	})

	t.Run("every source survives the round trip", func(t *testing.T) {
		cases := []struct {
			code    string
			source  valueobject.RejectionSource
			capture bool
		}{
			{"51", valueobject.SourceAcquirer, false},
			{"41", valueobject.SourceAcquirer, true},
			{"CARD_BLOCKED", valueobject.SourceBlocklist, true},
			{"UNKNOWN_RESPONSE", valueobject.SourceValidation, false},
			{"FRAUD_REJECTED", valueobject.SourceFraud, false},
			{"TIMEOUT", valueobject.SourceTimeout, false},
		}
		for _, tc := range cases {
			code, source := tc.code, string(tc.source)
			params := baseReconstituteParams(t)
			params.RejectionCode = &code
			params.RejectionSource = &source

			rc := aggregate.Reconstitute(params).RejectionCode()
			if rc == nil {
				t.Fatalf("%s: RejectionCode() = nil", tc.source)
			}
			if rc.Code() != tc.code || rc.Source() != tc.source {
				t.Errorf("RejectionCode() = %v, want %s(%s)", rc, tc.source, tc.code)
			}
			if rc.RequiresCardCapture() != tc.capture {
				t.Errorf("%v RequiresCardCapture() = %v, want %v", rc, rc.RequiresCardCapture(), tc.capture)
			}
		}
	})

	t.Run("missing or unknown source reads as acquirer", func(t *testing.T) {
		code, unknown := "05", "SOMETHING_ELSE"
		for _, source := range []*string{nil, &unknown} {
			params := baseReconstituteParams(t)
			params.RejectionCode = &code
			params.RejectionSource = source

			rc := aggregate.Reconstitute(params).RejectionCode()
			if rc == nil || rc.Source() != valueobject.SourceAcquirer {
				t.Errorf("RejectionCode() = %v, want source %v", rc, valueobject.SourceAcquirer)
			}
		}
	})

	t.Run("timeout source", func(t *testing.T) {
		code := "TIMEOUT"
		source := string(valueobject.SourceTimeout)
		params := baseReconstituteParams(t)
		params.RejectionCode = &code
		params.RejectionSource = &source

		tx := aggregate.Reconstitute(params)

		rc := tx.RejectionCode()
		if rc == nil {
			t.Fatal("RejectionCode() = nil, want non-nil")
		}
		if rc.Source() != valueobject.SourceTimeout {
			t.Errorf("RejectionCode().Source() = %v, want %v", rc.Source(), valueobject.SourceTimeout)
		}
	})

	t.Run("falls back to ISO acquirer code when source is nil", func(t *testing.T) {
		code := valueobject.ISO_DO_NOT_HONOR
		params := baseReconstituteParams(t)
		params.RejectionCode = &code

		tx := aggregate.Reconstitute(params)

		rc := tx.RejectionCode()
		if rc == nil {
			t.Fatal("RejectionCode() = nil, want non-nil")
		}
		if rc.Source() != valueobject.SourceAcquirer {
			t.Errorf("RejectionCode().Source() = %v, want %v", rc.Source(), valueobject.SourceAcquirer)
		}
		if rc.Code() != code {
			t.Errorf("RejectionCode().Code() = %q, want %q", rc.Code(), code)
		}
	})

	t.Run("falls back to ISO acquirer code when source is ACQUIRER", func(t *testing.T) {
		code := valueobject.ISO_INSUFFICIENT_FUNDS
		source := string(valueobject.SourceAcquirer)
		params := baseReconstituteParams(t)
		params.RejectionCode = &code
		params.RejectionSource = &source

		tx := aggregate.Reconstitute(params)

		rc := tx.RejectionCode()
		if rc == nil {
			t.Fatal("RejectionCode() = nil, want non-nil")
		}
		if rc.Code() != code {
			t.Errorf("RejectionCode().Code() = %q, want %q", rc.Code(), code)
		}
	})
}

