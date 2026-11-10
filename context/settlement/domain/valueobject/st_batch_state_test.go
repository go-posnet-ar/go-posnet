package valueobject_test

import (
	"testing"

	"github.com/juantevez/go-posnet/context/settlement/domain/valueobject"
)

// ─── BatchState.IsTerminal ────────────────────────────────────────────────────

func TestBatchState_IsTerminal(t *testing.T) {
	tests := []struct {
		state valueobject.BatchState
		want  bool
	}{
		{valueobject.BatchStateOpen, false},
		{valueobject.BatchStatePendingClose, false},
		{valueobject.BatchStateClosed, false},
		{valueobject.BatchStateSubmitted, false},
		{valueobject.BatchStateSettled, true},
		{valueobject.BatchStateDisputed, false},
	}

	for _, tc := range tests {
		t.Run(string(tc.state), func(t *testing.T) {
			if got := tc.state.IsTerminal(); got != tc.want {
				t.Errorf("IsTerminal() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ─── BatchState.CanTransitionTo ───────────────────────────────────────────────

// allowedTransitions refleja la matriz esperada de transiciones válidas.
// Se usa para probar exhaustivamente las 6x6 combinaciones posibles.
var allowedTransitions = map[valueobject.BatchState]map[valueobject.BatchState]bool{
	valueobject.BatchStateOpen: {
		valueobject.BatchStatePendingClose: true,
	},
	valueobject.BatchStatePendingClose: {
		valueobject.BatchStateClosed:   true,
		valueobject.BatchStateDisputed: true,
	},
	valueobject.BatchStateClosed: {
		valueobject.BatchStateSubmitted: true,
		valueobject.BatchStateDisputed:  true,
	},
	valueobject.BatchStateSubmitted: {
		valueobject.BatchStateSettled:  true,
		valueobject.BatchStateDisputed: true,
	},
	valueobject.BatchStateDisputed: {
		valueobject.BatchStateSubmitted: true,
		valueobject.BatchStateOpen:      true,
		valueobject.BatchStateClosed:    true,
	},
	valueobject.BatchStateSettled: {},
}

func TestBatchState_CanTransitionTo_Matrix(t *testing.T) {
	allStates := []valueobject.BatchState{
		valueobject.BatchStateOpen,
		valueobject.BatchStatePendingClose,
		valueobject.BatchStateClosed,
		valueobject.BatchStateSubmitted,
		valueobject.BatchStateSettled,
		valueobject.BatchStateDisputed,
	}

	for _, from := range allStates {
		for _, to := range allStates {
			want := allowedTransitions[from][to]
			t.Run(string(from)+"->"+string(to), func(t *testing.T) {
				if got := from.CanTransitionTo(to); got != want {
					t.Errorf("CanTransitionTo() = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestBatchState_CanTransitionTo_UnknownState(t *testing.T) {
	unknown := valueobject.BatchState("UNKNOWN")
	if unknown.CanTransitionTo(valueobject.BatchStateOpen) {
		t.Error("CanTransitionTo() = true for an unknown source state, want false")
	}
	if valueobject.BatchStateOpen.CanTransitionTo(unknown) {
		t.Error("CanTransitionTo() = true for an unknown target state, want false")
	}
}

// ─── BatchState.String / ParseBatchState ──────────────────────────────────────

func TestBatchState_String(t *testing.T) {
	if got := valueobject.BatchStateOpen.String(); got != "OPEN" {
		t.Errorf("String() = %q, want %q", got, "OPEN")
	}
}

func TestParseBatchState(t *testing.T) {
	tests := []struct {
		input   string
		want    valueobject.BatchState
		wantErr bool
	}{
		{"OPEN", valueobject.BatchStateOpen, false},
		{"PENDING_CLOSE", valueobject.BatchStatePendingClose, false},
		{"CLOSED", valueobject.BatchStateClosed, false},
		{"SUBMITTED", valueobject.BatchStateSubmitted, false},
		{"SETTLED", valueobject.BatchStateSettled, false},
		{"DISPUTED", valueobject.BatchStateDisputed, false},
		{"open", "", true},
		{"", "", true},
		{"UNKNOWN", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := valueobject.ParseBatchState(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseBatchState(%q) error = nil, want error", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBatchState(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("ParseBatchState(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

// ─── BatchTransactionType ─────────────────────────────────────────────────────

func TestBatchTransactionType_String(t *testing.T) {
	if got := valueobject.BatchTxPurchase.String(); got != "PURCHASE" {
		t.Errorf("String() = %q, want %q", got, "PURCHASE")
	}
}

func TestParseBatchTransactionType(t *testing.T) {
	tests := []struct {
		input   string
		want    valueobject.BatchTransactionType
		wantErr bool
	}{
		{"PURCHASE", valueobject.BatchTxPurchase, false},
		{"REVERSAL", valueobject.BatchTxReversal, false},
		{"ADJUSTMENT", valueobject.BatchTxAdjustment, false},
		{"OFFLINE", "", true},
		{"purchase", "", true},
		{"", "", true},
		{"UNKNOWN", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := valueobject.ParseBatchTransactionType(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseBatchTransactionType(%q) error = nil, want error", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBatchTransactionType(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("ParseBatchTransactionType(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
