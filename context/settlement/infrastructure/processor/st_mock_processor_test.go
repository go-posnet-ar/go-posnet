package processor

import (
	"context"
	"testing"
	"time"

	"github.com/juantevez/go-posnet/context/settlement/domain/aggregate"
	"github.com/juantevez/go-posnet/context/settlement/domain/service"
	"github.com/juantevez/go-posnet/pkg/domain"
)

var _ service.SettlementProcessor = (*MockProcessor)(nil)

func TestMockProcessor_Submit(t *testing.T) {
	b, err := aggregate.NewSettlementBatch(domain.NewTerminalID(), domain.NewMerchantID(), time.Now(), "ARS")
	if err != nil {
		t.Fatalf("NewSettlementBatch() error = %v", err)
	}

	got, err := NewMockProcessor().Submit(context.Background(), b)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if want := "MOCK-" + b.ID(); got != want {
		t.Errorf("Submit() = %q, want %q", got, want)
	}
}
