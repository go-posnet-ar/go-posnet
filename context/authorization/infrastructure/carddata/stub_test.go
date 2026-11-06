package carddata_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/juantevez/go-posnet/context/authorization/domain/service"
	"github.com/juantevez/go-posnet/context/authorization/infrastructure/carddata"
	"github.com/juantevez/go-posnet/pkg/domain"
)

func TestStub_IsStablePerToken(t *testing.T) {
	token, err := domain.NewCardToken(strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("NewCardToken() error = %v", err)
	}
	stub := carddata.NewStub()
	first, err := stub.Resolve(context.Background(), token)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	second, _ := stub.Resolve(context.Background(), token)
	if first != second || first.PAN == "" || len(first.ExpiryYYMM) != 4 {
		t.Errorf("first = %q, second = %q", first.PAN, second.PAN)
	}
	if none, err := stub.Resolve(context.Background(), domain.CardToken{}); err != nil || none.PAN == "" {
		t.Errorf("without a token = %v", err)
	}
}

// El PAN no llega a un log por accidente, con ningún formato.
func TestCardData_IsRedacted(t *testing.T) {
	card := service.CardData{PAN: "4242424242424242", ExpiryYYMM: "3012"}
	var logged strings.Builder
	slog.New(slog.NewTextHandler(&logged, nil)).Info("x", slog.Any("card", card))

	for name, out := range map[string]string{
		"%v": fmt.Sprintf("%v", card), "%+v": fmt.Sprintf("%+v", card), "%#v": fmt.Sprintf("%#v", card), "slog": logged.String(),
	} {
		if strings.Contains(out, "4242") {
			t.Errorf("%s exposes the PAN: %s", name, out)
		}
	}
}
