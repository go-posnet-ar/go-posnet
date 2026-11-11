package command_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/application/port"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/outbox"
)

// missingReasons devuelve posnet_settlement_admin_missing_reason por action.
func missingReasons(t *testing.T, m map[string]metricdata.Metrics) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	data, ok := m["posnet_settlement_admin_missing_reason"]
	if !ok {
		return out
	}
	for _, dp := range data.Data.(metricdata.Sum[int64]).DataPoints {
		action, _ := dp.Attributes.Value(attribute.Key("action"))
		out[action.AsString()] = dp.Value
	}
	return out
}

// El motivo, si llega, queda en la auditoría.
func TestForceClose_RecordsTheReason(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	expectEventTx(pool, 0)

	cmd := port.ForceCloseCommand{BatchID: "b-1", OperatorID: "op-1", Reason: "terminal roto, ticket 42"}
	if err := h.ForceClose(context.Background(), cmd); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	if len(repo.auditEntries) != 1 || repo.auditEntries[0].Reason != "terminal roto, ticket 42" {
		t.Errorf("audit = %+v, want the reason", repo.auditEntries)
	}
}

// Sin SETTLEMENT_REQUIRE_REASON, un pedido sin motivo se acepta y se cuenta.
func TestMissingReason_IsCountedWhenOptional(t *testing.T) {
	reader := newTestReader(t)
	metrics, err := command.NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}
	repo := &fakeBatchRepo{findByIDResult: newOpenBatch(t, domain.NewTerminalID(), domain.NewMerchantID())}
	h, pool := newEventAdmin(t, repo, &fakePublisher{}, 2.5)
	h.SetMetrics(metrics)
	expectEventTx(pool, 0)

	if err := h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "b-1", OperatorID: "op-1", Reason: "  "}); err != nil {
		t.Fatalf("ForceClose() error = %v", err)
	}
	got := missingReasons(t, collectMetrics(t, reader))
	// Las otras series nacen en 0: la transición se puede seguir por action desde el arranque.
	want := map[string]int64{"FORCE_CLOSE": 1, "RESUBMIT": 0, "SETTLE": 0}
	for action, v := range want {
		if got[action] != v {
			t.Errorf("missing reason[%s] = %d, want %d (all: %v)", action, got[action], v, got)
		}
	}
}

// Con SETTLEMENT_REQUIRE_REASON=true, las tres operaciones rechazan un pedido sin motivo antes de
// cargar el lote.
func TestMissingReason_IsRejectedWhenRequired(t *testing.T) {
	ops := map[string]func(*command.AdminHandler) error{
		"force-close": func(h *command.AdminHandler) error {
			return h.ForceClose(context.Background(), port.ForceCloseCommand{BatchID: "b-1", OperatorID: "op-1"})
		},
		"resubmit": func(h *command.AdminHandler) error {
			return h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "b-1", OperatorID: "op-1"})
		},
		"settle": func(h *command.AdminHandler) error {
			return h.SettleBatch(context.Background(), port.SettleBatchCommand{BatchID: "b-1", OperatorID: "op-1"})
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			repo := &fakeBatchRepo{findByIDErr: errors.New("must not load the batch")}
			h := command.NewAdminHandler(repo, &fakeProcessor{})
			h.RequireReason(true)

			err := op(h)
			var ve *pkgerrors.ValidationError
			if !errors.As(err, &ve) || !strings.Contains(err.Error(), "reason") {
				t.Errorf("error = %v, want a reason ValidationError", err)
			}
		})
	}
}

// Un resubmit que encuentra el envío ya aceptado conserva el motivo del operador.
func TestResubmit_RecoveredKeepsTheOperatorReason(t *testing.T) {
	repo := &fakeBatchRepo{findByIDResult: newClosedBatch(t)}
	pool := newMockPool(t)
	h := command.NewAdminHandler(repo, &fakeProcessor{})
	h.ConfigureSettlement(pool, repo, &fakePublisher{}, outbox.NewStore(idempotencySchema), 2.5)
	expectEventTx(pool, 0)
	h.SetBatchSubmissions(&fakeSubmissions{previous: "conf-1"})

	err := h.ResubmitBatch(context.Background(), port.ResubmitBatchCommand{BatchID: "batch-1", OperatorID: "op-1", Reason: "procesador caído ayer"})
	if err != nil {
		t.Fatalf("ResubmitBatch() error = %v", err)
	}
	if len(repo.auditEntries) != 1 {
		t.Fatalf("audit = %+v, want one entry", repo.auditEntries)
	}
	reason := repo.auditEntries[0].Reason
	if !strings.HasPrefix(reason, "procesador caído ayer") || !strings.Contains(reason, "already accepted") {
		t.Errorf("reason = %q, want the operator reason and the recovery", reason)
	}
	if repo.auditEntries[0].Action != repository.AuditActionResubmit {
		t.Errorf("action = %s", repo.auditEntries[0].Action)
	}
}
