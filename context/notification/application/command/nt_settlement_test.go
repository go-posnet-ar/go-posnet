package command_test

import (
	"context"
	"errors"
	"testing"

	"github.com/juantevez/go-posnet/context/notification/application/command"
	"github.com/juantevez/go-posnet/context/notification/application/port"
	"github.com/juantevez/go-posnet/context/notification/domain/aggregate"
	"github.com/juantevez/go-posnet/context/notification/domain/valueobject"
	"github.com/juantevez/go-posnet/pkg/domain"
	pkgerrors "github.com/juantevez/go-posnet/pkg/errors"
	"github.com/juantevez/go-posnet/pkg/natsutil"
)

func validSettlementCmd() port.NotifySettlementCompletedCommand {
	return port.NotifySettlementCompletedCommand{
		EventID: domain.NewTransactionID().String(), MerchantID: domain.NewMerchantID().String(),
		SettlementDate: "2026-01-01", Currency: "ARS", Revision: 2,
		TotalBatches: 2, TotalAmount: 15000, AdjustmentAmount: -400, NetAmount: 14625, MDRPercent: 2.5,
		DeltaBatches: 1, DeltaAmount: 5000, DeltaNetAmount: 4875, CompletedAt: "2026-01-02T10:00:00Z",
	}
}

func newSettlementHandler(t *testing.T, repo *fakeNotificationRepo, claimed bool) *command.NotifyHandler {
	t.Helper()
	pool := newMockPool(t)
	if claimed {
		expectClaimed(pool, 1)
	}
	return command.NewNotifyHandler(repo, &fakeTerminalNotifier{}, &fakeWebhookDispatcher{httpStatus: 200}, &fakeEventPublisher{},
		natsutil.NewIdempotencyStore(nil, idempotencySchema), pool)
}

// El comercio recibe por webhook el neto acumulado del día y lo que agregó la revisión.
func TestNotifySettlementCompleted_Success(t *testing.T) {
	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	cmd := validSettlementCmd()
	if err := newSettlementHandler(t, repo, true).NotifySettlementCompleted(context.Background(), cmd); err != nil {
		t.Fatalf("NotifySettlementCompleted() error = %v", err)
	}
	waitForSaves(t, repo.saveSignal, 2)

	n := repo.lastSaved()
	r := n.Receipt()
	if n.Channel() != valueobject.ChannelWebhook || r.Result != valueobject.ResultSettlementCompleted || r.AmountCents != 14625 || r.Currency != "ARS" {
		t.Errorf("notification = %s, receipt = %+v", n.Channel(), r)
	}
	if s := r.Settlement; s == nil || s.Revision != 2 || s.GrossAmount != 15000 || s.DeltaBatches != 1 || s.DeltaNetAmount != 4875 || s.SettlementDate != "2026-01-01" {
		t.Errorf("settlement = %+v", r.Settlement)
	}
	if n.MerchantID().String() != cmd.MerchantID {
		t.Errorf("merchant = %s, want %s", n.MerchantID(), cmd.MerchantID)
	}
}

// El ID sintético es el mismo para la misma revisión, y distinto para otra.
func TestNotifySettlementCompleted_SyntheticIDPerRevision(t *testing.T) {
	ids := map[int]string{}
	for _, rev := range []int{1, 1, 2} {
		repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
		cmd := validSettlementCmd()
		cmd.MerchantID, cmd.Revision = "6f1b3c8e-0000-4000-8000-000000000001", rev
		if err := newSettlementHandler(t, repo, true).NotifySettlementCompleted(context.Background(), cmd); err != nil {
			t.Fatalf("NotifySettlementCompleted() error = %v", err)
		}
		waitForSaves(t, repo.saveSignal, 2)
		id := repo.lastSaved().TransactionID().String()
		if prev, ok := ids[rev]; ok && prev != id {
			t.Errorf("revision %d: id = %s, want the same %s", rev, id, prev)
		}
		ids[rev] = id
	}
	if ids[1] == ids[2] {
		t.Errorf("revisions 1 and 2 share the id %s", ids[1])
	}
}

// Una revisión ya notificada no se repite, aunque llegue con otro event_id.
func TestNotifySettlementCompleted_AlreadyNotifiedIsSkipped(t *testing.T) {
	repo := &fakeNotificationRepo{byTransaction: []*aggregate.Notification{newPendingNotification(t, valueobject.ChannelWebhook)}}
	if err := newSettlementHandler(t, repo, false).NotifySettlementCompleted(context.Background(), validSettlementCmd()); err != nil {
		t.Fatalf("NotifySettlementCompleted() error = %v", err)
	}
	if repo.savedCount() != 0 {
		t.Errorf("saved count = %d, want 0", repo.savedCount())
	}
}

// Un evento anterior a las revisiones es la revisión 1, con las diferencias iguales al acumulado.
func TestNotifySettlementCompleted_EventWithoutRevision(t *testing.T) {
	repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
	cmd := validSettlementCmd()
	cmd.Revision, cmd.DeltaBatches, cmd.DeltaAmount, cmd.DeltaNetAmount = 0, 0, 0, 0
	if err := newSettlementHandler(t, repo, true).NotifySettlementCompleted(context.Background(), cmd); err != nil {
		t.Fatalf("NotifySettlementCompleted() error = %v", err)
	}
	waitForSaves(t, repo.saveSignal, 2)
	if s := repo.lastSaved().Receipt().Settlement; s.Revision != 1 || s.DeltaBatches != 2 || s.DeltaNetAmount != 14625 {
		t.Errorf("settlement = %+v, want revision 1 with deltas equal to the totals", s)
	}
}

func TestNotifySettlementCompleted_Errors(t *testing.T) {
	var ve *pkgerrors.ValidationError
	for name, mutate := range map[string]func(*port.NotifySettlementCompletedCommand){
		"invalid merchant": func(c *port.NotifySettlementCompletedCommand) { c.MerchantID = "nope" },
		"no date":          func(c *port.NotifySettlementCompletedCommand) { c.SettlementDate = "" },
		"no currency":      func(c *port.NotifySettlementCompletedCommand) { c.Currency = "" },
	} {
		cmd := validSettlementCmd()
		mutate(&cmd)
		if err := newSettlementHandler(t, &fakeNotificationRepo{}, false).NotifySettlementCompleted(context.Background(), cmd); !errors.As(err, &ve) {
			t.Errorf("%s: error = %v, want ValidationError", name, err)
		}
	}
	repo := &fakeNotificationRepo{byTransactionErr: errors.New("db down")}
	if err := newSettlementHandler(t, repo, false).NotifySettlementCompleted(context.Background(), validSettlementCmd()); err == nil || errors.As(err, &ve) {
		t.Errorf("lookup failure: error = %v, want a transient error", err)
	}
}

// Un lote en disputa que vuelve a CLOSED se informa como revisión, con el detalle del lote.
func TestNotifyBatchClosed_RevisionCarriesTheBatchDetail(t *testing.T) {
	for _, tc := range []struct {
		revision bool
		want     string
	}{{false, valueobject.ResultBatchClosed}, {true, valueobject.ResultBatchRevised}} {
		repo := &fakeNotificationRepo{saveSignal: make(chan struct{}, 10)}
		cmd := validBatchClosedCmd(t)
		cmd.Discrepancies, cmd.ReconciledWith, cmd.Trigger, cmd.Revision = 2, "TERMINAL", "resolve_dispute", tc.revision
		if err := newSettlementHandler(t, repo, true).NotifyBatchClosed(context.Background(), cmd); err != nil {
			t.Fatalf("NotifyBatchClosed() error = %v", err)
		}
		waitForSaves(t, repo.saveSignal, 2)
		r := repo.lastSaved().Receipt()
		if r.Result != tc.want || r.Batch == nil || r.Batch.Discrepancies != 2 || r.Batch.TotalCount != 10 ||
			r.Batch.BatchDate != "2026-01-01" || r.Batch.ReconciledWith != "TERMINAL" || r.Batch.Trigger != "resolve_dispute" {
			t.Errorf("revision %v: receipt = %+v, batch = %+v", tc.revision, r, r.Batch)
		}
	}
}
