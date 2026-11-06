package command_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/juantevez/go-posnet/context/settlement/application/command"
	"github.com/juantevez/go-posnet/context/settlement/domain/repository"
)

// newTestReader instala un MeterProvider con lector manual mientras dura el test.
func newTestReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// batchesByState devuelve posnet_settlement_batches por valor del label state.
func batchesByState(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	m, ok := collectMetrics(t, reader)["posnet_settlement_batches"]
	if !ok {
		return out
	}
	for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
		state, _ := dp.Attributes.Value(attribute.Key("state"))
		out[state.AsString()] += dp.Value
	}
	return out
}

func newMetricsHandler(t *testing.T, repo *fakeBatchRepo) (*command.BatchHandler, *sdkmetric.ManualReader) {
	t.Helper()
	reader := newTestReader(t)
	metrics, err := command.NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics() error = %v", err)
	}
	pool := newMockPool(t)
	expectClaimed(pool, 1)
	h := newHandler(pool, repo, &fakePublisher{}, &fakeProcessor{confirmationID: "conf-1"})
	h.SetMetrics(metrics)
	return h, reader
}

func TestProcessBatchClose_MetricCountsDisputedBatchOnce(t *testing.T) {
	repo := &fakeBatchRepo{findOpenResult: closableFoundBatch(t)}
	h, reader := newMetricsHandler(t, repo)
	cmd := validProcessCloseCmd()
	cmd.TerminalCount = 5

	if err := h.ProcessBatchClose(context.Background(), cmd); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}

	got := batchesByState(t, reader)
	if got["DISPUTED"] != 1 || got["CLOSED"] != 0 {
		t.Errorf("batches by state = %v, want only DISPUTED=1", got)
	}
}

func TestProcessBatchClose_MetricCountsClosedBatch(t *testing.T) {
	repo := &fakeBatchRepo{findOpenResult: closableFoundBatch(t)}
	h, reader := newMetricsHandler(t, repo)

	if err := h.ProcessBatchClose(context.Background(), validProcessCloseCmd()); err != nil {
		t.Fatalf("ProcessBatchClose() error = %v", err)
	}

	got := batchesByState(t, reader)
	if got["CLOSED"] != 1 || got["DISPUTED"] != 0 {
		t.Errorf("batches by state = %v, want only CLOSED=1", got)
	}
}

type fakeAttentionCounter struct {
	counts       repository.BatchAttentionCounts
	err          error
	today        time.Time
	closedBefore time.Time
}

func (f *fakeAttentionCounter) CountBatchesNeedingAttention(_ context.Context, today, closedBefore time.Time) (repository.BatchAttentionCounts, error) {
	f.today, f.closedBefore = today, closedBefore
	return f.counts, f.err
}

func attentionByReason(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	m, ok := collectMetrics(t, reader)["posnet_settlement_batches_attention"]
	if !ok {
		return out
	}
	for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
		reason, _ := dp.Attributes.Value(attribute.Key("reason"))
		out[reason.AsString()] = dp.Value
	}
	return out
}

func TestBatchAttentionGauge_ReportsEveryReason(t *testing.T) {
	reader := newTestReader(t)
	counter := &fakeAttentionCounter{counts: repository.BatchAttentionCounts{Disputed: 2, OpenOverdue: 1}}
	loc := time.FixedZone("BA", -3*60*60)
	if err := command.RegisterBatchAttentionGauge(counter, loc); err != nil {
		t.Fatalf("RegisterBatchAttentionGauge() error = %v", err)
	}

	before := time.Now()
	got := attentionByReason(t, reader)

	// closed_unsent en 0 también se informa: sin la serie, la alerta no podría resolverse.
	want := map[string]int64{"disputed": 2, "closed_unsent": 0, "open_overdue": 1}
	if len(got) != len(want) {
		t.Fatalf("attention = %v, want %v", got, want)
	}
	for reason, value := range want {
		if got[reason] != value {
			t.Errorf("attention[%s] = %d, want %d", reason, got[reason], value)
		}
	}

	y, m, d := before.In(loc).Date()
	if wantToday := time.Date(y, m, d, 0, 0, 0, 0, time.UTC); !counter.today.Equal(wantToday) {
		t.Errorf("today = %v, want business date %v", counter.today, wantToday)
	}
	if counter.closedBefore.After(before.Add(-time.Hour + time.Minute)) {
		t.Errorf("closedBefore = %v, want about one hour ago", counter.closedBefore)
	}
}

func TestBatchAttentionGauge_QueryErrorOmitsSeries(t *testing.T) {
	reader := newTestReader(t)
	counter := &fakeAttentionCounter{err: errors.New("db down")}
	if err := command.RegisterBatchAttentionGauge(counter, time.UTC); err != nil {
		t.Fatalf("RegisterBatchAttentionGauge() error = %v", err)
	}

	if got := attentionByReason(t, reader); len(got) != 0 {
		t.Errorf("attention = %v, want no series when the query fails", got)
	}
}
