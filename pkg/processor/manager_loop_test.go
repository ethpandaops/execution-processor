package processor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// countingProcessor counts ProcessNextBlock calls; with hold set, each call blocks until hold closes or ctx ends.
type countingProcessor struct {
	tracker.BlockProcessor

	calls atomic.Int64
	hold  chan struct{}
}

func (p *countingProcessor) ProcessNextBlock(ctx context.Context) error {
	p.calls.Add(1)

	if p.hold != nil {
		select {
		case <-p.hold:
		case <-ctx.Done():
		}
	}

	return nil
}

func newLoopTestManager(t *testing.T, processors map[string]tracker.BlockProcessor) *Manager {
	t.Helper()

	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	t.Cleanup(func() { _ = rc.Close() })

	stateManager, err := state.NewManager(log, &state.Config{
		Storage: state.StorageConfig{Config: clickhouse.Config{Addr: "127.0.0.1:1"}, Table: "loop_test"},
	})
	require.NoError(t, err)

	return &Manager{
		log: log,
		config: &Config{
			Interval:               10 * time.Millisecond,
			Mode:                   tracker.FORWARDS_MODE,
			MaxProcessQueueSize:    1000,
			BackpressureHysteresis: 0.8,
			StaleBlockDetection:    StaleBlockDetectionConfig{CheckInterval: time.Hour},
			GapDetection:           GapDetectionConfig{ScanInterval: time.Hour},
		},
		pool:                ethereum.NewPoolWithNodes(log, "loop_test", nil, nil),
		state:               stateManager,
		processors:          processors,
		redisClient:         rc,
		network:             &ethereum.Network{Name: "loop_test"},
		isLeader:            true,
		queueHighWaterMarks: map[string]int{},
	}
}

// TestBlockProcessingDoesNotWaitOnSlowProcessor checks one processor's long ProcessNextBlock never holds up another's.
func TestBlockProcessingDoesNotWaitOnSlowProcessor(t *testing.T) {
	slow := &countingProcessor{hold: make(chan struct{})}
	fast := &countingProcessor{}

	m := newLoopTestManager(t, map[string]tracker.BlockProcessor{"slow": slow, "fast": fast})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		m.runBlockProcessing(ctx, stop)
	}()

	require.Eventually(t, func() bool { return fast.calls.Load() >= 20 }, 10*time.Second, 10*time.Millisecond,
		"fast processor made %d calls while slow was busy", fast.calls.Load())
	require.Equal(t, int64(1), slow.calls.Load())

	close(slow.hold)

	require.Eventually(t, func() bool { return slow.calls.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)

	close(stop)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("block processing did not stop")
	}
}

// TestBlockProcessingStopsWhileProcessorIsBusy checks stop and cancellation end every processor loop.
func TestBlockProcessingStopsWhileProcessorIsBusy(t *testing.T) {
	slow := &countingProcessor{hold: make(chan struct{})}
	fast := &countingProcessor{}

	m := newLoopTestManager(t, map[string]tracker.BlockProcessor{"slow": slow, "fast": fast})

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		m.runBlockProcessing(ctx, stop)
	}()

	require.Eventually(t, func() bool { return slow.calls.Load() == 1 && fast.calls.Load() > 0 }, 10*time.Second, 10*time.Millisecond)

	close(stop)
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("block processing did not stop")
	}

	calls := fast.calls.Load()

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, calls, fast.calls.Load(), "fast processor kept running after stop")
}
