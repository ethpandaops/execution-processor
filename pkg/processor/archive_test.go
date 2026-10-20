package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/common"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, c.Write(&m))

	return m.GetCounter().GetValue()
}

// TestFailingTaskIsArchivedAndReported runs a task that always fails until asynq archives it.
func TestFailingTaskIsArchivedAndReported(t *testing.T) {
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}

	logger, hook := test.NewNullLogger()

	const (
		queue    = "archive-test"
		taskType = "transaction_structlog_process_forwards"
		txHash   = "0xa121687c0fd7d2a98b7f07ed0df8114402645c0dabe92723c0b1bc32fb061b59"
	)

	archived := common.TasksArchived.WithLabelValues(queue, taskType)
	before := counterValue(t, archived)

	server := asynq.NewServer(opt, asynq.Config{
		Concurrency:    1,
		Queues:         map[string]int{queue: 1},
		Logger:         logger,
		LogLevel:       asynq.FatalLevel,
		RetryDelayFunc: func(int, error, *asynq.Task) time.Duration { return 10 * time.Millisecond },
		ErrorHandler:   archiveErrorHandler(logger),
	})

	attempts := make(chan struct{}, 10)
	mux := asynq.NewServeMux()
	mux.HandleFunc(taskType, func(context.Context, *asynq.Task) error {
		attempts <- struct{}{}

		return errors.New("trace response exceeds the size limit")
	})

	require.NoError(t, server.Start(mux))
	t.Cleanup(server.Shutdown)

	client := asynq.NewClient(opt)

	t.Cleanup(func() { _ = client.Close() })

	payload := []byte(`{"block_number":26036079,"transaction_hash":"` + txHash + `","transaction_index":236}`)
	_, err := client.Enqueue(asynq.NewTask(taskType, payload), asynq.Queue(queue), asynq.MaxRetry(2), asynq.TaskID("structlog:"+txHash))
	require.NoError(t, err)

	inspector := asynq.NewInspector(opt)

	t.Cleanup(func() { _ = inspector.Close() })

	require.Eventually(t, func() bool {
		info, infoErr := inspector.GetQueueInfo(queue)

		return infoErr == nil && info.Archived == 1
	}, 30*time.Second, 50*time.Millisecond)

	require.Len(t, attempts, 3, "one attempt plus MaxRetry retries")
	require.Equal(t, before+1, counterValue(t, archived))

	var entry *logrus.Entry

	for _, e := range hook.AllEntries() {
		if e.Message == "Task archived after exhausting its retries" {
			entry = e
		}
	}

	require.NotNil(t, entry, "archiving is logged")
	require.Equal(t, logrus.ErrorLevel, entry.Level)
	require.Equal(t, txHash, entry.Data["transaction_hash"])
	require.Equal(t, "26036079", entry.Data["block_number"])
	require.Equal(t, "structlog:"+txHash, entry.Data["task_id"])
}
