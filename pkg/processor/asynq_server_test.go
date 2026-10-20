package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestAsynqServerSharesWorkersBetweenEqualPriorityQueues checks a busy queue cannot keep an equal-priority queue waiting.
func TestAsynqServerSharesWorkersBetweenEqualPriorityQueues(t *testing.T) {
	mr := miniredis.RunT(t)
	opt := asynq.RedisClientOpt{Addr: mr.Addr()}

	client := asynq.NewClient(opt)
	defer client.Close()

	const perQueue = 40

	for i := 0; i < perQueue; i++ {
		for _, q := range []string{"agg", "simple"} {
			_, err := client.Enqueue(asynq.NewTask("work", []byte(q)), asynq.Queue(q))
			require.NoError(t, err)
		}
	}

	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	srv := newAsynqServer(opt, 1, map[string]int{"agg": 10, "simple": 10}, log)

	var (
		mu    sync.Mutex
		order []string
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc("work", func(_ context.Context, task *asynq.Task) error {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, string(task.Payload()))

		return nil
	})

	require.NoError(t, srv.Start(mux))
	defer srv.Shutdown()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(order) >= perQueue
	}, 30*time.Second, 10*time.Millisecond)

	mu.Lock()

	first := append([]string(nil), order[:perQueue]...)

	mu.Unlock()

	seen := map[string]int{}
	for _, q := range first {
		seen[q]++
	}

	require.Positive(t, seen["simple"], "simple waited for every agg task: %v", first)
	require.Positive(t, seen["agg"], "agg waited for every simple task: %v", first)
}
