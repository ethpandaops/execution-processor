package rowbuffer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	return log
}

func TestBuffer_FlushOnRowLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushCalled := make(chan []int, 1)
		buf := New(Config{MaxRows: 10, FlushInterval: time.Hour},
			func(ctx context.Context, rows []int) error {
				flushCalled <- rows

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		// Submit 10 rows - should trigger flush
		go func() {
			_ = buf.Submit(context.Background(), make([]int, 10))
		}()

		synctest.Wait()

		select {
		case rows := <-flushCalled:
			assert.Len(t, rows, 10)
		default:
			t.Fatal("flush not called")
		}
	})
}

func TestBuffer_FlushOnTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushCalled := make(chan []int, 1)
		buf := New(Config{MaxRows: 1000, FlushInterval: time.Second},
			func(ctx context.Context, rows []int) error {
				flushCalled <- rows

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		// Submit 5 rows (below threshold)
		go func() {
			_ = buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		// Advance fake clock by 1 second
		time.Sleep(time.Second)

		synctest.Wait()

		select {
		case rows := <-flushCalled:
			assert.Len(t, rows, 5)
		default:
			t.Fatal("timer flush not triggered")
		}
	})
}

func TestBuffer_ConcurrentSubmissions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var totalRows atomic.Int64

		buf := New(Config{MaxRows: 100, FlushInterval: time.Second},
			func(ctx context.Context, rows []int) error {
				totalRows.Add(int64(len(rows)))

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		// 50 goroutines each submitting 10 rows
		for range 50 {
			go func() {
				_ = buf.Submit(context.Background(), make([]int, 10))
			}()
		}

		synctest.Wait()

		// Advance time to trigger timer flush for any remaining rows
		time.Sleep(time.Second)

		synctest.Wait()

		require.NoError(t, buf.Stop(context.Background()))
		assert.Equal(t, int64(500), totalRows.Load())
	})
}

func TestBuffer_ErrorPropagation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expectedErr := errors.New("clickhouse error")
		buf := New(Config{MaxRows: 10, FlushInterval: time.Hour},
			func(ctx context.Context, rows []int) error {
				return expectedErr
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		errChan := make(chan error, 3)

		// 3 goroutines submit, triggering flush
		for range 3 {
			go func() {
				errChan <- buf.Submit(context.Background(), make([]int, 4))
			}()
		}

		synctest.Wait()

		// All 3 should receive the error (2 submits trigger flush at 10 rows)
		receivedCount := 0

		for range 3 {
			select {
			case err := <-errChan:
				require.ErrorIs(t, err, expectedErr)

				receivedCount++
			default:
				// Some may still be pending
			}
		}

		// At least 2 should have received errors (the flush was triggered)
		require.GreaterOrEqual(t, receivedCount, 2)
	})
}

func TestBuffer_GracefulShutdown(t *testing.T) {
	flushCalled := make(chan []int, 1)
	buf := New(Config{MaxRows: 1000, FlushInterval: time.Hour},
		func(ctx context.Context, rows []int) error {
			flushCalled <- rows

			return nil
		}, newTestLogger())

	require.NoError(t, buf.Start(context.Background()))

	// Submit rows below threshold in a goroutine
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		_ = buf.Submit(context.Background(), make([]int, 50))
	}()

	// Give time for submit to start
	time.Sleep(10 * time.Millisecond)

	// Stop should flush remaining rows
	require.NoError(t, buf.Stop(context.Background()))

	// Wait for submit goroutine to finish
	wg.Wait()

	select {
	case rows := <-flushCalled:
		assert.Len(t, rows, 50)
	default:
		t.Fatal("shutdown flush not called")
	}
}

func TestBuffer_ContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buf := New(Config{MaxRows: 1000, FlushInterval: time.Hour},
			func(ctx context.Context, rows []int) error {
				// Simulate slow flush
				time.Sleep(10 * time.Second)

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		errChan := make(chan error, 1)

		go func() {
			errChan <- buf.Submit(ctx, make([]int, 5))
		}()

		synctest.Wait()

		// Advance time past the timeout
		time.Sleep(20 * time.Millisecond)

		synctest.Wait()

		select {
		case err := <-errChan:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		default:
			t.Fatal("expected context deadline error")
		}
	})
}

func TestBuffer_EmptySubmit(t *testing.T) {
	buf := New(Config{MaxRows: 10, FlushInterval: time.Hour},
		func(ctx context.Context, rows []int) error {
			t.Fatal("flush should not be called for empty submit")

			return nil
		}, newTestLogger())

	require.NoError(t, buf.Start(context.Background()))

	defer func() { _ = buf.Stop(context.Background()) }()

	// Empty submit should return immediately without error
	err := buf.Submit(context.Background(), []int{})
	require.NoError(t, err)
}

func TestBuffer_NotStarted(t *testing.T) {
	buf := New(Config{MaxRows: 10, FlushInterval: time.Hour},
		func(ctx context.Context, rows []int) error {
			return nil
		}, newTestLogger())

	// Submit without starting should fail
	err := buf.Submit(context.Background(), []int{1, 2, 3})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not started")
}

func TestBuffer_LenAndWaiterCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blockFlush := make(chan struct{})
		buf := New(Config{MaxRows: 1000, FlushInterval: time.Hour},
			func(ctx context.Context, rows []int) error {
				<-blockFlush

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() {
			close(blockFlush)

			_ = buf.Stop(context.Background())
		}()

		// Initially empty
		assert.Equal(t, 0, buf.Len())
		assert.Equal(t, 0, buf.WaiterCount())

		// Submit some rows
		go func() {
			_ = buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		// Should have rows and waiters pending
		assert.Equal(t, 5, buf.Len())
		assert.Equal(t, 1, buf.WaiterCount())
	})
}

func TestBuffer_FlushTriggers(t *testing.T) {
	tests := []struct {
		name          string
		maxRows       int
		flushInterval time.Duration
		submitRows    int
		waitTime      time.Duration
		expectFlush   bool
	}{
		{"size trigger", 10, time.Hour, 10, 0, true},
		{"size trigger partial", 10, time.Hour, 5, 0, false},
		{"timer trigger", 1000, 100 * time.Millisecond, 5, 150 * time.Millisecond, true},
		{"neither trigger", 1000, time.Hour, 5, 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				flushed := atomic.Bool{}
				buf := New(Config{MaxRows: tc.maxRows, FlushInterval: tc.flushInterval},
					func(ctx context.Context, rows []int) error {
						flushed.Store(true)

						return nil
					}, newTestLogger())

				require.NoError(t, buf.Start(context.Background()))

				defer func() { _ = buf.Stop(context.Background()) }()

				go func() {
					_ = buf.Submit(context.Background(), make([]int, tc.submitRows))
				}()

				synctest.Wait()

				if tc.waitTime > 0 {
					time.Sleep(tc.waitTime)

					synctest.Wait()
				}

				assert.Equal(t, tc.expectFlush, flushed.Load())
			})
		})
	}
}

func TestBuffer_MultipleFlushes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushCount := atomic.Int32{}

		var totalRows atomic.Int64

		buf := New(Config{MaxRows: 10, FlushInterval: time.Hour},
			func(ctx context.Context, rows []int) error {
				flushCount.Add(1)
				totalRows.Add(int64(len(rows)))

				return nil
			}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		// Submit 25 rows - should trigger 2 flushes (10+10), leaving 5
		for range 25 {
			go func() {
				_ = buf.Submit(context.Background(), make([]int, 1))
			}()
		}

		synctest.Wait()

		require.NoError(t, buf.Stop(context.Background()))

		// Should have flushed all 25 rows across multiple flushes
		assert.Equal(t, int64(25), totalRows.Load())
		assert.GreaterOrEqual(t, flushCount.Load(), int32(2))
	})
}
