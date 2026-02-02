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
	"github.com/sony/gobreaker/v2"
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

func TestBuffer_ConcurrentFlushes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var maxConcurrent atomic.Int32

		var currentConcurrent atomic.Int32

		buf := New(Config{
			MaxRows:              10,
			FlushInterval:        time.Hour,
			MaxConcurrentFlushes: 3,
		}, func(ctx context.Context, rows []int) error {
			cur := currentConcurrent.Add(1)

			// Track max concurrent flushes observed
			for {
				maxVal := maxConcurrent.Load()
				if cur <= maxVal || maxConcurrent.CompareAndSwap(maxVal, cur) {
					break
				}
			}

			// Simulate some work
			time.Sleep(10 * time.Millisecond)

			currentConcurrent.Add(-1)

			return nil
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		// Submit 50 rows in batches of 10 (triggers 5 flushes)
		for i := range 5 {
			go func(batch int) {
				_ = buf.Submit(context.Background(), make([]int, 10))
			}(i)
		}

		synctest.Wait()

		// Advance time to let flushes complete
		time.Sleep(100 * time.Millisecond)

		synctest.Wait()

		require.NoError(t, buf.Stop(context.Background()))

		// Max concurrent should be <= 3 (the limit)
		assert.LessOrEqual(t, maxConcurrent.Load(), int32(3))
		// And at least 1 flush should have happened
		assert.GreaterOrEqual(t, maxConcurrent.Load(), int32(1))
	})
}

func TestBuffer_FlushSemaphoreLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushStarted := make(chan struct{}, 10)
		blockFlush := make(chan struct{})

		var flushCount atomic.Int32

		buf := New(Config{
			MaxRows:              5,
			FlushInterval:        time.Hour,
			MaxConcurrentFlushes: 2,
		}, func(ctx context.Context, rows []int) error {
			flushStarted <- struct{}{}

			<-blockFlush

			flushCount.Add(1)

			return nil
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		// Submit 20 rows rapidly (triggers 4 flushes of 5 rows each)
		for i := range 4 {
			go func(batch int) {
				_ = buf.Submit(context.Background(), make([]int, 5))
			}(i)
		}

		synctest.Wait()

		// Only 2 flushes should start initially (semaphore limit)
		startedCount := 0

		for {
			select {
			case <-flushStarted:
				startedCount++
			default:
				goto done
			}
		}

	done:
		assert.Equal(t, 2, startedCount, "only 2 flushes should start due to semaphore")

		// Unblock flushes
		close(blockFlush)

		synctest.Wait()

		require.NoError(t, buf.Stop(context.Background()))

		// All 4 flushes should have completed
		assert.Equal(t, int32(4), flushCount.Load())
	})
}

func TestBuffer_GracefulShutdown_WaitsForInflight(t *testing.T) {
	flushStarted := make(chan struct{})
	blockFlush := make(chan struct{})

	var flushCompleted atomic.Bool

	buf := New(Config{
		MaxRows:              10,
		FlushInterval:        time.Hour,
		MaxConcurrentFlushes: 2,
	}, func(ctx context.Context, rows []int) error {
		close(flushStarted)
		<-blockFlush
		flushCompleted.Store(true)

		return nil
	}, newTestLogger())

	require.NoError(t, buf.Start(context.Background()))

	// Submit rows to trigger flush
	go func() {
		_ = buf.Submit(context.Background(), make([]int, 10))
	}()

	// Wait for flush to start
	<-flushStarted

	// Start Stop() in goroutine - it should block until flush completes
	stopDone := make(chan struct{})

	go func() {
		_ = buf.Stop(context.Background())

		close(stopDone)
	}()

	// Give Stop() time to potentially return early (it shouldn't)
	time.Sleep(10 * time.Millisecond)

	// Stop should still be blocked
	select {
	case <-stopDone:
		t.Fatal("Stop() returned before flush completed")
	default:
		// Expected - Stop is still waiting
	}

	// Unblock the flush
	close(blockFlush)

	// Now Stop should complete
	select {
	case <-stopDone:
		assert.True(t, flushCompleted.Load(), "flush should have completed")
	case <-time.After(time.Second):
		t.Fatal("Stop() did not complete after flush unblocked")
	}
}

func TestBuffer_DefaultMaxConcurrentFlushes(t *testing.T) {
	// Create buffer with Config{} (no MaxConcurrentFlushes set)
	buf := New(Config{}, func(ctx context.Context, rows []int) error {
		return nil
	}, newTestLogger())

	// Verify defaults to 10 (check via config)
	assert.Equal(t, 10, buf.config.MaxConcurrentFlushes)
	// Also verify the semaphore has capacity 10
	assert.Equal(t, 10, cap(buf.flushSem))
}

func TestBuffer_ConcurrentFlushErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var flushNum atomic.Int32

		expectedErr := errors.New("batch 2 error")

		buf := New(Config{
			MaxRows:              5,
			FlushInterval:        time.Hour,
			MaxConcurrentFlushes: 2,
		}, func(ctx context.Context, rows []int) error {
			num := flushNum.Add(1)

			// Batch 2 fails, others succeed
			if num == 2 {
				return expectedErr
			}

			return nil
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		errChan := make(chan error, 4)

		// Submit 4 batches of 5 rows each
		for i := range 4 {
			go func(batch int) {
				errChan <- buf.Submit(context.Background(), make([]int, 5))
			}(i)
		}

		synctest.Wait()

		require.NoError(t, buf.Stop(context.Background()))

		// Collect all errors
		var errs []error

		for range 4 {
			select {
			case err := <-errChan:
				errs = append(errs, err)
			default:
			}
		}

		// Should have exactly one error (from batch 2)
		errorCount := 0

		for _, err := range errs {
			if err != nil {
				errorCount++

				assert.ErrorIs(t, err, expectedErr)
			}
		}

		assert.Equal(t, 1, errorCount, "exactly one batch should have failed")
	})
}

func TestBuffer_CircuitBreakerTrips(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushErr := errors.New("clickhouse error")

		var flushCount atomic.Int32

		buf := New(Config{
			MaxRows:                   5,
			FlushInterval:             time.Hour,
			MaxConcurrentFlushes:      1, // Force sequential flushes for deterministic behavior
			CircuitBreakerMaxFailures: 3, // Trip after 3 consecutive failures
			CircuitBreakerTimeout:     time.Hour,
		}, func(ctx context.Context, rows []int) error {
			flushCount.Add(1)

			return flushErr
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		errChan := make(chan error, 5)

		// Submit 5 batches - first 3 should fail with flushErr, then circuit opens
		for i := range 5 {
			go func(batch int) {
				errChan <- buf.Submit(context.Background(), make([]int, 5))
			}(i)
		}

		synctest.Wait()

		// Collect errors
		var flushErrCount int

		var circuitOpenCount int

		for range 5 {
			select {
			case err := <-errChan:
				if errors.Is(err, flushErr) {
					flushErrCount++
				} else if errors.Is(err, gobreaker.ErrOpenState) {
					circuitOpenCount++
				}
			default:
			}
		}

		// With sequential flushes, should have exactly 3 flush errors before circuit tripped
		assert.Equal(t, 3, int(flushCount.Load()), "should have attempted exactly 3 flushes before circuit opened")
		assert.Equal(t, 3, flushErrCount, "should have 3 flush errors")
		assert.Equal(t, 2, circuitOpenCount, "should have 2 circuit open rejections")
	})
}

func TestBuffer_CircuitBreakerRejectsWhenOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flushErr := errors.New("clickhouse error")

		buf := New(Config{
			MaxRows:                   5,
			FlushInterval:             time.Hour,
			CircuitBreakerMaxFailures: 1, // Trip after 1 failure
			CircuitBreakerTimeout:     time.Hour,
		}, func(ctx context.Context, rows []int) error {
			return flushErr
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		errChan := make(chan error, 1)

		// First batch trips the circuit
		go func() {
			errChan <- buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		err := <-errChan
		require.ErrorIs(t, err, flushErr, "first batch should fail with flush error")

		// Second batch should be rejected immediately by circuit breaker
		go func() {
			errChan <- buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		err = <-errChan
		require.ErrorIs(t, err, gobreaker.ErrOpenState, "second batch should be rejected by open circuit")
	})
}

func TestBuffer_CircuitBreakerRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var shouldFail atomic.Bool
		shouldFail.Store(true)

		buf := New(Config{
			MaxRows:                   5,
			FlushInterval:             time.Hour,
			CircuitBreakerMaxFailures: 1,
			CircuitBreakerTimeout:     100 * time.Millisecond, // Short timeout for testing
		}, func(ctx context.Context, rows []int) error {
			if shouldFail.Load() {
				return errors.New("clickhouse error")
			}

			return nil
		}, newTestLogger())

		require.NoError(t, buf.Start(context.Background()))

		defer func() { _ = buf.Stop(context.Background()) }()

		errChan := make(chan error, 1)

		// First batch trips the circuit
		go func() {
			errChan <- buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		err := <-errChan
		require.Error(t, err, "first batch should fail")

		// Wait for circuit to transition to half-open
		time.Sleep(150 * time.Millisecond)

		synctest.Wait()

		// Fix the "database" before the trial request
		shouldFail.Store(false)

		// Next batch should succeed (half-open allows one trial)
		go func() {
			errChan <- buf.Submit(context.Background(), make([]int, 5))
		}()

		synctest.Wait()

		err = <-errChan
		require.NoError(t, err, "batch after recovery should succeed")
	})
}

func TestBuffer_CircuitBreakerDefaultConfig(t *testing.T) {
	buf := New(Config{}, func(ctx context.Context, rows []int) error {
		return nil
	}, newTestLogger())

	// Verify defaults
	assert.Equal(t, uint32(5), buf.config.CircuitBreakerMaxFailures)
	assert.Equal(t, 60*time.Second, buf.config.CircuitBreakerTimeout)
}
