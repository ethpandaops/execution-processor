package cryo

import (
	"runtime"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// TestPeakHeapUnderConcurrency reports the resident cost of processing blocks
// in parallel, which is what sizes a replica. Allocation churn is not the same
// number: most of a block's allocation is released as soon as its columns are
// handed to ClickHouse.
//
// It reports rather than asserts, because the figure depends on the machine and
// a threshold here would be noise. Run with -v to see it.
func TestPeakHeapUnderConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of megabytes")
	}

	paths := make([]string, 0, len(datasets))
	sinks := make([]benchSink, 0, len(datasets))

	for _, ds := range datasets {
		paths = append(paths, benchFixturePath(t, ds.Name))
		sinks = append(sinks, benchSinkFor(ds))
	}

	for _, workers := range []int{1, 4, 8, 16, 32} {
		runtime.GC()

		var before runtime.MemStats

		runtime.ReadMemStats(&before)

		var group errgroup.Group

		group.SetLimit(workers)

		// Each unit of work is one whole block: decode all sixteen parquet
		// files and build every column block, exactly as a task does.
		for range workers * 4 {
			group.Go(func() error {
				for i, path := range paths {
					table, err := decode.File(path)
					if err != nil {
						return err
					}

					if err := sinks[i].encodeOnly(table, fixtureMeta); err != nil {
						return err
					}
				}

				return nil
			})
		}

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}

		var peak runtime.MemStats

		runtime.ReadMemStats(&peak)

		// HeapAlloc straight after the workload counts garbage that simply has
		// not been collected yet. Collecting first is what distinguishes the
		// memory a replica actually needs from the memory it churned through.
		runtime.GC()

		var live runtime.MemStats

		runtime.ReadMemStats(&live)

		t.Logf("workers=%-3d heap_sys=%6.1f MiB  live_after_gc=%6.1f MiB  churned=%6.1f MiB  gc_cycles=%d",
			workers,
			float64(peak.HeapSys)/(1<<20),
			float64(live.HeapAlloc)/(1<<20),
			float64(peak.TotalAlloc-before.TotalAlloc)/(1<<20),
			peak.NumGC-before.NumGC,
		)
	}
}
