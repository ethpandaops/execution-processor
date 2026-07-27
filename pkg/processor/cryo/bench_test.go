package cryo

import (
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// benchBlock is the fixture the benchmarks run against. 23000026 is the
// heaviest of the three, so it is a closer analogue of a real mainnet block
// than the lighter 23000000.
const benchBlock = "b23000026"

// benchSink exposes the stages of a sink separately so a benchmark can
// attribute cost. The signatures are deliberately non-generic so one interface
// covers every dataset's datasetSink[R].
type benchSink interface {
	mapOnly(t *decode.Table, meta rowMeta) (int, error)
	encodeOnly(t *decode.Table, meta rowMeta) error
}

func (s *datasetSink[R]) mapOnly(t *decode.Table, meta rowMeta) (int, error) {
	rows, err := s.decode(t, meta)

	return len(rows), err
}

func (s *datasetSink[R]) encodeOnly(t *decode.Table, meta rowMeta) error {
	rows, err := s.decode(t, meta)
	if err != nil {
		return err
	}

	cols := s.acquireCols()
	defer s.releaseCols(cols)

	for i := range rows {
		if err := cols.Append(rows[i]); err != nil {
			return err
		}
	}

	_ = cols.Input()

	return nil
}

// resetChecker exposes a fill/Reset/refill comparison without leaking the
// row type, so one assertion covers all sixteen datasets.
type resetChecker interface {
	checkReset(t *decode.Table, meta rowMeta) (names []string, fresh, reused []int, err error)
}

func (s *datasetSink[R]) checkReset(t *decode.Table, meta rowMeta) ([]string, []int, []int, error) {
	rows, err := s.decode(t, meta)
	if err != nil {
		return nil, nil, nil, err
	}

	fill := func(cols columnar[R]) ([]string, []int, error) {
		for i := range rows {
			if appendErr := cols.Append(rows[i]); appendErr != nil {
				return nil, nil, appendErr
			}
		}

		input := cols.Input()
		names := make([]string, 0, len(input))
		counts := make([]int, 0, len(input))

		for _, col := range input {
			names = append(names, col.Name)
			counts = append(counts, col.Data.Rows())
		}

		return names, counts, nil
	}

	names, fresh, err := fill(s.newCols())
	if err != nil {
		return nil, nil, nil, err
	}

	recycled := s.newCols()

	if _, _, fillErr := fill(recycled); fillErr != nil {
		return nil, nil, nil, fillErr
	}

	recycled.Reset()

	_, reused, err := fill(recycled)
	if err != nil {
		return nil, nil, nil, err
	}

	return names, fresh, reused, nil
}

func benchLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	return log
}

func benchSinkFor(ds *Dataset) benchSink {
	s, ok := ds.newSink(sinkDeps{
		log:                 benchLogger(),
		dataset:             ds,
		table:               ds.Table,
		network:             "mainnet",
		processor:           "cryo_bench",
		bufferMaxRows:       DefaultBufferMaxRows,
		bufferFlushInterval: DefaultBufferFlushInterval,
	}).(benchSink)
	if !ok {
		panic("datasetSink does not implement benchSink")
	}

	return s
}

func benchFixturePath(tb testing.TB, dataset string) string {
	tb.Helper()

	matches, err := filepath.Glob(filepath.Join("testdata", benchBlock, "ethereum__"+dataset+"__*.parquet"))
	if err != nil || len(matches) != 1 {
		tb.Fatalf("expected one %s fixture, got %v (%v)", dataset, matches, err)
	}

	return matches[0]
}

func benchTable(tb testing.TB, dataset string) *decode.Table {
	tb.Helper()

	table, err := decode.File(benchFixturePath(tb, dataset))
	if err != nil {
		tb.Fatal(err)
	}

	return table
}

// BenchmarkDecodeFile measures parquet decode alone, per dataset.
func BenchmarkDecodeFile(b *testing.B) {
	for _, ds := range datasets {
		path := benchFixturePath(b, ds.Name)
		rows := benchTable(b, ds.Name).Rows()

		b.Run(ds.Name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := decode.File(path); err != nil {
					b.Fatal(err)
				}
			}

			reportRowRate(b, rows)
		})
	}
}

// BenchmarkMapRows measures turning a decoded table into row structs, which is
// where internal_index and every null and hex decision happens.
func BenchmarkMapRows(b *testing.B) {
	for _, ds := range datasets {
		table := benchTable(b, ds.Name)
		sink := benchSinkFor(ds)

		b.Run(ds.Name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := sink.mapOnly(table, fixtureMeta); err != nil {
					b.Fatal(err)
				}
			}

			reportRowRate(b, table.Rows())
		})
	}
}

// BenchmarkEncodeColumns measures map plus the ch-go column block build, which
// together are the work done on every flush.
func BenchmarkEncodeColumns(b *testing.B) {
	for _, ds := range datasets {
		table := benchTable(b, ds.Name)
		sink := benchSinkFor(ds)

		b.Run(ds.Name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if err := sink.encodeOnly(table, fixtureMeta); err != nil {
					b.Fatal(err)
				}
			}

			reportRowRate(b, table.Rows())
		})
	}
}

// BenchmarkBlockPipeline measures everything a task does except the ClickHouse
// round trip: decode, map and encode all 16 datasets of one block.
func BenchmarkBlockPipeline(b *testing.B) {
	paths := make([]string, 0, len(datasets))
	sinks := make([]benchSink, 0, len(datasets))
	total := 0

	for _, ds := range datasets {
		paths = append(paths, benchFixturePath(b, ds.Name))
		sinks = append(sinks, benchSinkFor(ds))
		total += benchTable(b, ds.Name).Rows()
	}

	b.ReportAllocs()

	for b.Loop() {
		for i, path := range paths {
			table, err := decode.File(path)
			if err != nil {
				b.Fatal(err)
			}

			if err := sinks[i].encodeOnly(table, fixtureMeta); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ReportMetric(float64(b.Elapsed().Milliseconds())/float64(b.N), "ms/block")
	reportRowRate(b, total)
}

func reportRowRate(b *testing.B, rows int) {
	b.Helper()

	if rows == 0 || b.Elapsed() == 0 {
		return
	}

	b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
}
