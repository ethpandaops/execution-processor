package cryo

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/rowbuffer"
)

// rowMeta carries the per-batch literals every dataset appends to its rows.
type rowMeta struct {
	network string
	updated time.Time
}

// sinkDeps are the shared dependencies a dataset needs to buffer and insert.
type sinkDeps struct {
	log                 logrus.FieldLogger
	clickhouse          clickhouse.ClientInterface
	dataset             *Dataset
	table               string
	network             string
	processor           string
	bufferMaxRows       int
	bufferFlushInterval time.Duration
}

// sink owns one dataset's row buffer and ClickHouse mapping. A single cryo
// invocation produces rows for several datasets, so a processor holds one sink
// per dataset in its group and flushes each on its own thresholds.
type sink interface {
	Dataset() string
	Table() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	// Consume decodes a parquet table and submits its rows for insertion,
	// blocking until they are flushed. It returns the number of rows submitted.
	Consume(ctx context.Context, t *decode.Table, meta rowMeta) (int, error)
	// ValidateSchema checks the target table's columns against the mapping.
	ValidateSchema(ctx context.Context) error
}

// columnar is a dataset's ch-go column block.
type columnar[R any] interface {
	Append(R) error
	Input() proto.Input
	Reset()
	Rows() int
}

// decodeFunc turns a decoded parquet table into rows, preserving file order.
type decodeFunc[R any] func(t *decode.Table, meta rowMeta) ([]R, error)

// datasetSink is the single implementation of sink; datasets differ only in
// their row type, decoder and column block.
type datasetSink[R any] struct {
	deps    sinkDeps
	log     logrus.FieldLogger
	buffer  *rowbuffer.Buffer[R]
	decode  decodeFunc[R]
	newCols func() columnar[R]
}

func newDatasetSink[R any](deps sinkDeps, dec decodeFunc[R], newCols func() columnar[R]) sink {
	log := deps.log.WithField("dataset", deps.dataset.Name)

	s := &datasetSink[R]{
		deps:    deps,
		log:     log,
		decode:  dec,
		newCols: newCols,
	}

	s.buffer = rowbuffer.New(
		rowbuffer.Config{
			MaxRows:       deps.bufferMaxRows,
			FlushInterval: deps.bufferFlushInterval,
			Network:       deps.network,
			Processor:     deps.processor,
			Table:         deps.table,
		},
		s.flush,
		log,
	)

	return s
}

func (s *datasetSink[R]) Dataset() string { return s.deps.dataset.Name }

func (s *datasetSink[R]) Table() string { return s.deps.table }

func (s *datasetSink[R]) Start(ctx context.Context) error {
	return s.buffer.Start(ctx)
}

func (s *datasetSink[R]) Stop(ctx context.Context) error {
	return s.buffer.Stop(ctx)
}

func (s *datasetSink[R]) Consume(ctx context.Context, t *decode.Table, meta rowMeta) (int, error) {
	rows, err := s.decode(t, meta)
	if err != nil {
		return 0, fmt.Errorf("decode %s: %w", s.deps.dataset.Name, err)
	}

	if len(rows) == 0 {
		return 0, nil
	}

	if err := s.buffer.Submit(ctx, rows); err != nil {
		return 0, fmt.Errorf("buffer %s: %w", s.deps.dataset.Name, err)
	}

	return len(rows), nil
}

// flush maps buffered rows into a ch-go column block and inserts it.
func (s *datasetSink[R]) flush(ctx context.Context, rows []R) error {
	if len(rows) == 0 {
		return nil
	}

	insertCtx, cancel := context.WithTimeout(ctx, tracker.DefaultClickHouseTimeout)
	defer cancel()

	cols := s.newCols()

	for i := range rows {
		if err := cols.Append(rows[i]); err != nil {
			return fmt.Errorf("append %s row %d: %w", s.deps.dataset.Name, i, err)
		}
	}

	input := cols.Input()

	if err := s.deps.clickhouse.Do(insertCtx, ch.Query{
		Body:  input.Into(s.deps.table),
		Input: input,
	}); err != nil {
		common.ClickHouseInsertsRows.WithLabelValues(
			s.deps.network, s.deps.processor, s.deps.table, "failed", "",
		).Add(float64(len(rows)))

		return fmt.Errorf("insert into %s: %w", s.deps.table, err)
	}

	common.ClickHouseInsertsRows.WithLabelValues(
		s.deps.network, s.deps.processor, s.deps.table, "success", "",
	).Add(float64(len(rows)))

	return nil
}

// ValidateSchema compares the target table against the column block this
// dataset writes, so that schema drift fails at boot rather than at the first
// insert of a block that happens to exercise the drifted column.
//
// The check runs in both directions. A column the mapping does not write is as
// dangerous as one it writes wrongly: ClickHouse fills the gap with the type
// default and the insert succeeds, so the column silently becomes zero for
// every row.
func (s *datasetSink[R]) ValidateSchema(ctx context.Context) error {
	actual, err := describeTable(ctx, s.deps.clickhouse, s.deps.table)
	if err != nil {
		return err
	}

	written := make(map[string]struct{}, len(actual))

	for _, col := range s.newCols().Input() {
		written[col.Name] = struct{}{}

		got, ok := actual[col.Name]
		if !ok {
			return fmt.Errorf("table %s has no column %q", s.deps.table, col.Name)
		}

		want := string(col.Data.Type())
		if got != want {
			return fmt.Errorf("table %s column %q is %s, mapping writes %s", s.deps.table, col.Name, got, want)
		}
	}

	missing := make([]string, 0, len(actual))

	for name := range actual {
		if _, ok := written[name]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)

		return fmt.Errorf("table %s has columns the %s mapping never writes, which would be silently defaulted: %s",
			s.deps.table, s.deps.dataset.Name, strings.Join(missing, ", "))
	}

	return nil
}
