package cryo

import (
	"errors"
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// ErrIncompleteCoverage reports that cryo returned successfully but its output
// does not cover the blocks that were asked for.
//
// This is the failure the per-block ledger otherwise cannot see. A node serving
// empty traces for a range it has pruned, or a cryo that skips a chunk and
// still exits zero, produces an empty parquet that is indistinguishable from a
// block with genuinely no rows — and the block is then checkpointed complete,
// so nothing ever revisits it. Production lost whole blocks of storage_reads
// exactly this way.
var ErrIncompleteCoverage = errors.New("cryo output does not cover the requested blocks")

// verifyCoverage checks a group's output against the range it was asked for.
func verifyCoverage(g *Group, tables map[string]*decode.Table, from, to uint64) error {
	expected := to - from + 1

	for _, ds := range g.collected() {
		table, ok := tables[ds.Name]
		if !ok {
			return fmt.Errorf("%w: dataset %s is absent", ErrIncompleteCoverage, ds.Name)
		}

		if err := verifyBlockNumbers(ds.Name, table, from, to); err != nil {
			return err
		}
	}

	// blocks is the one dataset with a knowable row count: exactly one per
	// block, whatever the block contains. Every group collects it for this
	// reason, so the assertion is always available.
	table, ok := tables[blocksDataset.Name]
	if !ok {
		return fmt.Errorf("%w: blocks is absent", ErrIncompleteCoverage)
	}

	if got := table.Rows(); got < 0 || uint64(got) != expected {
		return fmt.Errorf("%w: blocks has %d rows, expected %d", ErrIncompleteCoverage, got, expected)
	}

	return nil
}

// verifyBlockNumbers checks that every row belongs to the requested range, so
// that output written for the wrong blocks is never attributed to these.
func verifyBlockNumbers(dataset string, table *decode.Table, from, to uint64) error {
	if table.Rows() == 0 {
		return nil
	}

	col, err := table.Int("block_number")
	if err != nil {
		return fmt.Errorf("dataset %s: %w", dataset, err)
	}

	for i := range table.Rows() {
		if col.IsNull(i) {
			return fmt.Errorf("%w: dataset %s row %d has no block number", ErrIncompleteCoverage, dataset, i)
		}

		if got := col.Int(i); got < from || got > to {
			return fmt.Errorf("%w: dataset %s row %d is block %d, outside %d-%d",
				ErrIncompleteCoverage, dataset, i, got, from, to)
		}
	}

	return nil
}
