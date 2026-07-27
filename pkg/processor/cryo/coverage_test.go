package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

func fixtureTables(t *testing.T, block string, datasets ...string) map[string]*decode.Table {
	t.Helper()

	tables := make(map[string]*decode.Table, len(datasets))
	for _, ds := range datasets {
		tables[ds] = fixtureTable(t, block, ds)
	}

	return tables
}

func TestVerifyCoverageAcceptsRealOutput(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks", "transactions", "logs", "state_diffs")

	tables := fixtureTables(t, "b23000000",
		"blocks", "transactions", "logs", "balance_diffs", "nonce_diffs", "storage_diffs")

	require.NoError(t, verifyCoverage(group, tables, 23000000, 23000000))
}

// TestVerifyCoverageAcceptsGenuinelyEmptyDataset guards against the check
// becoming so strict it rejects normal output: erc721_transfers really does
// have no rows at block 23000000.
func TestVerifyCoverageAcceptsGenuinelyEmptyDataset(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks", "erc721_transfers")
	tables := fixtureTables(t, "b23000000", "blocks", "erc721_transfers")

	require.Zero(t, tables["erc721_transfers"].Rows())
	require.NoError(t, verifyCoverage(group, tables, 23000000, 23000000))
}

// TestVerifyCoverageRejectsSilentlyEmptyBlocks is the case that lost whole
// blocks of storage_reads in production: cryo succeeds, writes an empty
// parquet, and nothing distinguishes that from a block with no rows.
func TestVerifyCoverageRejectsSilentlyEmptyBlocks(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks")

	// b23000000's blocks fixture holds block 23000000, so asking for a
	// different block is the same shape as cryo returning the wrong window.
	tables := fixtureTables(t, "b23000000", "blocks")

	err := verifyCoverage(group, tables, 23000001, 23000001)

	require.ErrorIs(t, err, ErrIncompleteCoverage)
	require.Contains(t, err.Error(), "outside 23000001-23000001")
}

func TestVerifyCoverageRejectsShortBlockCount(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks")
	tables := fixtureTables(t, "b23000000", "blocks")

	// A range request that comes back with one block is a skipped chunk.
	err := verifyCoverage(group, tables, 23000000, 23000009)

	require.ErrorIs(t, err, ErrIncompleteCoverage)
	require.Contains(t, err.Error(), "blocks has 1 rows, expected 10")
}

func TestVerifyCoverageRejectsAbsentDataset(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks", "logs")
	tables := fixtureTables(t, "b23000000", "blocks")

	err := verifyCoverage(group, tables, 23000000, 23000000)

	require.ErrorIs(t, err, ErrIncompleteCoverage)
	require.Contains(t, err.Error(), "dataset logs is absent")
}

// TestVerifyCoverageCanaryCoversGroupsThatDoNotWriteBlocks is the blind spot
// that made this necessary. cryo asked for a block it cannot serve writes an
// empty parquet and exits zero, so a group of datasets that can all legitimately
// be empty has nothing to distinguish success from silence. Every group
// therefore collects blocks, whose row count is knowable, even when it does not
// write it.
func TestVerifyCoverageCanaryCoversGroupsThatDoNotWriteBlocks(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "logs", "erc20_transfers", "erc721_transfers")

	require.NotNil(t, group.Canary, "a group without blocks must collect it as a canary")
	require.Contains(t, group.Datatypes, "blocks", "the canary must be asked of cryo")
	require.NotContains(t, datasetNames(group.Datasets), "blocks", "the canary must not be written")

	tables := fixtureTables(t, "b23000000", "logs", "erc20_transfers", "erc721_transfers", "blocks")
	require.NoError(t, verifyCoverage(group, tables, 23000000, 23000000))

	// Every dataset empty, which is exactly what a node that cannot serve the
	// range produces, and what the old pipeline recorded as success.
	delete(tables, "blocks")
	require.ErrorIs(t, verifyCoverage(group, tables, 23000000, 23000000), ErrIncompleteCoverage)
}

func TestNewGroupDoesNotDuplicateBlocksCanary(t *testing.T) {
	t.Parallel()

	group := testGroup(t, "blocks", "transactions")

	require.Nil(t, group.Canary, "a group that already writes blocks needs no canary")
	require.Equal(t, []string{"blocks", "transactions"}, group.Datatypes)
}

func datasetNames(datasets []*Dataset) []string {
	names := make([]string, 0, len(datasets))
	for _, ds := range datasets {
		names = append(names, ds.Name)
	}

	return names
}

// TestGroupRequiresGasLimitColumn pins the one column cryo omits unless asked.
// Losing it does not fail: ClickHouse writes zero for every row, which is how
// staging silently held gas_limit = 0.
func TestGroupRequiresGasLimitColumn(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"gas_limit"}, testGroup(t, "blocks").RequiredColumns())

	// Reached through the canary too, so no group can lose it.
	require.Equal(t, []string{"gas_limit"}, testGroup(t, "logs").RequiredColumns())
}

// TestColumnBlocksResetCompletely guards the column pool. Reset was dead code
// until blocks started being recycled; a Reset that misses a column now leaves
// the previous batch's rows in place, so the next flush builds a block whose
// columns disagree on length.
func TestColumnBlocksResetCompletely(t *testing.T) {
	t.Parallel()

	for _, ds := range datasets {
		t.Run(ds.Name, func(t *testing.T) {
			t.Parallel()

			table := fixtureTable(t, "b23000026", ds.Name)
			if table.Rows() == 0 {
				t.Skipf("%s has no rows at this block", ds.Name)
			}

			checker, ok := ds.newSink(sinkDeps{
				log: benchLogger(), dataset: ds, table: ds.Table, network: "mainnet",
			}).(resetChecker)
			require.True(t, ok)

			names, fresh, reused, err := checker.checkReset(table, fixtureMeta)
			require.NoError(t, err)
			require.NotEmpty(t, fresh)

			for i := range fresh {
				require.Equal(t, fresh[i], reused[i],
					"column %q holds %d rows after Reset+refill but %d in a fresh block: Reset is incomplete",
					names[i], reused[i], fresh[i])
			}
		})
	}
}
