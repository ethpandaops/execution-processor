package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeAddressAppearances(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "address_appearances", decodeAddressAppearances)
	require.NoError(t, err)
	require.Len(t, rows, 2309)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, "0x0511b790404361753f71d85f71714f05bdf5459255960856bd8fcb37e0f7af5a", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", first.Address)
	require.Equal(t, "miner_fee", first.Relationship)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	// One address appears four times in this transaction under four
	// relationships, each a distinct row.
	for i, relationship := range []string{"call_from", "erc20_transfer_from", "erc20_transfer_from_to", "tx_from"} {
		row := rows[i+1]
		require.Equal(t, first.TransactionHash, row.TransactionHash)
		require.Equal(t, uint32(i+2), row.InternalIndex)
		require.Equal(t, "0x68770d610c6cb38493416eca5fdbd7e053c12c86", row.Address)
		require.Equal(t, relationship, row.Relationship)
	}

	// The seventh row closes the first transaction; the eighth starts the next.
	require.Equal(t, first.TransactionHash, rows[6].TransactionHash)
	require.Equal(t, uint32(7), rows[6].InternalIndex)
	require.Equal(t, "tx_to", rows[6].Relationship)
	require.Equal(t, "0x0637faa52c470dd7ea0c434db9cc82b7f0f059fd1123c8e914de5d434db0d1c3", rows[7].TransactionHash)
	require.Equal(t, uint32(1), rows[7].InternalIndex)

	last := rows[2308]
	require.Equal(t, "0xffbd484e846b720325ef6fa8302cb2b73b4df87a9412e8a298e25c1876daa6ec", last.TransactionHash)
	require.Equal(t, "0x4656e33ce80748db2b2816e00eb115c0f6183f63", last.Address)
	require.Equal(t, "tx_from", last.Relationship)
}

// TestDecodeAddressAppearancesInternalIndex pins the one column that carries
// information nothing else in the row does: 1047 of the 2309 rows repeat a
// (transaction, address, relationship) triple, so without internal_index the
// ReplacingMergeTree sort key would collapse them.
func TestDecodeAddressAppearancesInternalIndex(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "address_appearances", decodeAddressAppearances)
	require.NoError(t, err)

	type triple struct {
		hash         string
		address      string
		relationship string
	}

	type sortKey struct {
		hash  string
		index uint32
	}

	var (
		next    = make(map[string]uint32)
		triples = make(map[triple]struct{})
		keys    = make(map[sortKey]struct{})
	)

	for i, r := range rows {
		next[r.TransactionHash]++
		require.Equal(t, next[r.TransactionHash], r.InternalIndex, "row %d", i)

		triples[triple{r.TransactionHash, r.Address, r.Relationship}] = struct{}{}
		keys[sortKey{r.TransactionHash, r.InternalIndex}] = struct{}{}
	}

	require.Len(t, next, 137)
	require.Len(t, triples, 1262)
	require.Len(t, keys, len(rows), "internal_index must make every row's sort key unique")
}

func TestAddressAppearancesColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "address_appearances", decodeAddressAppearances)
	require.NoError(t, err)

	cols := newAddressAppearanceColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_address_appearances", cols.Input())
}
