package cryo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDecodeBlocks(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "blocks", decodeBlocks)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	r := rows[0]
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, "0xe368c631c74a82c3043e6d44c4bef6e6139a6501b39c7700c2552554d10e6c3b", r.BlockHash)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", r.Author.Value)
	require.Equal(t, uint64(11210841), r.GasUsed.Value)
	require.Equal(t, uint64(45000000), r.GasLimit)
	require.Equal(t, uint64(239712557), r.BaseFeePerGas.Value)
	require.Equal(t, "mainnet", r.MetaNetworkName)

	// extra_data is stored twice: verbatim hex, and the raw bytes it encodes.
	require.Equal(t, "0xe29ca82051756173617220287175617361722e77696e2920e29ca8", r.ExtraData.Value)
	require.Equal(t, "✨ Quasar (quasar.win) ✨", r.ExtraDataString.Value)

	require.Equal(t, time.Unix(1753492931, 0).UTC(), r.BlockDateTime)
}

func TestBlockColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "blocks", decodeBlocks)
	require.NoError(t, err)

	cols := newBlockColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_block", cols.Input())
}
