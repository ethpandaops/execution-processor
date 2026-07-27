package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeNonceDiffs(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "nonce_diffs", decodeNonceDiffs)
	require.NoError(t, err)
	require.Len(t, rows, 139)

	r := rows[0]
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", r.Address)
	require.Equal(t, uint64(12963272), r.FromValue)
	require.Equal(t, uint64(12963273), r.ToValue)
	require.Equal(t, "mainnet", r.MetaNetworkName)
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)

	require.Equal(t, uint64(1), rows[1].TransactionIndex)
	require.Equal(t, uint32(1), rows[1].InternalIndex)
	require.Equal(t, uint64(12963273), rows[1].FromValue)

	// A transaction that bumps a second account's nonce produces a second row
	// under the same hash.
	second := rows[36]
	require.Equal(t, "0xe99d668cb5ea5a2c06bf5d7ff12a75b8f7331d21e8ffeaf00c43156b57b9a7e0", second.TransactionHash)
	require.Equal(t, uint32(2), second.InternalIndex)
	require.Equal(t, "0x9fa5c5733b53814692de4fb31fd592070de5f5f0", second.Address)
	require.Equal(t, uint64(75977), second.FromValue)
	require.Equal(t, uint64(75978), second.ToValue)

	last := rows[138]
	require.Equal(t, uint64(136), last.TransactionIndex)
	require.Equal(t, "0x1894e4033d2ecb3f3b3bfa8b2c82c213593965754b40042a4ec392b2733ba76c", last.TransactionHash)
	require.Equal(t, uint32(1), last.InternalIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", last.Address)
	require.Equal(t, uint64(75878), last.FromValue)
	require.Equal(t, uint64(75879), last.ToValue)
}

func TestNonceDiffsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "nonce_diffs", decodeNonceDiffs)
	require.NoError(t, err)

	cols := newNonceDiffColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_nonce_diffs", cols.Input())
}
