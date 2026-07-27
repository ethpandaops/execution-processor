package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBalanceDiffs(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "balance_diffs", decodeBalanceDiffs)
	require.NoError(t, err)
	require.Len(t, rows, 377)

	r := rows[0]
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", r.Address)
	require.Equal(t, "mainnet", r.MetaNetworkName)
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)

	// Balances routinely exceed uint64, so they stay decimal strings until the
	// UInt256 column takes them.
	require.Equal(t, "38432751121515279399317", r.FromValue)
	require.Equal(t, "38432750982043899049813", r.ToValue)

	require.Equal(t, uint32(2), rows[1].InternalIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", rows[1].Address)
	require.Equal(t, uint32(1), rows[2].InternalIndex)
	require.Equal(t, "0x6747dea6d070127292e2a600d879ac67b986253088e82de01d6bfa92e7ffb90c", rows[2].TransactionHash)
	require.Equal(t, uint32(3), rows[4].InternalIndex)

	last := rows[376]
	require.Equal(t, uint64(136), last.TransactionIndex)
	require.Equal(t, "0x1894e4033d2ecb3f3b3bfa8b2c82c213593965754b40042a4ec392b2733ba76c", last.TransactionHash)
	require.Equal(t, uint32(2), last.InternalIndex)
	require.Equal(t, "0x5995510b29924a0c68e5e21cb95ac426519c43bf", last.Address)
	require.Equal(t, "162525574300734667111", last.FromValue)
	require.Equal(t, "162549100389846748107", last.ToValue)
}

func TestBalanceDiffsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "balance_diffs", decodeBalanceDiffs)
	require.NoError(t, err)

	cols := newBalanceDiffColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	// 38432751121515279399317 does not fit in a uint64, and a float would lose
	// its low digits.
	require.Equal(t, uint64(0x71909f9361e1b995), cols.FromValue[0].Low.Low)
	require.Equal(t, uint64(0x823), cols.FromValue[0].Low.High)
	require.Equal(t, uint64(0), cols.FromValue[0].High.Low)

	requireInputMatchesDDL(t, "canonical_execution_balance_diffs", cols.Input())
}
