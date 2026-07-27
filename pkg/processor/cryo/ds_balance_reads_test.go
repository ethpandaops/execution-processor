package cryo

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestDecodeBalanceReads(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "balance_reads", decodeBalanceReads)
	require.NoError(t, err)
	require.Len(t, rows, 641)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", first.Address)
	require.Equal(t, "38432751121515279399317", first.Balance)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	second := rows[1]
	require.Equal(t, uint32(2), second.InternalIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", second.Address)
	require.Equal(t, "9456062291357014287", second.Balance)

	last := rows[640]
	require.Equal(t, uint64(136), last.TransactionIndex)
	require.Equal(t, "0x1894e4033d2ecb3f3b3bfa8b2c82c213593965754b40042a4ec392b2733ba76c", last.TransactionHash)
	require.Equal(t, uint32(2), last.InternalIndex)
	require.Equal(t, "0x5995510b29924a0c68e5e21cb95ac426519c43bf", last.Address)
	require.Equal(t, "162525574300734667111", last.Balance)

	// 137 transactions read balances, and every row is attributed to one of
	// them, so no row falls back to the "0x" key.
	seen := make(map[string]uint32, len(rows))

	for _, r := range rows {
		require.NotEqual(t, emptyHex, r.TransactionHash)
		seen[r.TransactionHash]++
		require.Equal(t, seen[r.TransactionHash], r.InternalIndex)
	}

	require.Len(t, seen, 137)
}

func TestBalanceReadsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "balance_reads", decodeBalanceReads)
	require.NoError(t, err)

	cols := newBalanceReadColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	// A balance beyond uint64 must survive as 256-bit limbs rather than losing
	// precision through a float.
	require.Equal(t, proto.UInt256{
		Low: proto.UInt128{Low: 14951621711058254183, High: 8},
	}, cols.Balance[640])
	require.Equal(t, proto.UInt256FromUInt64(9456062291357014287), cols.Balance[1])

	requireInputMatchesDDL(t, "canonical_execution_balance_reads", cols.Input())
}
