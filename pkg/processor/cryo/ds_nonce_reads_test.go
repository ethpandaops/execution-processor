package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeNonceReads(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "nonce_reads", decodeNonceReads)
	require.NoError(t, err)
	require.Len(t, rows, 592)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", first.Address)
	require.Equal(t, uint64(12963272), first.Nonce)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	second := rows[1]
	require.Equal(t, uint32(2), second.InternalIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", second.Address)
	require.Equal(t, uint64(75876), second.Nonce)

	last := rows[591]
	require.Equal(t, uint64(136), last.TransactionIndex)
	require.Equal(t, "0x1894e4033d2ecb3f3b3bfa8b2c82c213593965754b40042a4ec392b2733ba76c", last.TransactionHash)
	require.Equal(t, uint32(2), last.InternalIndex)
	require.Equal(t, "0x5995510b29924a0c68e5e21cb95ac426519c43bf", last.Address)
	require.Equal(t, uint64(1), last.Nonce)

	seen := make(map[string]uint32, len(rows))

	for _, r := range rows {
		require.NotEqual(t, emptyHex, r.TransactionHash)
		seen[r.TransactionHash]++
		require.Equal(t, seen[r.TransactionHash], r.InternalIndex)
	}

	require.Len(t, seen, 137)
}

func TestNonceReadsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "nonce_reads", decodeNonceReads)
	require.NoError(t, err)

	cols := newNonceReadColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	require.Equal(t, uint64(12963272), cols.Nonce[0])

	requireInputMatchesDDL(t, "canonical_execution_nonce_reads", cols.Input())
}
