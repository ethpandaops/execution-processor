package cryo

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStorageReads(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "storage_reads", decodeStorageReads)
	require.NoError(t, err)
	require.Len(t, rows, 900)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", first.ContractAddress)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	// The slot and the word it holds stay verbatim hex; leading zero nibbles
	// would be lost if either were read as a number.
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000001", first.Slot)
	require.Equal(t, "0x0000000000000000000000004914f61d25e5c567143774b76edbf4d5109a8566", first.Value)

	second := rows[1]
	require.Equal(t, uint32(2), second.InternalIndex)
	require.Equal(t, "0x07081a045c3dbf2e63b62a407ef205e7586e2629d2e2b95ff093308ca0ff3727", second.Slot)
	require.Equal(t, "0x00000000000000000000000000000000000000000000000000035412376833d6", second.Value)

	last := rows[899]
	require.Equal(t, uint64(131), last.TransactionIndex)
	require.Equal(t, "0x6c49082790288faf4c4b4a49dd7428817a8ce142cb7d474941229b1ca0b0b992", last.TransactionHash)
	require.Equal(t, uint32(5), last.InternalIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", last.ContractAddress)
	require.Equal(t, "0xd7c2ca1b2001ebdfb058bef6a6231a3c83cff193a748eecbefc3e36fdd8bc0d6", last.Slot)
	require.Equal(t, "0x00000000000000000000000000000000000000000000000000000107e59fa4b1", last.Value)

	seen := make(map[string]uint32, len(rows))

	for _, r := range rows {
		require.Len(t, r.Slot, 66)
		require.Len(t, r.Value, 66)
		require.True(t, strings.HasPrefix(r.Slot, emptyHex))
		require.True(t, strings.HasPrefix(r.Value, emptyHex))
		require.Len(t, r.ContractAddress, 42)

		seen[r.TransactionHash]++
		require.Equal(t, seen[r.TransactionHash], r.InternalIndex)
	}

	require.Len(t, seen, 76)
}

func TestStorageReadsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "storage_reads", decodeStorageReads)
	require.NoError(t, err)

	cols := newStorageReadColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000001", cols.Slot.Row(0))

	requireInputMatchesDDL(t, "canonical_execution_storage_reads", cols.Input())
}
