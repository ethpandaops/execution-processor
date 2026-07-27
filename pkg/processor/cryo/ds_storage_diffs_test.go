package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeStorageDiffs(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "storage_diffs", decodeStorageDiffs)
	require.NoError(t, err)
	require.Len(t, rows, 359)

	r := rows[0]
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", r.Address)
	require.Equal(t, "mainnet", r.MetaNetworkName)
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)

	// The slot and both values are 32-byte words stored verbatim, not numbers.
	require.Equal(t, "0x07081a045c3dbf2e63b62a407ef205e7586e2629d2e2b95ff093308ca0ff3727", r.Slot)
	require.Equal(t, "0x00000000000000000000000000000000000000000000000000035412376833d6", r.FromValue)
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000354110d97a9b6", r.ToValue)

	// A leading-zero word keeps every one of its 64 hex digits.
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000000", rows[1].FromValue)
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000129d08a20", rows[1].ToValue)
	require.Equal(t, uint32(2), rows[1].InternalIndex)

	require.Equal(t, uint64(5), rows[2].TransactionIndex)
	require.Equal(t, "0xcb51cc795fccee5df8468c88475be47dda46ba014c4d649a2ecd44d39d5876f2", rows[2].TransactionHash)
	require.Equal(t, uint32(1), rows[2].InternalIndex)
	require.Equal(t, "0x000000000004444c5dc75cb358380d2e3de08a90", rows[2].Address)
	require.Equal(t, uint32(3), rows[4].InternalIndex)

	last := rows[358]
	require.Equal(t, uint64(131), last.TransactionIndex)
	require.Equal(t, "0x6c49082790288faf4c4b4a49dd7428817a8ce142cb7d474941229b1ca0b0b992", last.TransactionHash)
	require.Equal(t, uint32(2), last.InternalIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", last.Address)
	require.Equal(t, "0xd7c2ca1b2001ebdfb058bef6a6231a3c83cff193a748eecbefc3e36fdd8bc0d6", last.Slot)
	require.Equal(t, "0x00000000000000000000000000000000000000000000000000000107e59fa4b1", last.FromValue)
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000fdeba58f01", last.ToValue)
}

func TestStorageDiffsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "storage_diffs", decodeStorageDiffs)
	require.NoError(t, err)

	cols := newStorageDiffColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_storage_diffs", cols.Input())
}
