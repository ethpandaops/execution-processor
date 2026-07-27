package cryo

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestDecodeErc20Transfers(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "erc20_transfers", decodeErc20Transfers)
	require.NoError(t, err)
	require.Len(t, rows, 152)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, uint64(0), first.LogIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", first.Erc20)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", first.FromAddress)
	require.Equal(t, "0xf2abf514dfbaf3323a5cb95bb2e4ab180a5ba3e8", first.ToAddress)
	require.Equal(t, "4996500000", first.Value)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	// Rows 1 and 2 share a transaction, so internal_index counts within it while
	// log_index keeps counting across the block.
	require.Equal(t, "0xcb51cc795fccee5df8468c88475be47dda46ba014c4d649a2ecd44d39d5876f2", rows[1].TransactionHash)
	require.Equal(t, uint32(1), rows[1].InternalIndex)
	require.Equal(t, uint64(4), rows[1].LogIndex)
	require.Equal(t, "5264232533978631", rows[1].Value)

	require.Equal(t, rows[1].TransactionHash, rows[2].TransactionHash)
	require.Equal(t, uint32(2), rows[2].InternalIndex)
	require.Equal(t, uint64(5), rows[2].LogIndex)
	require.Equal(t, "12370946454849782151684", rows[2].Value)

	last := rows[151]
	require.Equal(t, uint64(131), last.TransactionIndex)
	require.Equal(t, uint64(321), last.LogIndex)
	require.Equal(t, "0x6c49082790288faf4c4b4a49dd7428817a8ce142cb7d474941229b1ca0b0b992", last.TransactionHash)
	require.Equal(t, "42848622000", last.Value)
}

func TestErc20TransferColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "erc20_transfers", decodeErc20Transfers)
	require.NoError(t, err)

	cols := newErc20TransferColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	// 12370946454849782151684 does not fit a uint64, so the limbs prove the
	// decimal string reached the U256 column without passing through one.
	require.Equal(t, proto.UInt256{
		Low: proto.UInt128{Low: 11627925464382568964, High: 670},
	}, cols.Value[2])

	requireInputMatchesDDL(t, "canonical_execution_erc20_transfers", cols.Input())
}
