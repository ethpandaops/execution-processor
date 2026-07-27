package cryo

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestDecodeNativeTransfers(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "native_transfers", decodeNativeTransfers)
	require.NoError(t, err)
	require.Len(t, rows, 797)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, uint64(0), first.TransferIndex)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", first.FromAddress)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", first.ToAddress)
	require.Equal(t, "0", first.Value)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	require.Equal(t, first.TransactionHash, rows[1].TransactionHash)
	require.Equal(t, uint32(2), rows[1].InternalIndex)
	require.Equal(t, uint64(1), rows[1].TransferIndex)

	// transfer_index counts across the whole block, so a new transaction resets
	// internal_index without resetting it.
	require.Equal(t, "0x6747dea6d070127292e2a600d879ac67b986253088e82de01d6bfa92e7ffb90c", rows[2].TransactionHash)
	require.Equal(t, uint32(1), rows[2].InternalIndex)
	require.Equal(t, uint64(2), rows[2].TransferIndex)
	require.Equal(t, "1085803440000000000", rows[2].Value)

	big := rows[470]
	require.Equal(t, "0xc55c306596f2d46cf0658b7432834e40ddb0056d5d3083b2bae93de9753824ad", big.TransactionHash)
	require.Equal(t, uint64(56), big.TransactionIndex)
	require.Equal(t, uint64(470), big.TransferIndex)
	require.Equal(t, uint32(1), big.InternalIndex)
	require.Equal(t, "82520860000000000000", big.Value)

	last := rows[796]
	require.Equal(t, "0x1894e4033d2ecb3f3b3bfa8b2c82c213593965754b40042a4ec392b2733ba76c", last.TransactionHash)
	require.Equal(t, uint64(136), last.TransactionIndex)
	require.Equal(t, uint64(796), last.TransferIndex)
	require.Equal(t, "0x396343362be2a4da1ce0c1c210945346fb82aa49", last.FromAddress)
	require.Equal(t, "23526089112080996", last.Value)
}

func TestNativeTransferColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "native_transfers", decodeNativeTransfers)
	require.NoError(t, err)

	cols := newNativeTransferColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	// 82.52 ETH in wei overflows a uint64, so the limbs prove the decimal string
	// reached the U256 column without passing through one.
	require.Equal(t, proto.UInt256{
		Low: proto.UInt128{Low: 8733883705161793536, High: 4},
	}, cols.Value[470])

	requireInputMatchesDDL(t, "canonical_execution_native_transfers", cols.Input())
}
