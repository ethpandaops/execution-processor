package cryo

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestDecodeTransactions(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "transactions", decodeTransactions)
	require.NoError(t, err)
	require.Len(t, rows, 137)

	r := rows[0]
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", r.TransactionHash)
	require.Equal(t, uint64(12963272), r.Nonce)
	require.Equal(t, "0x28c6c06298d514db089934071355e5743bf21d60", r.FromAddress)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", *r.ToAddress)
	require.Equal(t, "0", r.Value)
	require.Equal(t,
		"0xa9059cbb000000000000000000000000f2abf514dfbaf3323a5cb95bb2e4ab180a5ba3e80000000000000000000000000000000000000000000000000000000129d08a20",
		*r.Input)
	require.Equal(t, uint64(207128), r.GasLimit)
	require.Equal(t, uint64(62272), r.GasUsed)
	require.Equal(t, uint64(2239712557), r.GasPrice)
	require.Equal(t, uint8(2), r.TransactionType)
	require.Equal(t, uint64(2000000000), *r.MaxPriorityFeePerGas)
	require.Equal(t, uint64(102000000000), *r.MaxFeePerGas)
	require.True(t, r.Success)
	require.Equal(t, uint32(68), r.NInputBytes)
	require.Equal(t, uint32(39), r.NInputZeroBytes)
	require.Equal(t, uint32(29), r.NInputNonzeroBytes)
	require.Equal(t, "mainnet", r.MetaNetworkName)

	// A plain transfer carries no calldata, which cryo emits as "0x".
	require.Nil(t, rows[1].Input)

	// Legacy transactions have no 1559 fee fields at all, which is distinct
	// from having them set to zero.
	require.Equal(t, uint8(0), rows[2].TransactionType)
	require.Nil(t, rows[2].MaxPriorityFeePerGas)
	require.Nil(t, rows[2].MaxFeePerGas)
	require.Equal(t, uint64(31022955380952), rows[2].GasPrice)

	missing := 0

	for _, row := range rows {
		if row.MaxFeePerGas == nil {
			missing++
		}

		require.Equal(t, row.MaxFeePerGas == nil, row.MaxPriorityFeePerGas == nil)
	}

	require.Equal(t, 18, missing)

	require.Equal(t, uint8(4), rows[3].TransactionType)
	require.Equal(t, uint8(3), rows[45].TransactionType)

	// Values routinely exceed uint64, so they must survive as decimal text.
	require.Equal(t, "82520860000000000000", rows[56].Value)

	require.False(t, rows[88].Success)
	require.Equal(t, "0x879d8faa84bfd73fd019a39e26a819d430636f5244089389b1003c407224dac7", rows[88].TransactionHash)
}

func TestTransactionColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "transactions", decodeTransactions)
	require.NoError(t, err)

	cols := newTransactionColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	require.Equal(t, proto.UInt256{Low: proto.UInt128{Low: 8733883705161793536, High: 4}}, cols.Value[56])
	require.Equal(t, proto.UInt128{Low: 31022955380952}, cols.GasPrice[2])

	requireInputMatchesDDL(t, "canonical_execution_transaction", cols.Input())
}
