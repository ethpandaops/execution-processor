package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeLogs(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "logs", decodeLogs)
	require.NoError(t, err)
	require.Len(t, rows, 322)

	r := rows[0]
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)
	require.Equal(t, uint64(23000000), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, uint32(0), r.LogIndex)
	require.Equal(t, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", r.Address)
	require.Equal(t, "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", r.Topic0.Value)
	require.Equal(t, "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60", r.Topic1.Value)
	require.Equal(t, "0x000000000000000000000000f2abf514dfbaf3323a5cb95bb2e4ab180a5ba3e8", r.Topic2.Value)
	require.False(t, r.Topic3.Set)
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000129d08a20", r.Data.Value)
	require.Equal(t, "mainnet", r.MetaNetworkName)

	// internal_index restarts at 1 for each transaction and counts every log
	// the transaction emitted, in the order cryo wrote them.
	require.Equal(t, uint32(1), rows[3].InternalIndex)
	require.Equal(t, uint32(5), rows[7].InternalIndex)
	require.Equal(t, uint32(11), rows[26].InternalIndex)
	require.Equal(t, uint32(47), rows[62].InternalIndex)
	require.Equal(t, "0xce3d9c0c9dc7a3db4f613a9a8f56be5b7a858b60fd758d2b4109e9db2b96dc5d", rows[62].TransactionHash)
	require.Equal(t, uint64(6), rows[62].TransactionIndex)
	require.Equal(t, uint32(1), rows[321].InternalIndex)

	// A log with no data at all arrives as "0x" and is stored as NULL.
	require.False(t, rows[26].Data.Set)
	require.False(t, rows[62].Data.Set)
}

func TestDecodeLogsAnonymous(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "logs", decodeLogs)
	require.NoError(t, err)
	require.Len(t, rows, 990)

	anonymous := 0

	for _, r := range rows {
		if !r.Topic0.Set {
			anonymous++

			require.False(t, r.Topic1.Set)
			require.False(t, r.Topic2.Set)
			require.False(t, r.Topic3.Set)
		}
	}

	// LOG0 emits no topics whatsoever, so topic0 is genuinely absent rather
	// than zero.
	require.Equal(t, 10, anonymous)

	r := rows[26]
	require.False(t, r.Topic0.Set)
	require.Equal(t, uint32(26), r.LogIndex)
	require.Equal(t, uint64(1), r.TransactionIndex)
	require.Equal(t, "0xffbf7de7b5c7740694ec15a8dfd2b6c0e42fde82fd70db8eb8122a4f10c68257", r.TransactionHash)
	require.Equal(t, uint32(17), r.InternalIndex)
	require.Equal(t, "0xe0e0e08a6a4b9dc7bd67bcb7aade5cf48157d444", r.Address)
	require.True(t, r.Data.Set)
}

func TestLogsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "logs", decodeLogs)
	require.NoError(t, err)

	cols := newLogColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_logs", cols.Input())
}
