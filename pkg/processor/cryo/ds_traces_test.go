package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// preMergeBlock has an uncle, so it carries the two reward traces that share a
// NULL transaction hash and must not collapse onto one sort key.
const preMergeBlock = "b15000051"

func TestTracesDataset(t *testing.T) {
	t.Parallel()

	require.Equal(t, "traces", tracesDataset.Name)
	require.Equal(t, "canonical_execution_traces", tracesDataset.Table)
	require.True(t, tracesDataset.InternalIndex)
	require.Zero(t, tracesDataset.MinBlock)
}

func TestDecodeTraces(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, preMergeBlock, "traces", decodeTraces)
	require.NoError(t, err)
	require.Len(t, rows, 818)

	// Row 2 is the first trace of the block's first transaction; rows 0 and 1
	// are the reward traces.
	r := rows[2]
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)
	require.Equal(t, uint64(15000051), r.BlockNumber)
	require.Equal(t, uint64(0), r.TransactionIndex)
	require.Equal(t, "0x763827e5a5197faf46175f7a83460028926cc6a2721acc9b50b5cceba824fe20", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, "0x2b4cdbcf60f009889e88b1982f1d1f9ce8fd2233", r.ActionFrom)
	require.Equal(t, "0xbeefbabeea323f07c59926295205d3b7a17e8638", *r.ActionTo)
	require.Zero(t, r.ActionValue)
	require.Equal(t, uint64(476176), *r.ActionGas)
	require.Equal(t, uint64(221260), *r.ResultGasUsed)
	require.Equal(t, "call", r.ActionCallType)
	require.Equal(t, "call", r.ActionType)
	require.Empty(t, r.ActionRewardType)
	require.Nil(t, r.ActionInit)
	require.Nil(t, r.ResultCode)
	require.Nil(t, r.ResultAddress)
	require.Equal(t, uint32(3), r.Subtraces)
	require.Nil(t, r.Error)
	require.Equal(t, "mainnet", r.MetaNetworkName)

	// cryo emits an empty return as the literal "0x", which the target column
	// stores as NULL.
	require.Nil(t, r.ResultOutput)

	// trace_address is an underscore-joined path of child indices, never hex.
	require.Nil(t, r.TraceAddress)
	require.Equal(t, "0", *rows[3].TraceAddress)
	require.Equal(t, "2_0_0", *rows[7].TraceAddress)

	require.Equal(t, uint32(2), rows[3].InternalIndex)
	require.Equal(t, "static_call", rows[3].ActionCallType)
	require.Equal(t, "0x0902f1ac", *rows[3].ActionInput)
}

// TestDecodeTracesRewards pins the behaviour the mapping exists to protect: a
// pre-merge block with an uncle produces two reward traces with no transaction
// hash, and both must survive under distinct internal indices.
func TestDecodeTracesRewards(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, preMergeBlock, "traces", decodeTraces)
	require.NoError(t, err)

	var rewards []traceRow

	for _, r := range rows {
		if r.TransactionHash == emptyHex {
			rewards = append(rewards, r)
		}
	}

	require.Len(t, rewards, 2)
	require.Equal(t, uint32(1), rewards[0].InternalIndex)
	require.Equal(t, uint32(2), rewards[1].InternalIndex)

	blockReward, err := uint256("2062500000000000000")
	require.NoError(t, err)
	require.Equal(t, "reward", rewards[0].ActionRewardType)
	require.Equal(t, blockReward, rewards[0].ActionValue)
	require.Equal(t, "0x1ad91ee08f21be3de0ba2ba6918e714da6b45836", rewards[0].ActionFrom)

	uncleReward, err := uint256("1750000000000000000")
	require.NoError(t, err)
	require.Equal(t, "uncle", rewards[1].ActionRewardType)
	require.Equal(t, uncleReward, rewards[1].ActionValue)
	require.Equal(t, "0xea674fdde714fd979de3edf0f56aa9716b898ec8", rewards[1].ActionFrom)

	for _, r := range rewards {
		require.Equal(t, uint64(0), r.TransactionIndex)
		require.Equal(t, "reward", r.ActionType)
		require.Nil(t, r.ActionTo)
		require.Nil(t, r.ActionGas)
		require.Nil(t, r.ResultGasUsed)
		require.Nil(t, r.TraceAddress)
		require.Empty(t, r.ActionCallType)
	}
}

func TestDecodeTracesPostMerge(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "traces", decodeTraces)
	require.NoError(t, err)
	require.Len(t, rows, 797)

	var suicides, creates int

	for _, r := range rows {
		// Post-merge blocks have no reward traces at all, so every row's
		// reward type is NULL in parquet and empty in the non-nullable target.
		require.Empty(t, r.ActionRewardType)
		require.NotEqual(t, emptyHex, r.TransactionHash)

		switch r.ActionType {
		case "suicide":
			suicides++

			require.Nil(t, r.ActionGas)
			require.Nil(t, r.ResultGasUsed)
			require.Empty(t, r.ActionCallType)
		case "create":
			creates++

			require.Nil(t, r.ActionTo)
			require.NotNil(t, r.ActionInit)
			require.NotNil(t, r.ResultAddress)
			require.Nil(t, r.ResultCode)
		}
	}

	require.Equal(t, 2, suicides)
	require.Equal(t, 2, creates)
}

func TestTracesColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, preMergeBlock, "traces", decodeTraces)
	require.NoError(t, err)

	cols := newTraceColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, "canonical_execution_traces", cols.Input())
}
