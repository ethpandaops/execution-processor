package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const contractsTable = "canonical_execution_contracts"

func TestContractsDataset(t *testing.T) {
	t.Parallel()

	require.Equal(t, "contracts", contractsDataset.Name)
	require.Equal(t, contractsTable, contractsDataset.Table)
	require.True(t, contractsDataset.InternalIndex)
	require.Zero(t, contractsDataset.MinBlock)
}

func TestDecodeContracts(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "contracts", decodeContracts)
	require.NoError(t, err)
	require.Len(t, rows, 4)

	r := rows[0]
	require.Equal(t, fixtureMeta.updated, r.UpdatedDateTime)
	require.Equal(t, uint64(23000026), r.BlockNumber)
	require.Equal(t, "0xd8a375f59e03b9ea056afeb3257cd7e805322d34015320f4a4104b96332cbb28", r.TransactionHash)
	require.Equal(t, uint32(1), r.InternalIndex)
	require.Equal(t, uint32(0), r.CreateIndex)
	require.Equal(t, "0x70b3f08431e7d7ba1ca202017d0b19caad8d1fe9", r.ContractAddress)
	require.Equal(t, "0x4e565f63257d90f988e5ec9d065bab00f94d2dfd", r.Deployer)
	require.Equal(t, "0x9fa5c5733b53814692de4fb31fd592070de5f5f0", r.Factory)
	require.Equal(t, uint32(785), r.NInitCodeBytes)
	require.Equal(t, uint32(0), r.NCodeBytes)
	require.Equal(t, "mainnet", r.MetaNetworkName)

	// This contract self-destructs in its constructor, so cryo reports its code
	// as the literal "0x" and the target column stores NULL.
	require.False(t, r.Code.Set)

	last := rows[3]
	require.Equal(t, uint32(3), last.CreateIndex)
	require.Equal(t, uint32(1), last.InternalIndex)
	require.Equal(t, "0x9f1eb7bf7b105bd7baa144286c836ccb85d41417", last.ContractAddress)
	require.Equal(t, "0x67e0409b97fa2a41c9105b2adee931c8649ea0dc", last.Deployer)
	require.Equal(t, "0x2971adfa57b20e5a416ae5a708a8655a9c74f723", last.Factory)
	require.Equal(t,
		"0x3d602d80600a3d3981f3363d3d373d3d3d363d73fe02a32cbe0cb9ad9a945576a5bb53a3c123a3a35af43d82803e903d91602b57fd5bf3",
		last.InitCode)
	require.Equal(t,
		"0x363d3d373d3d3d363d73fe02a32cbe0cb9ad9a945576a5bb53a3c123a3a35af43d82803e903d91602b57fd5bf3",
		last.Code.Value)
	require.Equal(t, "0xf51217697e54d3e7c4d961223d0ec7a0f4962b08eb43d9e27c22df8e30b6b3e1", last.InitCodeHash)
	require.Equal(t, "0x269179116bc54c44db2053d1b6076ac2eacd45b921cf397525f967ff87095344", last.CodeHash)
	require.Equal(t, uint32(55), last.NInitCodeBytes)
	require.Equal(t, uint32(45), last.NCodeBytes)
}

// TestDecodeContractsEmpty covers a block that deploys nothing: cryo still
// writes the parquet file, with zero rows.
func TestDecodeContractsEmpty(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, preMergeBlock, "contracts", decodeContracts)
	require.NoError(t, err)
	require.Empty(t, rows)

	cols := newContractColumns()
	require.Equal(t, 0, cols.Rows())
	requireInputMatchesDDL(t, contractsTable, cols.Input())
}

func TestContractsColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "contracts", decodeContracts)
	require.NoError(t, err)

	cols := newContractColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	requireInputMatchesDDL(t, contractsTable, cols.Input())
}
