package cryo

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

// erc721TokenLarge is the token id of the first row of the b23000026 fixture,
// an ENS label hash. At 255 bits it fills all four U256 limbs, which is the
// case a float or uint64 intermediate would silently destroy.
const erc721TokenLarge = "54878141629548454963394928233127112722036498901245006402027164760877303839156"

func TestDecodeErc721Transfers(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "erc721_transfers", decodeErc721Transfers)
	require.NoError(t, err)
	require.Len(t, rows, 42)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000026), first.BlockNumber)
	require.Equal(t, uint64(102), first.TransactionIndex)
	require.Equal(t, "0x8591f4c01a8849e191bc10101c7de815fa7f73a972d1c732823b9759977a5baf", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, uint64(463), first.LogIndex)
	// cryo emits the token contract under the column name erc20 even here.
	require.Equal(t, "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85", first.Erc721)
	require.Equal(t, "0xc0f7ee3fc687173d15f1d20182a04b22d3157fb4", first.FromAddress)
	require.Equal(t, "0x0000000000000000000000000000000000000000", first.ToAddress)
	require.Equal(t, erc721TokenLarge, first.Token)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	require.Equal(t, first.TransactionHash, rows[1].TransactionHash)
	require.Equal(t, uint32(2), rows[1].InternalIndex)
	require.Equal(t, uint64(464), rows[1].LogIndex)
	require.Equal(t, erc721TokenLarge, rows[1].Token)

	// The block's second transaction restarts internal_index at 1.
	require.Equal(t, "0x5940e7800479d3fd08490428e8f74160a54e09bd77f1c8824c97e0c689903e51", rows[40].TransactionHash)
	require.Equal(t, uint32(1), rows[40].InternalIndex)
	require.Equal(t, uint64(921), rows[40].LogIndex)

	last := rows[41]
	require.Equal(t, rows[40].TransactionHash, last.TransactionHash)
	require.Equal(t, uint32(2), last.InternalIndex)
	require.Equal(t, uint64(201), last.TransactionIndex)
	require.Equal(t, uint64(928), last.LogIndex)
	require.Equal(t, "0x283af0b28c62c092c9727f1ee09c02ca627eb7f5", last.FromAddress)
	require.Equal(t, "0xed976ca9036bc2d4e25ba8219fada1be503a09c7", last.ToAddress)
	require.Equal(t, "56460438191091326310504608336249342392432960708470807119089512209041103960200", last.Token)
}

// TestDecodeErc721TransfersEmptyBlock covers a block with no erc721 activity,
// which cryo still writes as a parquet file carrying only the schema.
func TestDecodeErc721TransfersEmptyBlock(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "erc721_transfers", decodeErc721Transfers)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestErc721TransferColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000026", "erc721_transfers", decodeErc721Transfers)
	require.NoError(t, err)

	cols := newErc721TransferColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())

	require.Equal(t, proto.UInt256{
		Low: proto.UInt128{
			Low:  8239877381706201524,
			High: 11825367184831116616,
		},
		High: proto.UInt128{
			Low:  4942523148105832171,
			High: 8742592352801473700,
		},
	}, cols.Token[0])

	require.Equal(t, proto.UInt256{
		Low: proto.UInt128{
			Low:  8071277476105599112,
			High: 1821699312930359461,
		},
		High: proto.UInt128{
			Low:  14729306524578318847,
			High: 8994666738122137780,
		},
	}, cols.Token[41])

	requireInputMatchesDDL(t, "canonical_execution_erc721_transfers", cols.Input())
}
