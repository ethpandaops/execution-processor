package cryo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeFourByteCounts(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "four_byte_counts", decodeFourByteCounts)
	require.NoError(t, err)
	require.Len(t, rows, 278)

	first := rows[0]
	require.Equal(t, fixtureMeta.updated, first.UpdatedDateTime)
	require.Equal(t, uint64(23000000), first.BlockNumber)
	require.Equal(t, uint64(0), first.TransactionIndex)
	require.Equal(t, "0x5946ef0de28db53ffe00f0fcdb4cc19c9da234819430bf957cc4c15200d8bcac", first.TransactionHash)
	require.Equal(t, uint32(1), first.InternalIndex)
	require.Equal(t, "0xa9059cbb", first.Signature)
	require.Equal(t, uint64(64), first.Size)
	require.Equal(t, uint64(2), first.Count)
	require.Equal(t, "mainnet", first.MetaNetworkName)

	second := rows[1]
	require.Equal(t, uint64(3), second.TransactionIndex)
	require.Equal(t, "0x3a49a2dcaf21efef0db0aeb21dba2b335a502078c8d5d8b9fb32a078e506d274", second.TransactionHash)
	require.Equal(t, uint32(1), second.InternalIndex)
	require.Equal(t, "0x5c43fcf6", second.Signature)

	last := rows[277]
	require.Equal(t, uint64(131), last.TransactionIndex)
	require.Equal(t, "0x6c49082790288faf4c4b4a49dd7428817a8ce142cb7d474941229b1ca0b0b992", last.TransactionHash)
	require.Equal(t, uint32(1), last.InternalIndex)
	require.Equal(t, "0xa9059cbb", last.Signature)
	require.Equal(t, uint64(64), last.Size)
	require.Equal(t, uint64(2), last.Count)
}

// TestFourByteCountsInternalIndexIsUnique guards the reason this table carries
// internal_index at all: cryo counts a selector once per calldata size, so a
// transaction can repeat a signature, and without the extra sort key those rows
// collapse into one another.
func TestFourByteCountsInternalIndexIsUnique(t *testing.T) {
	t.Parallel()

	for block, want := range map[string]int{"b23000000": 278, "b23000026": 1094} {
		t.Run(block, func(t *testing.T) {
			t.Parallel()

			rows, err := decodeFixture(t, block, "four_byte_counts", decodeFourByteCounts)
			require.NoError(t, err)
			require.Len(t, rows, want)

			type key struct {
				hash  string
				index uint32
			}

			seen := make(map[key]struct{}, len(rows))
			perTx := make(map[string]uint32, len(rows))

			for i, r := range rows {
				k := key{hash: r.TransactionHash, index: r.InternalIndex}

				_, dup := seen[k]

				require.NotZero(t, r.InternalIndex, "row %d has a zero internal_index", i)
				require.False(t, dup, "row %d repeats (%s, %d)", i, r.TransactionHash, r.InternalIndex)

				seen[k] = struct{}{}
				perTx[r.TransactionHash]++

				require.Equal(t, perTx[r.TransactionHash], r.InternalIndex,
					"row %d is not the next index for %s", i, r.TransactionHash)
			}

			require.Len(t, seen, len(rows))
		})
	}
}

// TestFourByteCountsRepeatedSignature pins the transaction that motivates the
// column: the same selector appears twice under two calldata sizes.
func TestFourByteCountsRepeatedSignature(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "four_byte_counts", decodeFourByteCounts)
	require.NoError(t, err)

	const hash = "0x50c8d7b157abe4c0515adab242a25bd39f815fcb51f1e03c21650e38afb887e0"

	var (
		tx      []fourByteCountRow
		repeats []fourByteCountRow
	)

	for _, r := range rows {
		if r.TransactionHash != hash {
			continue
		}

		tx = append(tx, r)

		if r.Signature == "0x7bd243de" {
			repeats = append(repeats, r)
		}
	}

	require.Len(t, tx, 12)

	for i, r := range tx {
		require.Equal(t, uint32(i+1), r.InternalIndex)
		require.Equal(t, uint64(122), r.TransactionIndex)
	}

	require.Len(t, repeats, 2)
	require.Equal(t, uint32(9), repeats[0].InternalIndex)
	require.Equal(t, uint64(192), repeats[0].Size)
	require.Equal(t, uint32(10), repeats[1].InternalIndex)
	require.Equal(t, uint64(224), repeats[1].Size)
}

func TestFourByteCountColumnsAppend(t *testing.T) {
	t.Parallel()

	rows, err := decodeFixture(t, "b23000000", "four_byte_counts", decodeFourByteCounts)
	require.NoError(t, err)

	cols := newFourByteCountColumns()
	for _, r := range rows {
		require.NoError(t, cols.Append(r))
	}

	require.Equal(t, len(rows), cols.Rows())
	require.Equal(t, "0xa9059cbb", cols.Signature.Row(0))
	require.Equal(t, uint32(1), cols.InternalIndex[0])

	requireInputMatchesDDL(t, "canonical_execution_four_byte_counts", cols.Input())
}
