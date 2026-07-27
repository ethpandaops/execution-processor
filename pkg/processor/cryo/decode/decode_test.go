package decode_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// writeUnsignedParquet builds a file shaped like cryo's output: optional,
// unsigned integer leaves.
func writeUnsignedParquet(t *testing.T, u32 []int32, u64 []int64) string {
	t.Helper()

	u32Type, err := schema.NewPrimitiveNodeLogical(
		"u32", parquet.Repetitions.Optional, schema.NewIntLogicalType(32, false),
		parquet.Types.Int32, 0, 1)
	require.NoError(t, err)

	u64Type, err := schema.NewPrimitiveNodeLogical(
		"u64", parquet.Repetitions.Optional, schema.NewIntLogicalType(64, false),
		parquet.Types.Int64, 0, 2)
	require.NoError(t, err)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		schema.FieldList{u32Type, u64Type}, -1)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "unsigned.parquet")

	f, err := os.Create(path)
	require.NoError(t, err)

	writer := file.NewParquetWriter(f, root)

	rg, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)

	defLvls := make([]int16, len(u32))
	for i := range defLvls {
		defLvls[i] = 1
	}

	col32, err := rg.NextColumn()
	require.NoError(t, err)

	writer32, ok := col32.(*file.Int32ColumnChunkWriter)
	require.True(t, ok)

	_, err = writer32.WriteBatch(u32, defLvls, nil)
	require.NoError(t, err)
	require.NoError(t, col32.Close())

	col64, err := rg.NextColumn()
	require.NoError(t, err)

	writer64, ok := col64.(*file.Int64ColumnChunkWriter)
	require.True(t, ok)

	_, err = writer64.WriteBatch(u64, defLvls, nil)
	require.NoError(t, err)
	require.NoError(t, col64.Close())

	require.NoError(t, rg.Close())
	require.NoError(t, writer.Close())

	return path
}

// TestFileReadsUnsignedValuesWithTheSignBitSet is the 2038 case. Parquet stores
// unsigned integers in signed physical types, so cryo's uint32 block timestamps
// carry a set sign bit from 2038-01-19 onward, and every uint64 column does the
// same beyond 2^63. Treating that as corruption would wedge the processor on
// every block from that date.
func TestFileReadsUnsignedValuesWithTheSignBitSet(t *testing.T) {
	t.Parallel()

	// 2038-01-19T03:14:08Z is the first second that does not fit in an int32.
	var (
		afterY2038  = uint64(2147483648)
		maxUint32   = uint64(4294967295)
		beyondInt64 = uint64(1) << 63
		maxUint64   = ^uint64(0)
	)

	path := writeUnsignedParquet(t,
		[]int32{1, int32(afterY2038), int32(maxUint32)},
		[]int64{1, int64(beyondInt64), int64(maxUint64)},
	)

	table, err := decode.File(path)
	require.NoError(t, err, "an unsigned value with the sign bit set must decode, not error")

	u32, err := table.Int("u32")
	require.NoError(t, err)

	require.Equal(t, uint64(1), u32.Int(0))
	require.Equal(t, afterY2038, u32.Int(1))
	require.Equal(t, maxUint32, u32.Int(2))

	u64, err := table.Int("u64")
	require.NoError(t, err)

	require.Equal(t, uint64(1), u64.Int(0))
	require.Equal(t, beyondInt64, u64.Int(1))
	require.Equal(t, maxUint64, u64.Int(2))
}

func TestFileRejectsSignedIntegerColumn(t *testing.T) {
	t.Parallel()

	signed, err := schema.NewPrimitiveNodeLogical(
		"s32", parquet.Repetitions.Optional, schema.NewIntLogicalType(32, true),
		parquet.Types.Int32, 0, 1)
	require.NoError(t, err)

	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{signed}, -1)
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "signed.parquet")

	f, err := os.Create(path)
	require.NoError(t, err)

	writer := file.NewParquetWriter(f, root)

	rg, err := writer.AppendRowGroupChecked()
	require.NoError(t, err)

	col, err := rg.NextColumn()
	require.NoError(t, err)

	colWriter, ok := col.(*file.Int32ColumnChunkWriter)
	require.True(t, ok)

	_, err = colWriter.WriteBatch([]int32{-1}, []int16{1}, nil)
	require.NoError(t, err)
	require.NoError(t, col.Close())
	require.NoError(t, rg.Close())
	require.NoError(t, writer.Close())

	_, err = decode.File(path)
	require.ErrorContains(t, err, "signed integer")
}
