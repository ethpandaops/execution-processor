// Package decode reads cryo's parquet output into name-keyed columns.
//
// cryo writes flat, single-level parquet via Polars: every column is an
// OPTIONAL leaf of physical type BYTE_ARRAY, INT32, INT64, DOUBLE or BOOLEAN,
// LZ4_RAW compressed. There are no nested, list, struct or INT96 columns.
//
// Row order is preserved exactly as written, because it carries information
// the target schema encodes as internal_index and cannot recover otherwise.
package decode

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

const (
	// batchSize is the number of values requested per ReadBatch call.
	batchSize = 8192

	// maxPrealloc caps how much a file's declared row count may reserve up
	// front, so a corrupt footer costs an allocation rather than the process.
	maxPrealloc = 1 << 20
)

// Kind is the decoded Go representation of a parquet column.
type Kind uint8

const (
	// KindString holds BYTE_ARRAY values as Go strings.
	KindString Kind = iota
	// KindInt holds INT32/INT64 values widened to uint64.
	KindInt
	// KindFloat holds DOUBLE values.
	KindFloat
	// KindBool holds BOOLEAN values.
	KindBool
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	default:
		return "unknown"
	}
}

// ErrColumnMissing is returned when a requested column is absent from the file.
var ErrColumnMissing = errors.New("column missing from parquet file")

// Column holds one decoded parquet column. Only the slice matching Kind is
// populated; valid carries the per-row null mask.
type Column struct {
	name  string
	kind  Kind
	valid []bool
	strs  []string
	ints  []uint64
	flts  []float64
	bools []bool
}

// Name returns the parquet column name.
func (c *Column) Name() string { return c.name }

// Kind returns the decoded representation of the column.
func (c *Column) Kind() Kind { return c.kind }

// IsNull reports whether row i is null.
func (c *Column) IsNull(i int) bool { return !c.valid[i] }

// Str returns the string value at row i. The result is meaningless when the
// row is null; callers must check IsNull first.
func (c *Column) Str(i int) string { return c.strs[i] }

// Int returns the integer value at row i.
func (c *Column) Int(i int) uint64 { return c.ints[i] }

// Float returns the float value at row i.
func (c *Column) Float(i int) float64 { return c.flts[i] }

// Bool returns the boolean value at row i.
func (c *Column) Bool(i int) bool { return c.bools[i] }

// Table holds the decoded columns of one parquet file.
type Table struct {
	rows int
	cols map[string]*Column
}

// Rows returns the row count.
func (t *Table) Rows() int { return t.rows }

// Names returns the decoded column names.
func (t *Table) Names() []string {
	names := make([]string, 0, len(t.cols))
	for name := range t.cols {
		names = append(names, name)
	}

	return names
}

func (t *Table) column(name string, kind Kind) (*Column, error) {
	col, ok := t.cols[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrColumnMissing, name)
	}

	if col.kind != kind {
		return nil, fmt.Errorf("column %q is %s, want %s", name, col.kind, kind)
	}

	return col, nil
}

// Str returns the named column, requiring it to hold strings.
func (t *Table) Str(name string) (*Column, error) { return t.column(name, KindString) }

// Int returns the named column, requiring it to hold integers.
func (t *Table) Int(name string) (*Column, error) { return t.column(name, KindInt) }

// Float returns the named column, requiring it to hold floats.
func (t *Table) Float(name string) (*Column, error) { return t.column(name, KindFloat) }

// Bool returns the named column, requiring it to hold booleans.
func (t *Table) Bool(name string) (*Column, error) { return t.column(name, KindBool) }

// File decodes every column of a parquet file.
func File(path string) (*Table, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}

	defer func() { _ = rdr.Close() }()

	sch := rdr.MetaData().Schema

	indexes := make(map[string]int, sch.NumColumns())
	for i := range sch.NumColumns() {
		indexes[sch.Column(i).Name()] = i
	}

	rows := int(rdr.NumRows())

	table := &Table{rows: rows, cols: make(map[string]*Column, len(indexes))}

	for name, idx := range indexes {
		col, colErr := newColumn(sch.Column(idx), rows)
		if colErr != nil {
			return nil, fmt.Errorf("%s: %w", path, colErr)
		}

		table.cols[name] = col
	}

	for rg := range rdr.NumRowGroups() {
		group := rdr.RowGroup(rg)

		for name, idx := range indexes {
			reader, colErr := group.Column(idx)
			if colErr != nil {
				return nil, fmt.Errorf("%s: column %q row group %d: %w", path, name, rg, colErr)
			}

			if readErr := table.cols[name].read(reader); readErr != nil {
				return nil, fmt.Errorf("%s: column %q row group %d: %w", path, name, rg, readErr)
			}
		}
	}

	for name, col := range table.cols {
		if got := len(col.valid); got != rows {
			return nil, fmt.Errorf("%s: column %q decoded %d rows, file declares %d", path, name, got, rows)
		}
	}

	return table, nil
}

// newColumn allocates a column sized for the whole file from its schema entry.
func newColumn(desc *schema.Column, rows int) (*Column, error) {
	if desc.MaxRepetitionLevel() != 0 {
		return nil, fmt.Errorf("column %q is repeated, which cryo never emits", desc.Name())
	}

	prealloc := min(rows, maxPrealloc)

	col := &Column{name: desc.Name(), valid: make([]bool, 0, prealloc)}

	switch desc.PhysicalType() {
	case parquet.Types.ByteArray:
		col.kind = KindString
		col.strs = make([]string, 0, prealloc)
	case parquet.Types.Int32, parquet.Types.Int64:
		// Parquet stores unsigned integers in signed physical types, so a set
		// sign bit is data rather than an error. Only the logical type says
		// which it is, and cryo emits nothing signed.
		if lt, ok := desc.LogicalType().(schema.IntLogicalType); ok && lt.IsSigned() {
			return nil, fmt.Errorf("column %q is a signed integer, which cryo never emits", desc.Name())
		}

		col.kind = KindInt
		col.ints = make([]uint64, 0, prealloc)
	case parquet.Types.Double:
		col.kind = KindFloat
		col.flts = make([]float64, 0, prealloc)
	case parquet.Types.Boolean:
		col.kind = KindBool
		col.bools = make([]bool, 0, prealloc)
	case parquet.Types.Float, parquet.Types.Int96, parquet.Types.FixedLenByteArray, parquet.Types.Undefined:
		return nil, fmt.Errorf("column %q has unsupported physical type %s", desc.Name(), desc.PhysicalType())
	default:
		return nil, fmt.Errorf("column %q has unsupported physical type %s", desc.Name(), desc.PhysicalType())
	}

	return col, nil
}

// read appends one row group's worth of values.
func (c *Column) read(reader file.ColumnChunkReader) error {
	switch typed := reader.(type) {
	case *file.ByteArrayColumnChunkReader:
		return drain(c, typed, make([]parquet.ByteArray, batchSize), typed.ReadBatch,
			func(col *Column, v parquet.ByteArray) error {
				col.strs = append(col.strs, string(v))

				return nil
			},
			func(col *Column) { col.strs = append(col.strs, "") })
	case *file.Int32ColumnChunkReader:
		return drain(c, typed, make([]int32, batchSize), typed.ReadBatch, appendInt32, padInt)
	case *file.Int64ColumnChunkReader:
		return drain(c, typed, make([]int64, batchSize), typed.ReadBatch, appendInt64, padInt)
	case *file.Float64ColumnChunkReader:
		return drain(c, typed, make([]float64, batchSize), typed.ReadBatch,
			func(col *Column, v float64) error {
				col.flts = append(col.flts, v)

				return nil
			},
			func(col *Column) { col.flts = append(col.flts, 0) })
	case *file.BooleanColumnChunkReader:
		return drain(c, typed, make([]bool, batchSize), typed.ReadBatch,
			func(col *Column, v bool) error {
				col.bools = append(col.bools, v)

				return nil
			},
			func(col *Column) { col.bools = append(col.bools, false) })
	default:
		return fmt.Errorf("column %q has unsupported reader %T", c.name, reader)
	}
}

// readBatchFunc matches the generated ReadBatch method of every typed reader.
type readBatchFunc[T any] func(batchSize int64, values []T, defLvls, repLvls []int16) (int64, int, error)

// drain reads a column chunk to exhaustion. ReadBatch returns values densely
// packed — nulls are absent — so each batch is expanded against its definition
// levels to keep every row at its original index.
func drain[T any](
	c *Column,
	reader file.ColumnChunkReader,
	values []T,
	readBatch readBatchFunc[T],
	appendValue func(*Column, T) error,
	appendNull func(*Column),
) error {
	maxDef := reader.Descriptor().MaxDefinitionLevel()
	defLvls := make([]int16, batchSize)

	for reader.HasNext() {
		total, read, err := readBatch(batchSize, values, defLvls, nil)
		if err != nil {
			return err
		}

		if total == 0 {
			break
		}

		var next int

		for _, def := range defLvls[:total] {
			if def < maxDef {
				c.valid = append(c.valid, false)
				appendNull(c)

				continue
			}

			if next >= read {
				return fmt.Errorf("column %q: batch declared %d values but only %d were read", c.name, next+1, read)
			}

			if err := appendValue(c, values[next]); err != nil {
				return err
			}

			c.valid = append(c.valid, true)
			next++
		}
	}

	return reader.Err()
}

func appendInt32(c *Column, v int32) error {
	//nolint:gosec // G115: reinterpreting the bits is the point; the logical type is unsigned
	c.ints = append(c.ints, uint64(uint32(v)))

	return nil
}

func appendInt64(c *Column, v int64) error {
	//nolint:gosec // G115: reinterpreting the bits is the point; the logical type is unsigned
	c.ints = append(c.ints, uint64(v))

	return nil
}

func padInt(c *Column) { c.ints = append(c.ints, 0) }
