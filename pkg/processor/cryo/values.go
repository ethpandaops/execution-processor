package cryo

import (
	"fmt"
	"math/big"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// hashSize is the width of the FixedString columns holding 0x-prefixed hashes.
const hashSize = 66

// emptyHex is what cryo's --hex emits for a zero-length byte string, and what
// ClickHouse produces when hex-encoding a NULL coerced to an empty String.
const emptyHex = "0x"

// picker resolves the columns a dataset mapping needs, holding the first
// failure so a mapping can list its columns without an error check on each.
// A missing or wrongly typed column is a cryo schema change, and stopping the
// whole block is the intended response: the alternative is a silently absent
// column, which the target schema cannot distinguish from absent data.
type picker struct {
	table *decode.Table
	err   error
}

func newPicker(t *decode.Table) *picker {
	return &picker{table: t}
}

func (p *picker) str(name string) *decode.Column  { return p.take(p.table.Str(name)) }
func (p *picker) int(name string) *decode.Column  { return p.take(p.table.Int(name)) }
func (p *picker) bool(name string) *decode.Column { return p.take(p.table.Bool(name)) }

func (p *picker) take(c *decode.Column, err error) *decode.Column {
	if err != nil && p.err == nil {
		p.err = err
	}

	return c
}

// hexOrEmpty returns a hex column's value, mapping NULL to "0x" so that a row
// cryo could not attribute to a transaction still lands under a stable key.
func hexOrEmpty(c *decode.Column, i int) string {
	if c.IsNull(i) {
		return emptyHex
	}

	return c.Str(i)
}

// nullableHex returns the null value for a hex column that is either NULL or
// encodes a zero-length byte string, matching the empty-to-NULL guard the
// target schema expects.
//
// These return ch-go's nullable representation rather than a pointer. A *string
// costs a heap allocation per non-null value and adds an indirection the
// collector has to follow, on rows the buffer holds until the next flush.
// Carrying the value inline removes both.
func nullableHex(c *decode.Column, i int) proto.Nullable[string] {
	if c.IsNull(i) {
		return proto.Null[string]()
	}

	v := c.Str(i)
	if v == "" || v == emptyHex {
		return proto.Null[string]()
	}

	return proto.NewNullable(v)
}

// nullableStr returns the null value for a plain string column that is NULL or
// empty.
func nullableStr(c *decode.Column, i int) proto.Nullable[string] {
	if c.IsNull(i) {
		return proto.Null[string]()
	}

	v := c.Str(i)
	if v == "" {
		return proto.Null[string]()
	}

	return proto.NewNullable(v)
}

// strOrEmpty returns a plain string column's value, mapping NULL to "".
func strOrEmpty(c *decode.Column, i int) string {
	if c.IsNull(i) {
		return ""
	}

	return c.Str(i)
}

// uintOrZero returns an integer column's value, mapping NULL to 0.
func uintOrZero(c *decode.Column, i int) uint64 {
	if c.IsNull(i) {
		return 0
	}

	return c.Int(i)
}

// nullableUint returns the null value for a NULL integer column.
func nullableUint(c *decode.Column, i int) proto.Nullable[uint64] {
	if c.IsNull(i) {
		return proto.Null[uint64]()
	}

	return proto.NewNullable(c.Int(i))
}

// boolOrFalse returns a boolean column's value, mapping NULL to false.
func boolOrFalse(c *decode.Column, i int) bool {
	if c.IsNull(i) {
		return false
	}

	return c.Bool(i)
}

// fixedHash renders a 0x-prefixed hash into the fixed-width buffer the target
// column uses, right-padding with zero bytes exactly as ClickHouse does.
func fixedHash(s string) ([]byte, error) {
	if len(s) > hashSize {
		return nil, fmt.Errorf("hash %q is %d bytes, exceeding FixedString(%d)", s, len(s), hashSize)
	}

	buf := make([]byte, hashSize)
	copy(buf, s)

	return buf, nil
}

// uint256 parses a decimal string into ch-go's little-endian limb layout.
// cryo emits every U256 as a decimal string under --u256-types string, and the
// values routinely exceed uint64, so this must never route through a float.
func uint256(s string) (proto.UInt256, error) {
	if s == "" {
		return proto.UInt256{}, nil
	}

	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return proto.UInt256{}, fmt.Errorf("value %q is not a decimal integer", s)
	}

	if v.Sign() < 0 {
		return proto.UInt256{}, fmt.Errorf("value %q is negative", s)
	}

	if v.BitLen() > 256 {
		return proto.UInt256{}, fmt.Errorf("value %q exceeds 256 bits", s)
	}

	var buf [32]byte

	v.FillBytes(buf[:])

	// FillBytes writes big-endian; the limbs are little-endian by both word
	// order and byte order within each word.
	return proto.UInt256{
		Low: proto.UInt128{
			Low:  beUint64(buf[24:32]),
			High: beUint64(buf[16:24]),
		},
		High: proto.UInt128{
			Low:  beUint64(buf[8:16]),
			High: beUint64(buf[0:8]),
		},
	}, nil
}

func beUint64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

// internalIndex assigns a 1-based counter per transaction hash, walking rows in
// parquet file order. ClickHouse preserves neither insertion order nor, through
// a Distributed table, any recoverable order at all, so this column is the only
// record of the sequence cryo produced.
//
// Rows whose hash is NULL are counted under the same key they will be stored
// with rather than skipped. Skipping them collapses every block reward trace in
// a block onto one sort key, which loses one of the two rewards on pre-merge
// blocks with uncles.
func internalIndex(hashes *decode.Column, rows int) []uint32 {
	out := make([]uint32, rows)
	seen := make(map[string]uint32, rows)

	for i := range rows {
		key := hexOrEmpty(hashes, i)
		seen[key]++
		out[i] = seen[key]
	}

	return out
}
