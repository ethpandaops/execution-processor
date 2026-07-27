package cryo

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var blocksDataset = &Dataset{
	Name:          "blocks",
	Table:         "canonical_execution_block",
	InternalIndex: false,
	MinBlock:      0,
	// cryo omits gas_limit from blocks unless asked, and ClickHouse would fill
	// the gap with zero rather than failing, which is how staging came to hold
	// gas_limit = 0 for every row.
	RequiredColumns: []string{"gas_limit"},
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeBlocks, func() columnar[blockRow] { return newBlockColumns() })
	},
}

type blockRow struct {
	UpdatedDateTime time.Time
	BlockDateTime   time.Time
	BlockNumber     uint64
	BlockHash       string
	Author          *string
	GasUsed         *uint64
	GasLimit        uint64
	ExtraData       *string
	ExtraDataString *string
	BaseFeePerGas   *uint64
	MetaNetworkName string
}

func decodeBlocks(t *decode.Table, meta rowMeta) ([]blockRow, error) {
	p := newPicker(t)

	var (
		blockNumber   = p.int("block_number")
		blockHash     = p.str("block_hash")
		author        = p.str("author")
		gasUsed       = p.int("gas_used")
		gasLimit      = p.int("gas_limit")
		extraData     = p.str("extra_data")
		timestamp     = p.int("timestamp")
		baseFeePerGas = p.int("base_fee_per_gas")
	)

	if p.err != nil {
		return nil, p.err
	}

	rows := make([]blockRow, 0, t.Rows())

	for i := range t.Rows() {
		extraHex, extraString, decodeErr := extraDataPair(extraData, i)
		if decodeErr != nil {
			return nil, fmt.Errorf("row %d: %w", i, decodeErr)
		}

		rows = append(rows, blockRow{
			UpdatedDateTime: meta.updated,
			//nolint:gosec // cryo emits the block timestamp as an unsigned second count
			BlockDateTime:   time.Unix(int64(uintOrZero(timestamp, i)), 0).UTC(),
			BlockNumber:     uintOrZero(blockNumber, i),
			BlockHash:       hexOrEmpty(blockHash, i),
			Author:          nullableHex(author, i),
			GasUsed:         nullableUint(gasUsed, i),
			GasLimit:        uintOrZero(gasLimit, i),
			ExtraData:       extraHex,
			ExtraDataString: extraString,
			BaseFeePerGas:   nullableUint(baseFeePerGas, i),
			MetaNetworkName: meta.network,
		})
	}

	return rows, nil
}

// extraDataPair derives both extra_data columns from cryo's single hex field:
// the hex form is stored verbatim, and the string form is the raw bytes it
// encodes, which are arbitrary and need not be valid UTF-8.
func extraDataPair(c *decode.Column, i int) (hexForm, stringForm *string, err error) {
	if c.IsNull(i) {
		return nil, nil, nil
	}

	v := c.Str(i)
	if v == "" || v == emptyHex {
		return nil, nil, nil
	}

	raw, err := hex.DecodeString(v[len(emptyHex):])
	if err != nil {
		return nil, nil, fmt.Errorf("extra_data %q is not hex: %w", v, err)
	}

	decoded := string(raw)

	return &v, &decoded, nil
}

type blockColumns struct {
	UpdatedDateTime proto.ColDateTime
	BlockDateTime   *proto.ColDateTime64
	BlockNumber     proto.ColUInt64
	BlockHash       proto.ColFixedStr
	Author          *proto.ColNullable[string]
	GasUsed         *proto.ColNullable[uint64]
	GasLimit        proto.ColUInt64
	ExtraData       *proto.ColNullable[string]
	ExtraDataString *proto.ColNullable[string]
	BaseFeePerGas   *proto.ColNullable[uint64]
	MetaNetworkName *proto.ColLowCardinality[string]
}

func newBlockColumns() *blockColumns {
	return &blockColumns{
		BlockDateTime:   new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli),
		BlockHash:       proto.ColFixedStr{Size: hashSize},
		Author:          new(proto.ColStr).Nullable(),
		GasUsed:         new(proto.ColUInt64).Nullable(),
		ExtraData:       new(proto.ColStr).Nullable(),
		ExtraDataString: new(proto.ColStr).Nullable(),
		BaseFeePerGas:   new(proto.ColUInt64).Nullable(),
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *blockColumns) Append(r blockRow) error {
	hash, err := fixedHash(r.BlockHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockDateTime.Append(r.BlockDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.BlockHash.Append(hash)
	c.Author.Append(nullable(r.Author))
	c.GasUsed.Append(nullable(r.GasUsed))
	c.GasLimit.Append(r.GasLimit)
	c.ExtraData.Append(nullable(r.ExtraData))
	c.ExtraDataString.Append(nullable(r.ExtraDataString))
	c.BaseFeePerGas.Append(nullable(r.BaseFeePerGas))
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *blockColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockDateTime.Reset()
	c.BlockNumber.Reset()
	c.BlockHash.Reset()
	c.Author.Reset()
	c.GasUsed.Reset()
	c.GasLimit.Reset()
	c.ExtraData.Reset()
	c.ExtraDataString.Reset()
	c.BaseFeePerGas.Reset()
	c.MetaNetworkName.Reset()
}

func (c *blockColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *blockColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_date_time", Data: c.BlockDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "block_hash", Data: &c.BlockHash},
		{Name: "author", Data: c.Author},
		{Name: "gas_used", Data: c.GasUsed},
		{Name: "gas_limit", Data: &c.GasLimit},
		{Name: "extra_data", Data: c.ExtraData},
		{Name: "extra_data_string", Data: c.ExtraDataString},
		{Name: "base_fee_per_gas", Data: c.BaseFeePerGas},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
