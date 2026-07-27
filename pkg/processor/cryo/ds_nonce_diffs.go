package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var nonceDiffsDataset = &Dataset{
	Name:          "nonce_diffs",
	Table:         "canonical_execution_nonce_diffs",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeNonceDiffs, func() columnar[nonceDiffRow] { return newNonceDiffColumns() })
	},
}

type nonceDiffRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Address          string
	FromValue        uint64
	ToValue          uint64
	MetaNetworkName  string
}

func decodeNonceDiffs(t *decode.Table, meta rowMeta) ([]nonceDiffRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		address          = p.str("address")
		fromValue        = p.int("from_value")
		toValue          = p.int("to_value")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]nonceDiffRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, nonceDiffRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Address:          hexOrEmpty(address, i),
			FromValue:        uintOrZero(fromValue, i),
			ToValue:          uintOrZero(toValue, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type nonceDiffColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Address          proto.ColStr
	FromValue        proto.ColUInt64
	ToValue          proto.ColUInt64
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newNonceDiffColumns() *nonceDiffColumns {
	return &nonceDiffColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *nonceDiffColumns) Append(r nonceDiffRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.Address.Append(r.Address)
	c.FromValue.Append(r.FromValue)
	c.ToValue.Append(r.ToValue)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *nonceDiffColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Address.Reset()
	c.FromValue.Reset()
	c.ToValue.Reset()
	c.MetaNetworkName.Reset()
}

func (c *nonceDiffColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *nonceDiffColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "address", Data: &c.Address},
		{Name: "from_value", Data: &c.FromValue},
		{Name: "to_value", Data: &c.ToValue},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
