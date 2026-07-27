package cryo

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var balanceDiffsDataset = &Dataset{
	Name:          "balance_diffs",
	Table:         "canonical_execution_balance_diffs",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeBalanceDiffs, func() columnar[balanceDiffRow] { return newBalanceDiffColumns() })
	},
}

type balanceDiffRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Address          string
	// FromValue and ToValue stay decimal strings until Append, because balances
	// exceed uint64 and only proto.UInt256 can hold them.
	FromValue       string
	ToValue         string
	MetaNetworkName string
}

func decodeBalanceDiffs(t *decode.Table, meta rowMeta) ([]balanceDiffRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		address          = p.str("address")
		fromValue        = p.str("from_value_string")
		toValue          = p.str("to_value_string")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]balanceDiffRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, balanceDiffRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Address:          hexOrEmpty(address, i),
			FromValue:        strOrEmpty(fromValue, i),
			ToValue:          strOrEmpty(toValue, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type balanceDiffColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Address          proto.ColStr
	FromValue        proto.ColUInt256
	ToValue          proto.ColUInt256
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newBalanceDiffColumns() *balanceDiffColumns {
	return &balanceDiffColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *balanceDiffColumns) Append(r balanceDiffRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	fromValue, err := uint256(r.FromValue)
	if err != nil {
		return fmt.Errorf("from_value: %w", err)
	}

	toValue, err := uint256(r.ToValue)
	if err != nil {
		return fmt.Errorf("to_value: %w", err)
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.Address.Append(r.Address)
	c.FromValue.Append(fromValue)
	c.ToValue.Append(toValue)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *balanceDiffColumns) Reset() {
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

func (c *balanceDiffColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *balanceDiffColumns) Input() proto.Input {
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
