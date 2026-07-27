package cryo

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var erc20TransfersDataset = &Dataset{
	Name:          "erc20_transfers",
	Table:         "canonical_execution_erc20_transfers",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeErc20Transfers, func() columnar[erc20TransferRow] { return newErc20TransferColumns() })
	},
}

type erc20TransferRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	LogIndex         uint64
	Erc20            string
	FromAddress      string
	ToAddress        string
	Value            string
	MetaNetworkName  string
}

func decodeErc20Transfers(t *decode.Table, meta rowMeta) ([]erc20TransferRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		logIndex         = p.int("log_index")
		erc20            = p.str("erc20")
		fromAddress      = p.str("from_address")
		toAddress        = p.str("to_address")
		value            = p.str("value_string")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]erc20TransferRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, erc20TransferRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			LogIndex:         uintOrZero(logIndex, i),
			Erc20:            hexOrEmpty(erc20, i),
			FromAddress:      hexOrEmpty(fromAddress, i),
			ToAddress:        hexOrEmpty(toAddress, i),
			Value:            strOrEmpty(value, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type erc20TransferColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	LogIndex         proto.ColUInt64
	Erc20            proto.ColStr
	FromAddress      proto.ColStr
	ToAddress        proto.ColStr
	Value            proto.ColUInt256
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newErc20TransferColumns() *erc20TransferColumns {
	return &erc20TransferColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *erc20TransferColumns) Append(r erc20TransferRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	value, err := uint256(r.Value)
	if err != nil {
		return fmt.Errorf("value: %w", err)
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.LogIndex.Append(r.LogIndex)
	c.Erc20.Append(r.Erc20)
	c.FromAddress.Append(r.FromAddress)
	c.ToAddress.Append(r.ToAddress)
	c.Value.Append(value)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *erc20TransferColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.LogIndex.Reset()
	c.Erc20.Reset()
	c.FromAddress.Reset()
	c.ToAddress.Reset()
	c.Value.Reset()
	c.MetaNetworkName.Reset()
}

func (c *erc20TransferColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *erc20TransferColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "log_index", Data: &c.LogIndex},
		{Name: "erc20", Data: &c.Erc20},
		{Name: "from_address", Data: &c.FromAddress},
		{Name: "to_address", Data: &c.ToAddress},
		{Name: "value", Data: &c.Value},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
