package cryo

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var nativeTransfersDataset = &Dataset{
	Name:          "native_transfers",
	Table:         "canonical_execution_native_transfers",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeNativeTransfers, func() columnar[nativeTransferRow] { return newNativeTransferColumns() })
	},
}

type nativeTransferRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	TransferIndex    uint64
	FromAddress      string
	ToAddress        string
	Value            string
	MetaNetworkName  string
}

func decodeNativeTransfers(t *decode.Table, meta rowMeta) ([]nativeTransferRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		transferIndex    = p.int("transfer_index")
		fromAddress      = p.str("from_address")
		toAddress        = p.str("to_address")
		value            = p.str("value_string")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]nativeTransferRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, nativeTransferRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			TransferIndex:    uintOrZero(transferIndex, i),
			FromAddress:      hexOrEmpty(fromAddress, i),
			ToAddress:        hexOrEmpty(toAddress, i),
			Value:            strOrEmpty(value, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type nativeTransferColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	TransferIndex    proto.ColUInt64
	FromAddress      proto.ColStr
	ToAddress        proto.ColStr
	Value            proto.ColUInt256
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newNativeTransferColumns() *nativeTransferColumns {
	return &nativeTransferColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *nativeTransferColumns) Append(r nativeTransferRow) error {
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
	c.TransferIndex.Append(r.TransferIndex)
	c.FromAddress.Append(r.FromAddress)
	c.ToAddress.Append(r.ToAddress)
	c.Value.Append(value)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *nativeTransferColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.TransferIndex.Reset()
	c.FromAddress.Reset()
	c.ToAddress.Reset()
	c.Value.Reset()
	c.MetaNetworkName.Reset()
}

func (c *nativeTransferColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *nativeTransferColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "transfer_index", Data: &c.TransferIndex},
		{Name: "from_address", Data: &c.FromAddress},
		{Name: "to_address", Data: &c.ToAddress},
		{Name: "value", Data: &c.Value},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
