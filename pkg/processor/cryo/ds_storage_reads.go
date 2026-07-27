package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var storageReadsDataset = &Dataset{
	Name:          "storage_reads",
	Table:         "canonical_execution_storage_reads",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeStorageReads, func() columnar[storageReadRow] { return newStorageReadColumns() })
	},
}

type storageReadRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	ContractAddress  string
	Slot             string
	Value            string
	MetaNetworkName  string
}

func decodeStorageReads(t *decode.Table, meta rowMeta) ([]storageReadRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		contractAddress  = p.str("contract_address")
		slot             = p.str("slot")
		value            = p.str("value")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]storageReadRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, storageReadRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			ContractAddress:  hexOrEmpty(contractAddress, i),
			// The slot key and the word it holds are stored as the hex cryo
			// emits, not as numbers: the target columns are String.
			Slot:            hexOrEmpty(slot, i),
			Value:           hexOrEmpty(value, i),
			MetaNetworkName: meta.network,
		})
	}

	return rows, nil
}

type storageReadColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	ContractAddress  proto.ColStr
	Slot             proto.ColStr
	Value            proto.ColStr
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newStorageReadColumns() *storageReadColumns {
	return &storageReadColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *storageReadColumns) Append(r storageReadRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.ContractAddress.Append(r.ContractAddress)
	c.Slot.Append(r.Slot)
	c.Value.Append(r.Value)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *storageReadColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.ContractAddress.Reset()
	c.Slot.Reset()
	c.Value.Reset()
	c.MetaNetworkName.Reset()
}

func (c *storageReadColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *storageReadColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "contract_address", Data: &c.ContractAddress},
		{Name: "slot", Data: &c.Slot},
		{Name: "value", Data: &c.Value},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
