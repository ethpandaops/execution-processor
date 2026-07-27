package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var storageDiffsDataset = &Dataset{
	Name:          "storage_diffs",
	Table:         "canonical_execution_storage_diffs",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeStorageDiffs, func() columnar[storageDiffRow] { return newStorageDiffColumns() })
	},
}

type storageDiffRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Address          string
	// Slot, FromValue and ToValue are 32-byte words the target stores verbatim
	// as hex rather than as numbers.
	Slot            string
	FromValue       string
	ToValue         string
	MetaNetworkName string
}

func decodeStorageDiffs(t *decode.Table, meta rowMeta) ([]storageDiffRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		address          = p.str("address")
		slot             = p.str("slot")
		fromValue        = p.str("from_value")
		toValue          = p.str("to_value")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]storageDiffRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, storageDiffRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Address:          hexOrEmpty(address, i),
			Slot:             hexOrEmpty(slot, i),
			FromValue:        hexOrEmpty(fromValue, i),
			ToValue:          hexOrEmpty(toValue, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type storageDiffColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Address          proto.ColStr
	Slot             proto.ColStr
	FromValue        proto.ColStr
	ToValue          proto.ColStr
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newStorageDiffColumns() *storageDiffColumns {
	return &storageDiffColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *storageDiffColumns) Append(r storageDiffRow) error {
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
	c.Slot.Append(r.Slot)
	c.FromValue.Append(r.FromValue)
	c.ToValue.Append(r.ToValue)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *storageDiffColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Address.Reset()
	c.Slot.Reset()
	c.FromValue.Reset()
	c.ToValue.Reset()
	c.MetaNetworkName.Reset()
}

func (c *storageDiffColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *storageDiffColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "address", Data: &c.Address},
		{Name: "slot", Data: &c.Slot},
		{Name: "from_value", Data: &c.FromValue},
		{Name: "to_value", Data: &c.ToValue},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
