package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var fourByteCountsDataset = &Dataset{
	Name:          "four_byte_counts",
	Table:         "canonical_execution_four_byte_counts",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeFourByteCounts, func() columnar[fourByteCountRow] { return newFourByteCountColumns() })
	},
}

type fourByteCountRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Signature        string
	Size             uint64
	Count            uint64
	MetaNetworkName  string
}

func decodeFourByteCounts(t *decode.Table, meta rowMeta) ([]fourByteCountRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		signature        = p.str("signature")
		size             = p.int("size")
		count            = p.int("count")
	)

	if p.err != nil {
		return nil, p.err
	}

	// cryo counts each selector once per calldata size, so a transaction can
	// carry the same signature more than once; internal_index is what keeps
	// those rows apart under the table's sort key.
	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]fourByteCountRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, fourByteCountRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Signature:        hexOrEmpty(signature, i),
			Size:             uintOrZero(size, i),
			Count:            uintOrZero(count, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type fourByteCountColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Signature        proto.ColStr
	Size             proto.ColUInt64
	Count            proto.ColUInt64
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newFourByteCountColumns() *fourByteCountColumns {
	return &fourByteCountColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *fourByteCountColumns) Append(r fourByteCountRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.Signature.Append(r.Signature)
	c.Size.Append(r.Size)
	c.Count.Append(r.Count)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *fourByteCountColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Signature.Reset()
	c.Size.Reset()
	c.Count.Reset()
	c.MetaNetworkName.Reset()
}

func (c *fourByteCountColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *fourByteCountColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "signature", Data: &c.Signature},
		{Name: "size", Data: &c.Size},
		{Name: "count", Data: &c.Count},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
