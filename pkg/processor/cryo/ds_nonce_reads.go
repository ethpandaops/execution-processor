package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var nonceReadsDataset = &Dataset{
	Name:          "nonce_reads",
	Table:         "canonical_execution_nonce_reads",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeNonceReads, func() columnar[nonceReadRow] { return newNonceReadColumns() })
	},
}

type nonceReadRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Address          string
	Nonce            uint64
	MetaNetworkName  string
}

func decodeNonceReads(t *decode.Table, meta rowMeta) ([]nonceReadRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		address          = p.str("address")
		nonce            = p.int("nonce")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]nonceReadRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, nonceReadRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Address:          hexOrEmpty(address, i),
			Nonce:            uintOrZero(nonce, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type nonceReadColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Address          proto.ColStr
	Nonce            proto.ColUInt64
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newNonceReadColumns() *nonceReadColumns {
	return &nonceReadColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *nonceReadColumns) Append(r nonceReadRow) error {
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
	c.Nonce.Append(r.Nonce)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *nonceReadColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Address.Reset()
	c.Nonce.Reset()
	c.MetaNetworkName.Reset()
}

func (c *nonceReadColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *nonceReadColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "address", Data: &c.Address},
		{Name: "nonce", Data: &c.Nonce},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
