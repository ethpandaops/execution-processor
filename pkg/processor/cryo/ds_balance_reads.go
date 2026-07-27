package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var balanceReadsDataset = &Dataset{
	Name:          "balance_reads",
	Table:         "canonical_execution_balance_reads",
	InternalIndex: true,
	MinBlock:      1,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeBalanceReads, func() columnar[balanceReadRow] { return newBalanceReadColumns() })
	},
}

type balanceReadRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	Address          string
	Balance          string
	MetaNetworkName  string
}

func decodeBalanceReads(t *decode.Table, meta rowMeta) ([]balanceReadRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		address          = p.str("address")
		balance          = p.str("balance_string")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]balanceReadRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, balanceReadRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			Address:          hexOrEmpty(address, i),
			Balance:          strOrEmpty(balance, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type balanceReadColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	Address          proto.ColStr
	Balance          proto.ColUInt256
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newBalanceReadColumns() *balanceReadColumns {
	return &balanceReadColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *balanceReadColumns) Append(r balanceReadRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	balance, err := uint256(r.Balance)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.Address.Append(r.Address)
	c.Balance.Append(balance)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *balanceReadColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Address.Reset()
	c.Balance.Reset()
	c.MetaNetworkName.Reset()
}

func (c *balanceReadColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *balanceReadColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "address", Data: &c.Address},
		{Name: "balance", Data: &c.Balance},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
