package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var addressAppearancesDataset = &Dataset{
	Name:          "address_appearances",
	Table:         "canonical_execution_address_appearances",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeAddressAppearances, func() columnar[addressAppearanceRow] { return newAddressAppearanceColumns() })
	},
}

type addressAppearanceRow struct {
	UpdatedDateTime time.Time
	BlockNumber     uint64
	TransactionHash string
	InternalIndex   uint32
	Address         string
	Relationship    string
	MetaNetworkName string
}

func decodeAddressAppearances(t *decode.Table, meta rowMeta) ([]addressAppearanceRow, error) {
	p := newPicker(t)

	var (
		blockNumber     = p.int("block_number")
		transactionHash = p.str("transaction_hash")
		address         = p.str("address")
		relationship    = p.str("relationship")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]addressAppearanceRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, addressAppearanceRow{
			UpdatedDateTime: meta.updated,
			BlockNumber:     uintOrZero(blockNumber, i),
			TransactionHash: hexOrEmpty(transactionHash, i),
			InternalIndex:   idx[i],
			Address:         hexOrEmpty(address, i),
			Relationship:    strOrEmpty(relationship, i),
			MetaNetworkName: meta.network,
		})
	}

	return rows, nil
}

type addressAppearanceColumns struct {
	UpdatedDateTime proto.ColDateTime
	BlockNumber     proto.ColUInt64
	TransactionHash proto.ColFixedStr
	InternalIndex   proto.ColUInt32
	Address         proto.ColStr
	Relationship    *proto.ColLowCardinality[string]
	MetaNetworkName *proto.ColLowCardinality[string]
}

func newAddressAppearanceColumns() *addressAppearanceColumns {
	return &addressAppearanceColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		Relationship:    new(proto.ColStr).LowCardinality(),
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *addressAppearanceColumns) Append(r addressAppearanceRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.Address.Append(r.Address)
	c.Relationship.Append(r.Relationship)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *addressAppearanceColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.Address.Reset()
	c.Relationship.Reset()
	c.MetaNetworkName.Reset()
}

func (c *addressAppearanceColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *addressAppearanceColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "address", Data: &c.Address},
		{Name: "relationship", Data: c.Relationship},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
