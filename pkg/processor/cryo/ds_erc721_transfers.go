package cryo

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var erc721TransfersDataset = &Dataset{
	Name:          "erc721_transfers",
	Table:         "canonical_execution_erc721_transfers",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeErc721Transfers, func() columnar[erc721TransferRow] { return newErc721TransferColumns() })
	},
}

type erc721TransferRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	LogIndex         uint64
	Erc721           string
	FromAddress      string
	ToAddress        string
	Token            string
	MetaNetworkName  string
}

func decodeErc721Transfers(t *decode.Table, meta rowMeta) ([]erc721TransferRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		logIndex         = p.int("log_index")
		// cryo names the token contract column erc20 in its erc721 output too.
		erc721      = p.str("erc20")
		fromAddress = p.str("from_address")
		toAddress   = p.str("to_address")
		token       = p.str("token_id_string")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]erc721TransferRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, erc721TransferRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			LogIndex:         uintOrZero(logIndex, i),
			Erc721:           hexOrEmpty(erc721, i),
			FromAddress:      hexOrEmpty(fromAddress, i),
			ToAddress:        hexOrEmpty(toAddress, i),
			Token:            strOrEmpty(token, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type erc721TransferColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	LogIndex         proto.ColUInt64
	Erc721           proto.ColStr
	FromAddress      proto.ColStr
	ToAddress        proto.ColStr
	Token            proto.ColUInt256
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newErc721TransferColumns() *erc721TransferColumns {
	return &erc721TransferColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *erc721TransferColumns) Append(r erc721TransferRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	token, err := uint256(r.Token)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.LogIndex.Append(r.LogIndex)
	c.Erc721.Append(r.Erc721)
	c.FromAddress.Append(r.FromAddress)
	c.ToAddress.Append(r.ToAddress)
	c.Token.Append(token)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *erc721TransferColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.LogIndex.Reset()
	c.Erc721.Reset()
	c.FromAddress.Reset()
	c.ToAddress.Reset()
	c.Token.Reset()
	c.MetaNetworkName.Reset()
}

func (c *erc721TransferColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *erc721TransferColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "log_index", Data: &c.LogIndex},
		{Name: "erc721", Data: &c.Erc721},
		{Name: "from_address", Data: &c.FromAddress},
		{Name: "to_address", Data: &c.ToAddress},
		{Name: "token", Data: &c.Token},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
