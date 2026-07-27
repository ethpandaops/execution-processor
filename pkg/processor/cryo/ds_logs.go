package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var logsDataset = &Dataset{
	Name:          "logs",
	Table:         "canonical_execution_logs",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeLogs, func() columnar[logRow] { return newLogColumns() })
	},
}

type logRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	LogIndex         uint32
	Address          string
	Topic0           proto.Nullable[string]
	Topic1           proto.Nullable[string]
	Topic2           proto.Nullable[string]
	Topic3           proto.Nullable[string]
	Data             proto.Nullable[string]
	MetaNetworkName  string
}

func decodeLogs(t *decode.Table, meta rowMeta) ([]logRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		logIndex         = p.int("log_index")
		address          = p.str("address")
		topic0           = p.str("topic0")
		topic1           = p.str("topic1")
		topic2           = p.str("topic2")
		topic3           = p.str("topic3")
		data             = p.str("data")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]logRow, 0, t.Rows())

	for i := range t.Rows() {
		//nolint:gosec // log_index is uint32 in cryo's schema, widened by the decoder
		rows = append(rows, logRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			LogIndex:         uint32(uintOrZero(logIndex, i)),
			Address:          hexOrEmpty(address, i),
			Topic0:           nullableHex(topic0, i),
			Topic1:           nullableHex(topic1, i),
			Topic2:           nullableHex(topic2, i),
			Topic3:           nullableHex(topic3, i),
			Data:             nullableHex(data, i),
			MetaNetworkName:  meta.network,
		})
	}

	return rows, nil
}

type logColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	LogIndex         proto.ColUInt32
	Address          proto.ColStr
	Topic0           *proto.ColNullable[string]
	Topic1           *proto.ColNullable[string]
	Topic2           *proto.ColNullable[string]
	Topic3           *proto.ColNullable[string]
	Data             *proto.ColNullable[string]
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newLogColumns() *logColumns {
	return &logColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		Topic0:          new(proto.ColStr).Nullable(),
		Topic1:          new(proto.ColStr).Nullable(),
		Topic2:          new(proto.ColStr).Nullable(),
		Topic3:          new(proto.ColStr).Nullable(),
		Data:            new(proto.ColStr).Nullable(),
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *logColumns) Append(r logRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.LogIndex.Append(r.LogIndex)
	c.Address.Append(r.Address)
	c.Topic0.Append(r.Topic0)
	c.Topic1.Append(r.Topic1)
	c.Topic2.Append(r.Topic2)
	c.Topic3.Append(r.Topic3)
	c.Data.Append(r.Data)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *logColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.LogIndex.Reset()
	c.Address.Reset()
	c.Topic0.Reset()
	c.Topic1.Reset()
	c.Topic2.Reset()
	c.Topic3.Reset()
	c.Data.Reset()
	c.MetaNetworkName.Reset()
}

func (c *logColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *logColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "log_index", Data: &c.LogIndex},
		{Name: "address", Data: &c.Address},
		{Name: "topic0", Data: c.Topic0},
		{Name: "topic1", Data: c.Topic1},
		{Name: "topic2", Data: c.Topic2},
		{Name: "topic3", Data: c.Topic3},
		{Name: "data", Data: c.Data},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
