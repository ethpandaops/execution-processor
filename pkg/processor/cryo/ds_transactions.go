package cryo

import (
	"fmt"
	"math"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var transactionsDataset = &Dataset{
	Name:          "transactions",
	Table:         "canonical_execution_transaction",
	InternalIndex: false,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeTransactions, func() columnar[transactionRow] { return newTransactionColumns() })
	},
}

type transactionRow struct {
	UpdatedDateTime      time.Time
	BlockNumber          uint64
	TransactionIndex     uint64
	TransactionHash      string
	Nonce                uint64
	FromAddress          string
	ToAddress            proto.Nullable[string]
	Value                string
	Input                proto.Nullable[string]
	GasLimit             uint64
	GasUsed              uint64
	GasPrice             uint64
	TransactionType      uint8
	MaxPriorityFeePerGas proto.Nullable[uint64]
	MaxFeePerGas         proto.Nullable[uint64]
	Success              bool
	NInputBytes          uint32
	NInputZeroBytes      uint32
	NInputNonzeroBytes   uint32
	MetaNetworkName      string
}

func decodeTransactions(t *decode.Table, meta rowMeta) ([]transactionRow, error) {
	p := newPicker(t)

	var (
		blockNumber          = p.int("block_number")
		transactionIndex     = p.int("transaction_index")
		transactionHash      = p.str("transaction_hash")
		nonce                = p.int("nonce")
		fromAddress          = p.str("from_address")
		toAddress            = p.str("to_address")
		value                = p.str("value_string")
		input                = p.str("input")
		gasLimit             = p.int("gas_limit")
		gasUsed              = p.int("gas_used")
		gasPrice             = p.int("gas_price")
		transactionType      = p.int("transaction_type")
		maxPriorityFeePerGas = p.int("max_priority_fee_per_gas")
		maxFeePerGas         = p.int("max_fee_per_gas")
		success              = p.bool("success")
		nInputBytes          = p.int("n_input_bytes")
		nInputZeroBytes      = p.int("n_input_zero_bytes")
		nInputNonzeroBytes   = p.int("n_input_nonzero_bytes")
	)

	if p.err != nil {
		return nil, p.err
	}

	rows := make([]transactionRow, 0, t.Rows())

	for i := range t.Rows() {
		// cryo types the envelope as uint32 while the target stores a single
		// byte, which is all EIP-2718 allows; a wider value is a protocol
		// change rather than a value to truncate.
		txType := uintOrZero(transactionType, i)
		if txType > math.MaxUint8 {
			return nil, fmt.Errorf("row %d: transaction_type %d does not fit UInt8", i, txType)
		}

		//nolint:gosec // the n_input_* counts are uint32 in cryo's schema, widened by the decoder
		rows = append(rows, transactionRow{
			UpdatedDateTime:      meta.updated,
			BlockNumber:          uintOrZero(blockNumber, i),
			TransactionIndex:     uintOrZero(transactionIndex, i),
			TransactionHash:      hexOrEmpty(transactionHash, i),
			Nonce:                uintOrZero(nonce, i),
			FromAddress:          hexOrEmpty(fromAddress, i),
			ToAddress:            nullableHex(toAddress, i),
			Value:                strOrEmpty(value, i),
			Input:                nullableHex(input, i),
			GasLimit:             uintOrZero(gasLimit, i),
			GasUsed:              uintOrZero(gasUsed, i),
			GasPrice:             uintOrZero(gasPrice, i),
			TransactionType:      uint8(txType),
			MaxPriorityFeePerGas: nullableUint(maxPriorityFeePerGas, i),
			MaxFeePerGas:         nullableUint(maxFeePerGas, i),
			Success:              boolOrFalse(success, i),
			NInputBytes:          uint32(uintOrZero(nInputBytes, i)),
			NInputZeroBytes:      uint32(uintOrZero(nInputZeroBytes, i)),
			NInputNonzeroBytes:   uint32(uintOrZero(nInputNonzeroBytes, i)),
			MetaNetworkName:      meta.network,
		})
	}

	return rows, nil
}

type transactionColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	Nonce            proto.ColUInt64
	FromAddress      proto.ColStr
	ToAddress        *proto.ColNullable[string]
	Value            proto.ColUInt256
	InputData        *proto.ColNullable[string]
	GasLimit         proto.ColUInt64
	GasUsed          proto.ColUInt64
	// GasPrice is wider than anything cryo can produce: the effective gas
	// price is a uint64 on the wire, the target column simply reserves room.
	GasPrice             proto.ColUInt128
	TransactionType      proto.ColUInt8
	MaxPriorityFeePerGas *proto.ColNullable[uint64]
	MaxFeePerGas         *proto.ColNullable[uint64]
	Success              proto.ColBool
	NInputBytes          proto.ColUInt32
	NInputZeroBytes      proto.ColUInt32
	NInputNonzeroBytes   proto.ColUInt32
	MetaNetworkName      *proto.ColLowCardinality[string]
}

func newTransactionColumns() *transactionColumns {
	return &transactionColumns{
		TransactionHash:      proto.ColFixedStr{Size: hashSize},
		ToAddress:            new(proto.ColStr).Nullable(),
		InputData:            new(proto.ColStr).Nullable(),
		MaxPriorityFeePerGas: new(proto.ColUInt64).Nullable(),
		MaxFeePerGas:         new(proto.ColUInt64).Nullable(),
		MetaNetworkName:      new(proto.ColStr).LowCardinality(),
	}
}

func (c *transactionColumns) Append(r transactionRow) error {
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
	c.Nonce.Append(r.Nonce)
	c.FromAddress.Append(r.FromAddress)
	c.ToAddress.Append(r.ToAddress)
	c.Value.Append(value)
	c.InputData.Append(r.Input)
	c.GasLimit.Append(r.GasLimit)
	c.GasUsed.Append(r.GasUsed)
	c.GasPrice.Append(proto.UInt128{Low: r.GasPrice})
	c.TransactionType.Append(r.TransactionType)
	c.MaxPriorityFeePerGas.Append(r.MaxPriorityFeePerGas)
	c.MaxFeePerGas.Append(r.MaxFeePerGas)
	c.Success.Append(r.Success)
	c.NInputBytes.Append(r.NInputBytes)
	c.NInputZeroBytes.Append(r.NInputZeroBytes)
	c.NInputNonzeroBytes.Append(r.NInputNonzeroBytes)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *transactionColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.Nonce.Reset()
	c.FromAddress.Reset()
	c.ToAddress.Reset()
	c.Value.Reset()
	c.InputData.Reset()
	c.GasLimit.Reset()
	c.GasUsed.Reset()
	c.GasPrice.Reset()
	c.TransactionType.Reset()
	c.MaxPriorityFeePerGas.Reset()
	c.MaxFeePerGas.Reset()
	c.Success.Reset()
	c.NInputBytes.Reset()
	c.NInputZeroBytes.Reset()
	c.NInputNonzeroBytes.Reset()
	c.MetaNetworkName.Reset()
}

func (c *transactionColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *transactionColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "nonce", Data: &c.Nonce},
		{Name: "from_address", Data: &c.FromAddress},
		{Name: "to_address", Data: c.ToAddress},
		{Name: "value", Data: &c.Value},
		{Name: "input", Data: c.InputData},
		{Name: "gas_limit", Data: &c.GasLimit},
		{Name: "gas_used", Data: &c.GasUsed},
		{Name: "gas_price", Data: &c.GasPrice},
		{Name: "transaction_type", Data: &c.TransactionType},
		{Name: "max_priority_fee_per_gas", Data: c.MaxPriorityFeePerGas},
		{Name: "max_fee_per_gas", Data: c.MaxFeePerGas},
		{Name: "success", Data: &c.Success},
		{Name: "n_input_bytes", Data: &c.NInputBytes},
		{Name: "n_input_zero_bytes", Data: &c.NInputZeroBytes},
		{Name: "n_input_nonzero_bytes", Data: &c.NInputNonzeroBytes},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
