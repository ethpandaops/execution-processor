package cryo

import (
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var tracesDataset = &Dataset{
	Name:          "traces",
	Table:         "canonical_execution_traces",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeTraces, func() columnar[traceRow] { return newTraceColumns() })
	},
}

type traceRow struct {
	UpdatedDateTime  time.Time
	BlockNumber      uint64
	TransactionIndex uint64
	TransactionHash  string
	InternalIndex    uint32
	ActionFrom       string
	ActionTo         *string
	ActionValue      proto.UInt256
	ActionGas        *uint64
	ActionInput      *string
	ActionCallType   string
	ActionInit       *string
	ActionRewardType string
	ActionType       string
	ResultGasUsed    *uint64
	ResultOutput     *string
	ResultCode       *string
	ResultAddress    *string
	TraceAddress     *string
	Subtraces        uint32
	Error            *string
	MetaNetworkName  string
}

func decodeTraces(t *decode.Table, meta rowMeta) ([]traceRow, error) {
	p := newPicker(t)

	var (
		blockNumber      = p.int("block_number")
		transactionIndex = p.int("transaction_index")
		transactionHash  = p.str("transaction_hash")
		actionFrom       = p.str("action_from")
		actionTo         = p.str("action_to")
		actionValue      = p.str("action_value")
		actionGas        = p.int("action_gas")
		actionInput      = p.str("action_input")
		actionCallType   = p.str("action_call_type")
		actionInit       = p.str("action_init")
		actionRewardType = p.str("action_reward_type")
		actionType       = p.str("action_type")
		resultGasUsed    = p.int("result_gas_used")
		resultOutput     = p.str("result_output")
		resultCode       = p.str("result_code")
		resultAddress    = p.str("result_address")
		traceAddress     = p.str("trace_address")
		subtraces        = p.int("subtraces")
		traceError       = p.str("error")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]traceRow, 0, t.Rows())

	for i := range t.Rows() {
		// action_value is the one U256 cryo names without a _string suffix.
		value, err := uint256(strOrEmpty(actionValue, i))
		if err != nil {
			return nil, fmt.Errorf("row %d: action_value: %w", i, err)
		}

		rows = append(rows, traceRow{
			UpdatedDateTime:  meta.updated,
			BlockNumber:      uintOrZero(blockNumber, i),
			TransactionIndex: uintOrZero(transactionIndex, i),
			TransactionHash:  hexOrEmpty(transactionHash, i),
			InternalIndex:    idx[i],
			ActionFrom:       hexOrEmpty(actionFrom, i),
			ActionTo:         nullableHex(actionTo, i),
			ActionValue:      value,
			ActionGas:        nullableUint(actionGas, i),
			ActionInput:      nullableHex(actionInput, i),
			ActionCallType:   strOrEmpty(actionCallType, i),
			ActionInit:       nullableHex(actionInit, i),
			ActionRewardType: strOrEmpty(actionRewardType, i),
			ActionType:       strOrEmpty(actionType, i),
			ResultGasUsed:    nullableUint(resultGasUsed, i),
			ResultOutput:     nullableHex(resultOutput, i),
			ResultCode:       nullableHex(resultCode, i),
			ResultAddress:    nullableHex(resultAddress, i),
			// trace_address is an underscore-joined path of child indices, not hex.
			TraceAddress: nullableStr(traceAddress, i),
			//nolint:gosec // cryo emits subtraces as a parquet uint32
			Subtraces:       uint32(uintOrZero(subtraces, i)),
			Error:           nullableStr(traceError, i),
			MetaNetworkName: meta.network,
		})
	}

	return rows, nil
}

type traceColumns struct {
	UpdatedDateTime  proto.ColDateTime
	BlockNumber      proto.ColUInt64
	TransactionIndex proto.ColUInt64
	TransactionHash  proto.ColFixedStr
	InternalIndex    proto.ColUInt32
	ActionFrom       proto.ColStr
	ActionTo         *proto.ColNullable[string]
	ActionValue      proto.ColUInt256
	ActionGas        *proto.ColNullable[uint64]
	ActionInput      *proto.ColNullable[string]
	ActionCallType   *proto.ColLowCardinality[string]
	ActionInit       *proto.ColNullable[string]
	ActionRewardType proto.ColStr
	ActionType       *proto.ColLowCardinality[string]
	ResultGasUsed    *proto.ColNullable[uint64]
	ResultOutput     *proto.ColNullable[string]
	ResultCode       *proto.ColNullable[string]
	ResultAddress    *proto.ColNullable[string]
	TraceAddress     *proto.ColNullable[string]
	Subtraces        proto.ColUInt32
	Error            *proto.ColNullable[string]
	MetaNetworkName  *proto.ColLowCardinality[string]
}

func newTraceColumns() *traceColumns {
	return &traceColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		ActionTo:        new(proto.ColStr).Nullable(),
		ActionGas:       new(proto.ColUInt64).Nullable(),
		ActionInput:     new(proto.ColStr).Nullable(),
		ActionCallType:  new(proto.ColStr).LowCardinality(),
		ActionInit:      new(proto.ColStr).Nullable(),
		ActionType:      new(proto.ColStr).LowCardinality(),
		ResultGasUsed:   new(proto.ColUInt64).Nullable(),
		ResultOutput:    new(proto.ColStr).Nullable(),
		ResultCode:      new(proto.ColStr).Nullable(),
		ResultAddress:   new(proto.ColStr).Nullable(),
		TraceAddress:    new(proto.ColStr).Nullable(),
		Error:           new(proto.ColStr).Nullable(),
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *traceColumns) Append(r traceRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionIndex.Append(r.TransactionIndex)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.ActionFrom.Append(r.ActionFrom)
	c.ActionTo.Append(nullable(r.ActionTo))
	c.ActionValue.Append(r.ActionValue)
	c.ActionGas.Append(nullable(r.ActionGas))
	c.ActionInput.Append(nullable(r.ActionInput))
	c.ActionCallType.Append(r.ActionCallType)
	c.ActionInit.Append(nullable(r.ActionInit))
	c.ActionRewardType.Append(r.ActionRewardType)
	c.ActionType.Append(r.ActionType)
	c.ResultGasUsed.Append(nullable(r.ResultGasUsed))
	c.ResultOutput.Append(nullable(r.ResultOutput))
	c.ResultCode.Append(nullable(r.ResultCode))
	c.ResultAddress.Append(nullable(r.ResultAddress))
	c.TraceAddress.Append(nullable(r.TraceAddress))
	c.Subtraces.Append(r.Subtraces)
	c.Error.Append(nullable(r.Error))
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *traceColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionIndex.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.ActionFrom.Reset()
	c.ActionTo.Reset()
	c.ActionValue.Reset()
	c.ActionGas.Reset()
	c.ActionInput.Reset()
	c.ActionCallType.Reset()
	c.ActionInit.Reset()
	c.ActionRewardType.Reset()
	c.ActionType.Reset()
	c.ResultGasUsed.Reset()
	c.ResultOutput.Reset()
	c.ResultCode.Reset()
	c.ResultAddress.Reset()
	c.TraceAddress.Reset()
	c.Subtraces.Reset()
	c.Error.Reset()
	c.MetaNetworkName.Reset()
}

func (c *traceColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *traceColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "action_from", Data: &c.ActionFrom},
		{Name: "action_to", Data: c.ActionTo},
		{Name: "action_value", Data: &c.ActionValue},
		{Name: "action_gas", Data: c.ActionGas},
		{Name: "action_input", Data: c.ActionInput},
		{Name: "action_call_type", Data: c.ActionCallType},
		{Name: "action_init", Data: c.ActionInit},
		{Name: "action_reward_type", Data: &c.ActionRewardType},
		{Name: "action_type", Data: c.ActionType},
		{Name: "result_gas_used", Data: c.ResultGasUsed},
		{Name: "result_output", Data: c.ResultOutput},
		{Name: "result_code", Data: c.ResultCode},
		{Name: "result_address", Data: c.ResultAddress},
		{Name: "trace_address", Data: c.TraceAddress},
		{Name: "subtraces", Data: &c.Subtraces},
		{Name: "error", Data: c.Error},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
