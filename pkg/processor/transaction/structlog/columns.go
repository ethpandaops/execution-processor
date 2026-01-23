package structlog

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ClickHouseTime wraps time.Time for ClickHouse DateTime formatting.
type ClickHouseTime time.Time

// NewClickHouseTime creates a new ClickHouseTime from time.Time.
func NewClickHouseTime(t time.Time) ClickHouseTime {
	return ClickHouseTime(t)
}

// Time returns the underlying time.Time.
func (t ClickHouseTime) Time() time.Time {
	return time.Time(t)
}

// Columns holds all columns for structlog batch insert using ch-go columnar protocol.
type Columns struct {
	UpdatedDateTime        proto.ColDateTime
	BlockNumber            proto.ColUInt64
	TransactionHash        proto.ColStr
	TransactionIndex       proto.ColUInt32
	TransactionGas         proto.ColUInt64
	TransactionFailed      proto.ColBool
	TransactionReturnValue *proto.ColNullable[string]
	Index                  proto.ColUInt32
	ProgramCounter         proto.ColUInt32
	Operation              proto.ColStr
	Gas                    proto.ColUInt64
	GasCost                proto.ColUInt64
	GasUsed                proto.ColUInt64
	GasSelf                proto.ColUInt64
	Depth                  proto.ColUInt64
	ReturnData             *proto.ColNullable[string]
	Refund                 *proto.ColNullable[uint64]
	Error                  *proto.ColNullable[string]
	CallToAddress          *proto.ColNullable[string]
	CallFrameID            proto.ColUInt32
	CallFramePath          *proto.ColArr[uint32]
	MetaNetworkName        proto.ColStr
}

// NewColumns creates a new Columns instance with all columns initialized.
func NewColumns() *Columns {
	return &Columns{
		TransactionReturnValue: new(proto.ColStr).Nullable(),
		ReturnData:             new(proto.ColStr).Nullable(),
		Refund:                 new(proto.ColUInt64).Nullable(),
		Error:                  new(proto.ColStr).Nullable(),
		CallToAddress:          new(proto.ColStr).Nullable(),
		CallFramePath:          new(proto.ColUInt32).Array(),
	}
}

// Append adds a row to all columns.
func (c *Columns) Append(
	updatedDateTime time.Time,
	blockNumber uint64,
	txHash string,
	txIndex uint32,
	txGas uint64,
	txFailed bool,
	txReturnValue *string,
	index uint32,
	pc uint32,
	op string,
	gas uint64,
	gasCost uint64,
	gasUsed uint64,
	gasSelf uint64,
	depth uint64,
	returnData *string,
	refund *uint64,
	errStr *string,
	callTo *string,
	callFrameID uint32,
	callFramePath []uint32,
	network string,
) {
	c.UpdatedDateTime.Append(updatedDateTime)
	c.BlockNumber.Append(blockNumber)
	c.TransactionHash.Append(txHash)
	c.TransactionIndex.Append(txIndex)
	c.TransactionGas.Append(txGas)
	c.TransactionFailed.Append(txFailed)
	c.TransactionReturnValue.Append(nullableStr(txReturnValue))
	c.Index.Append(index)
	c.ProgramCounter.Append(pc)
	c.Operation.Append(op)
	c.Gas.Append(gas)
	c.GasCost.Append(gasCost)
	c.GasUsed.Append(gasUsed)
	c.GasSelf.Append(gasSelf)
	c.Depth.Append(depth)
	c.ReturnData.Append(nullableStr(returnData))
	c.Refund.Append(nullableUint64(refund))
	c.Error.Append(nullableStr(errStr))
	c.CallToAddress.Append(nullableStr(callTo))
	c.CallFrameID.Append(callFrameID)
	c.CallFramePath.Append(callFramePath)
	c.MetaNetworkName.Append(network)
}

// Reset clears all columns for reuse.
func (c *Columns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionHash.Reset()
	c.TransactionIndex.Reset()
	c.TransactionGas.Reset()
	c.TransactionFailed.Reset()
	c.TransactionReturnValue.Reset()
	c.Index.Reset()
	c.ProgramCounter.Reset()
	c.Operation.Reset()
	c.Gas.Reset()
	c.GasCost.Reset()
	c.GasUsed.Reset()
	c.GasSelf.Reset()
	c.Depth.Reset()
	c.ReturnData.Reset()
	c.Refund.Reset()
	c.Error.Reset()
	c.CallToAddress.Reset()
	c.CallFrameID.Reset()
	c.CallFramePath.Reset()
	c.MetaNetworkName.Reset()
}

// Input returns the proto.Input for inserting data.
func (c *Columns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "transaction_gas", Data: &c.TransactionGas},
		{Name: "transaction_failed", Data: &c.TransactionFailed},
		{Name: "transaction_return_value", Data: c.TransactionReturnValue},
		{Name: "index", Data: &c.Index},
		{Name: "program_counter", Data: &c.ProgramCounter},
		{Name: "operation", Data: &c.Operation},
		{Name: "gas", Data: &c.Gas},
		{Name: "gas_cost", Data: &c.GasCost},
		{Name: "gas_used", Data: &c.GasUsed},
		{Name: "gas_self", Data: &c.GasSelf},
		{Name: "depth", Data: &c.Depth},
		{Name: "return_data", Data: c.ReturnData},
		{Name: "refund", Data: c.Refund},
		{Name: "error", Data: c.Error},
		{Name: "call_to_address", Data: c.CallToAddress},
		{Name: "call_frame_id", Data: &c.CallFrameID},
		{Name: "call_frame_path", Data: c.CallFramePath},
		{Name: "meta_network_name", Data: &c.MetaNetworkName},
	}
}

// Rows returns the number of rows in the columns.
func (c *Columns) Rows() int {
	return c.BlockNumber.Rows()
}

// nullableStr converts a *string to proto.Nullable[string].
func nullableStr(s *string) proto.Nullable[string] {
	if s == nil {
		return proto.Null[string]()
	}

	return proto.NewNullable(*s)
}

// nullableUint64 converts a *uint64 to proto.Nullable[uint64].
func nullableUint64(v *uint64) proto.Nullable[uint64] {
	if v == nil {
		return proto.Null[uint64]()
	}

	return proto.NewNullable(*v)
}
