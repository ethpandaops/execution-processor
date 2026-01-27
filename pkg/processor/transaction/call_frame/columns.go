package call_frame

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

// Columns holds all columns for call_frame batch insert using ch-go columnar protocol.
type Columns struct {
	UpdatedDateTime   proto.ColDateTime
	BlockNumber       proto.ColUInt64
	TransactionHash   proto.ColStr
	TransactionIndex  proto.ColUInt32
	CallFrameID       proto.ColUInt32
	ParentCallFrameID *proto.ColNullable[uint32]
	Depth             proto.ColUInt32
	TargetAddress     *proto.ColNullable[string]
	CallType          proto.ColStr
	OpcodeCount       proto.ColUInt64
	ErrorCount        proto.ColUInt64
	Gas               proto.ColUInt64
	GasCumulative     proto.ColUInt64
	GasRefund         *proto.ColNullable[uint64]
	IntrinsicGas      *proto.ColNullable[uint64]
	MetaNetworkName   proto.ColStr
}

// NewColumns creates a new Columns instance with all columns initialized.
func NewColumns() *Columns {
	return &Columns{
		ParentCallFrameID: new(proto.ColUInt32).Nullable(),
		TargetAddress:     new(proto.ColStr).Nullable(),
		GasRefund:         new(proto.ColUInt64).Nullable(),
		IntrinsicGas:      new(proto.ColUInt64).Nullable(),
	}
}

// Append adds a row to all columns.
func (c *Columns) Append(
	updatedDateTime time.Time,
	blockNumber uint64,
	txHash string,
	txIndex uint32,
	callFrameID uint32,
	parentCallFrameID *uint32,
	depth uint32,
	targetAddress *string,
	callType string,
	opcodeCount uint64,
	errorCount uint64,
	gas uint64,
	gasCumulative uint64,
	gasRefund *uint64,
	intrinsicGas *uint64,
	network string,
) {
	c.UpdatedDateTime.Append(updatedDateTime)
	c.BlockNumber.Append(blockNumber)
	c.TransactionHash.Append(txHash)
	c.TransactionIndex.Append(txIndex)
	c.CallFrameID.Append(callFrameID)
	c.ParentCallFrameID.Append(nullableUint32(parentCallFrameID))
	c.Depth.Append(depth)
	c.TargetAddress.Append(nullableStr(targetAddress))
	c.CallType.Append(callType)
	c.OpcodeCount.Append(opcodeCount)
	c.ErrorCount.Append(errorCount)
	c.Gas.Append(gas)
	c.GasCumulative.Append(gasCumulative)
	c.GasRefund.Append(nullableUint64(gasRefund))
	c.IntrinsicGas.Append(nullableUint64(intrinsicGas))
	c.MetaNetworkName.Append(network)
}

// Reset clears all columns for reuse.
func (c *Columns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionHash.Reset()
	c.TransactionIndex.Reset()
	c.CallFrameID.Reset()
	c.ParentCallFrameID.Reset()
	c.Depth.Reset()
	c.TargetAddress.Reset()
	c.CallType.Reset()
	c.OpcodeCount.Reset()
	c.ErrorCount.Reset()
	c.Gas.Reset()
	c.GasCumulative.Reset()
	c.GasRefund.Reset()
	c.IntrinsicGas.Reset()
	c.MetaNetworkName.Reset()
}

// Input returns the proto.Input for inserting data.
func (c *Columns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "transaction_index", Data: &c.TransactionIndex},
		{Name: "call_frame_id", Data: &c.CallFrameID},
		{Name: "parent_call_frame_id", Data: c.ParentCallFrameID},
		{Name: "depth", Data: &c.Depth},
		{Name: "target_address", Data: c.TargetAddress},
		{Name: "call_type", Data: &c.CallType},
		{Name: "opcode_count", Data: &c.OpcodeCount},
		{Name: "error_count", Data: &c.ErrorCount},
		{Name: "gas", Data: &c.Gas},
		{Name: "gas_cumulative", Data: &c.GasCumulative},
		{Name: "gas_refund", Data: c.GasRefund},
		{Name: "intrinsic_gas", Data: c.IntrinsicGas},
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

// nullableUint32 converts a *uint32 to proto.Nullable[uint32].
func nullableUint32(v *uint32) proto.Nullable[uint32] {
	if v == nil {
		return proto.Null[uint32]()
	}

	return proto.NewNullable(*v)
}

// nullableUint64 converts a *uint64 to proto.Nullable[uint64].
func nullableUint64(v *uint64) proto.Nullable[uint64] {
	if v == nil {
		return proto.Null[uint64]()
	}

	return proto.NewNullable(*v)
}
