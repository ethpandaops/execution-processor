package structlog_agg

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

// Columns holds all columns for structlog_agg batch insert using ch-go columnar protocol.
type Columns struct {
	UpdatedDateTime     proto.ColDateTime
	BlockNumber         proto.ColUInt64
	TransactionHash     proto.ColStr
	TransactionIndex    proto.ColUInt32
	CallFrameID         proto.ColUInt32
	ParentCallFrameID   *proto.ColNullable[uint32]
	CallFramePath       *proto.ColArr[uint32] // Path from root to this frame
	Depth               proto.ColUInt32
	TargetAddress       *proto.ColNullable[string]
	CallType            proto.ColStr
	Operation           proto.ColStr // Empty string for summary row, opcode name for per-opcode rows
	OpcodeCount         proto.ColUInt64
	ErrorCount          proto.ColUInt64
	Gas                 proto.ColUInt64 // SUM(gas_self) - excludes child frame gas
	GasCumulative       proto.ColUInt64 // For summary: frame gas_cumulative; for per-opcode: SUM(gas_used)
	MinDepth            proto.ColUInt32 // Per-opcode: MIN(depth); summary: same as Depth
	MaxDepth            proto.ColUInt32 // Per-opcode: MAX(depth); summary: same as Depth
	GasRefund           *proto.ColNullable[uint64]
	IntrinsicGas        *proto.ColNullable[uint64]
	MemWordsSumBefore   proto.ColUInt64
	MemWordsSumAfter    proto.ColUInt64
	MemWordsSqSumBefore proto.ColUInt64
	MemWordsSqSumAfter  proto.ColUInt64
	ColdAccessCount     proto.ColUInt64
	MetaNetworkName     proto.ColStr
}

// NewColumns creates a new Columns instance with all columns initialized.
func NewColumns() *Columns {
	return &Columns{
		ParentCallFrameID: new(proto.ColUInt32).Nullable(),
		CallFramePath:     new(proto.ColUInt32).Array(),
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
	callFramePath []uint32,
	depth uint32,
	targetAddress *string,
	callType string,
	operation string,
	opcodeCount uint64,
	errorCount uint64,
	gas uint64,
	gasCumulative uint64,
	minDepth uint32,
	maxDepth uint32,
	gasRefund *uint64,
	intrinsicGas *uint64,
	memWordsSumBefore uint64,
	memWordsSumAfter uint64,
	memWordsSqSumBefore uint64,
	memWordsSqSumAfter uint64,
	coldAccessCount uint64,
	network string,
) {
	c.UpdatedDateTime.Append(updatedDateTime)
	c.BlockNumber.Append(blockNumber)
	c.TransactionHash.Append(txHash)
	c.TransactionIndex.Append(txIndex)
	c.CallFrameID.Append(callFrameID)
	c.ParentCallFrameID.Append(nullableUint32(parentCallFrameID))
	c.CallFramePath.Append(callFramePath)
	c.Depth.Append(depth)
	c.TargetAddress.Append(nullableStr(targetAddress))
	c.CallType.Append(callType)
	c.Operation.Append(operation)
	c.OpcodeCount.Append(opcodeCount)
	c.ErrorCount.Append(errorCount)
	c.Gas.Append(gas)
	c.GasCumulative.Append(gasCumulative)
	c.MinDepth.Append(minDepth)
	c.MaxDepth.Append(maxDepth)
	c.GasRefund.Append(nullableUint64(gasRefund))
	c.IntrinsicGas.Append(nullableUint64(intrinsicGas))
	c.MemWordsSumBefore.Append(memWordsSumBefore)
	c.MemWordsSumAfter.Append(memWordsSumAfter)
	c.MemWordsSqSumBefore.Append(memWordsSqSumBefore)
	c.MemWordsSqSumAfter.Append(memWordsSqSumAfter)
	c.ColdAccessCount.Append(coldAccessCount)
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
	c.CallFramePath.Reset()
	c.Depth.Reset()
	c.TargetAddress.Reset()
	c.CallType.Reset()
	c.Operation.Reset()
	c.OpcodeCount.Reset()
	c.ErrorCount.Reset()
	c.Gas.Reset()
	c.GasCumulative.Reset()
	c.MinDepth.Reset()
	c.MaxDepth.Reset()
	c.GasRefund.Reset()
	c.IntrinsicGas.Reset()
	c.MemWordsSumBefore.Reset()
	c.MemWordsSumAfter.Reset()
	c.MemWordsSqSumBefore.Reset()
	c.MemWordsSqSumAfter.Reset()
	c.ColdAccessCount.Reset()
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
		{Name: "call_frame_path", Data: c.CallFramePath},
		{Name: "depth", Data: &c.Depth},
		{Name: "target_address", Data: c.TargetAddress},
		{Name: "call_type", Data: &c.CallType},
		{Name: "operation", Data: &c.Operation},
		{Name: "opcode_count", Data: &c.OpcodeCount},
		{Name: "error_count", Data: &c.ErrorCount},
		{Name: "gas", Data: &c.Gas},
		{Name: "gas_cumulative", Data: &c.GasCumulative},
		{Name: "min_depth", Data: &c.MinDepth},
		{Name: "max_depth", Data: &c.MaxDepth},
		{Name: "gas_refund", Data: c.GasRefund},
		{Name: "intrinsic_gas", Data: c.IntrinsicGas},
		{Name: "memory_words_sum_before", Data: &c.MemWordsSumBefore},
		{Name: "memory_words_sum_after", Data: &c.MemWordsSumAfter},
		{Name: "memory_words_sq_sum_before", Data: &c.MemWordsSqSumBefore},
		{Name: "memory_words_sq_sum_after", Data: &c.MemWordsSqSumAfter},
		{Name: "cold_access_count", Data: &c.ColdAccessCount},
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
