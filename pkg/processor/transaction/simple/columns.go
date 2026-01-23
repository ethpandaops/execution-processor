package simple

import (
	"math/big"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Columns holds all columns for transaction batch insert using ch-go columnar protocol.
type Columns struct {
	UpdatedDateTime    proto.ColDateTime
	BlockNumber        proto.ColUInt64
	BlockHash          proto.ColStr
	ParentHash         proto.ColStr
	Position           proto.ColUInt32
	Hash               proto.ColStr
	From               proto.ColStr
	To                 *proto.ColNullable[string]
	Nonce              proto.ColUInt64
	GasPrice           proto.ColUInt128
	Gas                proto.ColUInt64
	GasTipCap          *proto.ColNullable[proto.UInt128]
	GasFeeCap          *proto.ColNullable[proto.UInt128]
	Value              proto.ColUInt128
	Type               proto.ColUInt8
	Size               proto.ColUInt32
	CallDataSize       proto.ColUInt32
	BlobGas            *proto.ColNullable[uint64]
	BlobGasFeeCap      *proto.ColNullable[proto.UInt128]
	BlobHashes         *proto.ColArr[string]
	Success            proto.ColBool
	NInputBytes        proto.ColUInt32
	NInputZeroBytes    proto.ColUInt32
	NInputNonzeroBytes proto.ColUInt32
	MetaNetworkName    proto.ColStr
}

// NewColumns creates a new Columns instance with all nullable and array columns initialized.
func NewColumns() *Columns {
	return &Columns{
		To:            new(proto.ColStr).Nullable(),
		GasTipCap:     new(proto.ColUInt128).Nullable(),
		GasFeeCap:     new(proto.ColUInt128).Nullable(),
		BlobGas:       new(proto.ColUInt64).Nullable(),
		BlobGasFeeCap: new(proto.ColUInt128).Nullable(),
		BlobHashes:    new(proto.ColStr).Array(),
	}
}

// Append adds a Transaction row to all columns.
func (c *Columns) Append(tx Transaction) {
	c.UpdatedDateTime.Append(time.Time(tx.UpdatedDateTime))
	c.BlockNumber.Append(tx.BlockNumber)
	c.BlockHash.Append(tx.BlockHash)
	c.ParentHash.Append(tx.ParentHash)
	c.Position.Append(tx.Position)
	c.Hash.Append(tx.Hash)
	c.From.Append(tx.From)
	c.To.Append(nullableStr(tx.To))
	c.Nonce.Append(tx.Nonce)
	c.GasPrice.Append(parseUInt128(tx.GasPrice))
	c.Gas.Append(tx.Gas)
	c.GasTipCap.Append(nullableUInt128(tx.GasTipCap))
	c.GasFeeCap.Append(nullableUInt128(tx.GasFeeCap))
	c.Value.Append(parseUInt128(tx.Value))
	c.Type.Append(tx.Type)
	c.Size.Append(tx.Size)
	c.CallDataSize.Append(tx.CallDataSize)
	c.BlobGas.Append(nullableUint64(tx.BlobGas))
	c.BlobGasFeeCap.Append(nullableUInt128(tx.BlobGasFeeCap))
	c.BlobHashes.Append(tx.BlobHashes)
	c.Success.Append(tx.Success)
	c.NInputBytes.Append(tx.NInputBytes)
	c.NInputZeroBytes.Append(tx.NInputZeroBytes)
	c.NInputNonzeroBytes.Append(tx.NInputNonzeroBytes)
	c.MetaNetworkName.Append(tx.MetaNetworkName)
}

// Reset clears all columns for reuse.
func (c *Columns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.BlockHash.Reset()
	c.ParentHash.Reset()
	c.Position.Reset()
	c.Hash.Reset()
	c.From.Reset()
	c.To.Reset()
	c.Nonce.Reset()
	c.GasPrice.Reset()
	c.Gas.Reset()
	c.GasTipCap.Reset()
	c.GasFeeCap.Reset()
	c.Value.Reset()
	c.Type.Reset()
	c.Size.Reset()
	c.CallDataSize.Reset()
	c.BlobGas.Reset()
	c.BlobGasFeeCap.Reset()
	c.BlobHashes.Reset()
	c.Success.Reset()
	c.NInputBytes.Reset()
	c.NInputZeroBytes.Reset()
	c.NInputNonzeroBytes.Reset()
	c.MetaNetworkName.Reset()
}

// Input returns the proto.Input for inserting data.
func (c *Columns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "block_hash", Data: &c.BlockHash},
		{Name: "parent_hash", Data: &c.ParentHash},
		{Name: "position", Data: &c.Position},
		{Name: "hash", Data: &c.Hash},
		{Name: "from", Data: &c.From},
		{Name: "to", Data: c.To},
		{Name: "nonce", Data: &c.Nonce},
		{Name: "gas_price", Data: &c.GasPrice},
		{Name: "gas", Data: &c.Gas},
		{Name: "gas_tip_cap", Data: c.GasTipCap},
		{Name: "gas_fee_cap", Data: c.GasFeeCap},
		{Name: "value", Data: &c.Value},
		{Name: "type", Data: &c.Type},
		{Name: "size", Data: &c.Size},
		{Name: "call_data_size", Data: &c.CallDataSize},
		{Name: "blob_gas", Data: c.BlobGas},
		{Name: "blob_gas_fee_cap", Data: c.BlobGasFeeCap},
		{Name: "blob_hashes", Data: c.BlobHashes},
		{Name: "success", Data: &c.Success},
		{Name: "n_input_bytes", Data: &c.NInputBytes},
		{Name: "n_input_zero_bytes", Data: &c.NInputZeroBytes},
		{Name: "n_input_nonzero_bytes", Data: &c.NInputNonzeroBytes},
		{Name: "meta_network_name", Data: &c.MetaNetworkName},
	}
}

// Rows returns the number of rows in the columns.
func (c *Columns) Rows() int {
	return c.BlockNumber.Rows()
}

// parseUInt128 parses a decimal string to proto.UInt128.
// Returns zero if the string is empty or invalid.
func parseUInt128(s string) proto.UInt128 {
	if s == "" {
		return proto.UInt128{}
	}

	bi := new(big.Int)
	if _, ok := bi.SetString(s, 10); !ok {
		return proto.UInt128{}
	}

	// Extract low and high 64-bit words
	// UInt128 = High * 2^64 + Low
	maxUint64 := new(big.Int).SetUint64(^uint64(0))
	maxUint64.Add(maxUint64, big.NewInt(1)) // 2^64

	low := new(big.Int).And(bi, new(big.Int).Sub(maxUint64, big.NewInt(1)))
	high := new(big.Int).Rsh(bi, 64)

	return proto.UInt128{
		Low:  low.Uint64(),
		High: high.Uint64(),
	}
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

// nullableUInt128 converts a *string (decimal) to proto.Nullable[proto.UInt128].
func nullableUInt128(s *string) proto.Nullable[proto.UInt128] {
	if s == nil {
		return proto.Null[proto.UInt128]()
	}

	return proto.NewNullable(parseUInt128(*s))
}
