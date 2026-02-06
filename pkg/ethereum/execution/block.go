package execution

import "math/big"

// Hash represents a 32-byte hash.
type Hash [32]byte

// Hex returns the hex string representation of the hash.
func (h Hash) Hex() string {
	return "0x" + encodeHex(h[:])
}

// String returns the hex string representation of the hash.
func (h Hash) String() string {
	return h.Hex()
}

// Address represents a 20-byte Ethereum address.
type Address [20]byte

// Hex returns the hex string representation of the address with checksum.
func (a Address) Hex() string {
	return "0x" + encodeHex(a[:])
}

// String returns the hex string representation of the address.
func (a Address) String() string {
	return a.Hex()
}

// encodeHex encodes bytes as hex string without 0x prefix.
func encodeHex(b []byte) string {
	const hexChars = "0123456789abcdef"

	result := make([]byte, len(b)*2)

	for i, v := range b {
		result[i*2] = hexChars[v>>4]
		result[i*2+1] = hexChars[v&0x0f]
	}

	return string(result)
}

// Transaction type constants matching go-ethereum values.
const (
	LegacyTxType     = 0
	AccessListTxType = 1
	DynamicFeeTxType = 2
	BlobTxType       = 3
)

// Block interface defines methods for accessing block data.
// Implementations are provided by data sources (RPC, embedded clients).
type Block interface {
	// Number returns the block number.
	Number() *big.Int

	// Hash returns the block hash.
	Hash() Hash

	// ParentHash returns the parent block hash.
	ParentHash() Hash

	// BaseFee returns the base fee per gas (EIP-1559), or nil for pre-London blocks.
	BaseFee() *big.Int

	// Transactions returns all transactions in the block.
	Transactions() []Transaction
}

// Transaction interface defines methods for accessing transaction data.
// The From() method returns the sender address, computed by the data source
// using its own crypto implementation (avoiding go-ethereum crypto imports).
type Transaction interface {
	// Hash returns the transaction hash.
	Hash() Hash

	// Type returns the transaction type (0=legacy, 1=access list, 2=dynamic fee, 3=blob).
	Type() uint8

	// To returns the recipient address, or nil for contract creation.
	To() *Address

	// From returns the sender address.
	// This is computed by the data source using types.Sender() or equivalent.
	From() Address

	// Nonce returns the sender account nonce.
	Nonce() uint64

	// Gas returns the gas limit.
	Gas() uint64

	// GasPrice returns the gas price (for legacy transactions).
	GasPrice() *big.Int

	// GasTipCap returns the max priority fee per gas (EIP-1559).
	GasTipCap() *big.Int

	// GasFeeCap returns the max fee per gas (EIP-1559).
	GasFeeCap() *big.Int

	// Value returns the value transferred in wei.
	Value() *big.Int

	// Data returns the input data (calldata).
	Data() []byte

	// Size returns the encoded transaction size in bytes.
	Size() uint64

	// ChainId returns the chain ID, or nil for legacy transactions.
	ChainId() *big.Int

	// BlobGas returns the blob gas used (for blob transactions).
	BlobGas() uint64

	// BlobGasFeeCap returns the max blob fee per gas (for blob transactions).
	BlobGasFeeCap() *big.Int

	// BlobHashes returns the versioned hashes (for blob transactions).
	BlobHashes() []Hash
}

// Receipt interface defines methods for accessing transaction receipt data.
type Receipt interface {
	// Status returns the transaction status (1=success, 0=failure).
	Status() uint64

	// TxHash returns the transaction hash.
	TxHash() Hash

	// GasUsed returns the post-refund gas used by the transaction (what the user pays).
	//
	// EIP-7778 context: This remains post-refund. The EIP-7778 split between receipt gas
	// and block gas only affects ExecutionResult at the EVM layer; the Receipt's GasUsed
	// field and its derivation from CumulativeGasUsed are unchanged.
	GasUsed() uint64
}
