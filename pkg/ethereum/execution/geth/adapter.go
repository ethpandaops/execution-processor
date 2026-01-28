//go:build !embedded

// Package geth provides go-ethereum adapters for the execution interfaces.
// This package contains all go-ethereum dependencies, allowing the core
// execution package to remain free of CGO-dependent imports.
package geth

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// Compile-time interface checks.
var (
	_ execution.Block       = (*BlockAdapter)(nil)
	_ execution.Transaction = (*TransactionAdapter)(nil)
	_ execution.Receipt     = (*ReceiptAdapter)(nil)
)

// BlockAdapter wraps a go-ethereum Block to implement execution.Block.
type BlockAdapter struct {
	block *types.Block
	txs   []execution.Transaction
}

// NewBlockAdapter creates a new BlockAdapter from a go-ethereum Block.
// It extracts sender addresses for all transactions using the appropriate signer.
func NewBlockAdapter(block *types.Block) *BlockAdapter {
	gethTxs := block.Transactions()
	txs := make([]execution.Transaction, len(gethTxs))

	for i, tx := range gethTxs {
		txs[i] = NewTransactionAdapter(tx)
	}

	return &BlockAdapter{
		block: block,
		txs:   txs,
	}
}

// Number returns the block number.
func (b *BlockAdapter) Number() *big.Int {
	return b.block.Number()
}

// Hash returns the block hash.
func (b *BlockAdapter) Hash() execution.Hash {
	return execution.Hash(b.block.Hash())
}

// ParentHash returns the parent block hash.
func (b *BlockAdapter) ParentHash() execution.Hash {
	return execution.Hash(b.block.ParentHash())
}

// BaseFee returns the base fee per gas (EIP-1559), or nil for pre-London blocks.
func (b *BlockAdapter) BaseFee() *big.Int {
	return b.block.BaseFee()
}

// Transactions returns all transactions in the block.
func (b *BlockAdapter) Transactions() []execution.Transaction {
	return b.txs
}

// TransactionAdapter wraps a go-ethereum Transaction to implement execution.Transaction.
type TransactionAdapter struct {
	tx   *types.Transaction
	from common.Address
}

// NewTransactionAdapter creates a new TransactionAdapter from a go-ethereum Transaction.
// It computes the sender address using the appropriate signer.
func NewTransactionAdapter(tx *types.Transaction) *TransactionAdapter {
	// Determine the appropriate signer for extracting the sender
	var signer types.Signer

	chainID := tx.ChainId()
	if chainID == nil || chainID.Sign() == 0 {
		// Legacy transaction without EIP-155 replay protection
		signer = types.HomesteadSigner{}
	} else {
		signer = types.LatestSignerForChainID(chainID)
	}

	// Extract sender - this uses go-ethereum's crypto package internally
	from, _ := types.Sender(signer, tx)

	return &TransactionAdapter{
		tx:   tx,
		from: from,
	}
}

// Hash returns the transaction hash.
func (t *TransactionAdapter) Hash() execution.Hash {
	return execution.Hash(t.tx.Hash())
}

// Type returns the transaction type.
func (t *TransactionAdapter) Type() uint8 {
	return t.tx.Type()
}

// To returns the recipient address, or nil for contract creation.
func (t *TransactionAdapter) To() *execution.Address {
	if t.tx.To() == nil {
		return nil
	}

	addr := execution.Address(*t.tx.To())

	return &addr
}

// From returns the sender address.
func (t *TransactionAdapter) From() execution.Address {
	return execution.Address(t.from)
}

// Nonce returns the sender account nonce.
func (t *TransactionAdapter) Nonce() uint64 {
	return t.tx.Nonce()
}

// Gas returns the gas limit.
func (t *TransactionAdapter) Gas() uint64 {
	return t.tx.Gas()
}

// GasPrice returns the gas price (for legacy transactions).
func (t *TransactionAdapter) GasPrice() *big.Int {
	return t.tx.GasPrice()
}

// GasTipCap returns the max priority fee per gas (EIP-1559).
func (t *TransactionAdapter) GasTipCap() *big.Int {
	return t.tx.GasTipCap()
}

// GasFeeCap returns the max fee per gas (EIP-1559).
func (t *TransactionAdapter) GasFeeCap() *big.Int {
	return t.tx.GasFeeCap()
}

// Value returns the value transferred in wei.
func (t *TransactionAdapter) Value() *big.Int {
	return t.tx.Value()
}

// Data returns the input data (calldata).
func (t *TransactionAdapter) Data() []byte {
	return t.tx.Data()
}

// Size returns the encoded transaction size in bytes.
func (t *TransactionAdapter) Size() uint64 {
	return t.tx.Size()
}

// ChainId returns the chain ID, or nil for legacy transactions.
func (t *TransactionAdapter) ChainId() *big.Int {
	return t.tx.ChainId()
}

// BlobGas returns the blob gas used (for blob transactions).
func (t *TransactionAdapter) BlobGas() uint64 {
	return t.tx.BlobGas()
}

// BlobGasFeeCap returns the max blob fee per gas (for blob transactions).
func (t *TransactionAdapter) BlobGasFeeCap() *big.Int {
	return t.tx.BlobGasFeeCap()
}

// BlobHashes returns the versioned hashes (for blob transactions).
func (t *TransactionAdapter) BlobHashes() []execution.Hash {
	gethHashes := t.tx.BlobHashes()
	hashes := make([]execution.Hash, len(gethHashes))

	for i, h := range gethHashes {
		hashes[i] = execution.Hash(h)
	}

	return hashes
}

// ReceiptAdapter wraps a go-ethereum Receipt to implement execution.Receipt.
type ReceiptAdapter struct {
	receipt *types.Receipt
}

// NewReceiptAdapter creates a new ReceiptAdapter from a go-ethereum Receipt.
func NewReceiptAdapter(receipt *types.Receipt) *ReceiptAdapter {
	return &ReceiptAdapter{receipt: receipt}
}

// Status returns the transaction status (1=success, 0=failure).
func (r *ReceiptAdapter) Status() uint64 {
	return r.receipt.Status
}

// TxHash returns the transaction hash.
func (r *ReceiptAdapter) TxHash() execution.Hash {
	return execution.Hash(r.receipt.TxHash)
}

// GasUsed returns the gas used by the transaction.
func (r *ReceiptAdapter) GasUsed() uint64 {
	return r.receipt.GasUsed
}

// AdaptReceipts converts a slice of go-ethereum receipts to execution.Receipt interfaces.
func AdaptReceipts(receipts []*types.Receipt) []execution.Receipt {
	result := make([]execution.Receipt, len(receipts))

	for i, r := range receipts {
		result[i] = NewReceiptAdapter(r)
	}

	return result
}
