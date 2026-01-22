package simple

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
)

// Transaction represents a row in the execution_transaction table.
//
//nolint:tagliatelle // ClickHouse uses snake_case column names
type Transaction struct {
	UpdatedDateTime    structlog.ClickHouseTime `json:"updated_date_time"`
	BlockNumber        uint64                   `json:"block_number"`
	BlockHash          string                   `json:"block_hash"`
	ParentHash         string                   `json:"parent_hash"`
	Position           uint32                   `json:"position"`
	Hash               string                   `json:"hash"`
	From               string                   `json:"from"`
	To                 *string                  `json:"to"`
	Nonce              uint64                   `json:"nonce"`
	GasPrice           string                   `json:"gas_price"`        // Effective gas price as UInt128 string
	Gas                uint64                   `json:"gas"`              // Gas limit
	GasTipCap          *string                  `json:"gas_tip_cap"`      // Nullable UInt128 string
	GasFeeCap          *string                  `json:"gas_fee_cap"`      // Nullable UInt128 string
	Value              string                   `json:"value"`            // UInt128 string
	Type               uint8                    `json:"type"`             // Transaction type
	Size               uint32                   `json:"size"`             // Transaction size in bytes
	CallDataSize       uint32                   `json:"call_data_size"`   // Size of call data
	BlobGas            *uint64                  `json:"blob_gas"`         // Nullable - for type 3 txs
	BlobGasFeeCap      *string                  `json:"blob_gas_fee_cap"` // Nullable UInt128 string
	BlobHashes         []string                 `json:"blob_hashes"`      // Array of versioned hashes
	Success            bool                     `json:"success"`
	NInputBytes        uint32                   `json:"n_input_bytes"`
	NInputZeroBytes    uint32                   `json:"n_input_zero_bytes"`
	NInputNonzeroBytes uint32                   `json:"n_input_nonzero_bytes"`
	MetaNetworkID      int32                    `json:"meta_network_id"`
	MetaNetworkName    string                   `json:"meta_network_name"`
}

// GetHandlers returns the task handlers for this processor.
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		ProcessForwardsTaskType:  p.handleProcessTask,
		ProcessBackwardsTaskType: p.handleProcessTask,
	}
}

// handleProcessTask handles block processing tasks.
func (p *Processor) handleProcessTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(
			p.network.Name,
			ProcessorName,
			c.ProcessForwardsQueue(ProcessorName),
			task.Type(),
		).Observe(duration.Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(
			p.network.Name,
			ProcessorName,
			c.ProcessForwardsQueue(ProcessorName),
			task.Type(),
			"unmarshal_error",
		).Inc()

		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	blockNumber := &payload.BlockNumber

	p.log.WithField("block", blockNumber.Uint64()).Debug("Processing block")

	// Get block
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	block, err := node.BlockByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	blockTxs := block.Transactions()

	// Build receipt map - try batch first, fall back to per-tx
	receiptMap := make(map[string]execution.Receipt, len(blockTxs))

	receipts, err := node.BlockReceipts(ctx, blockNumber)
	if err == nil {
		// Use batch receipts
		for _, r := range receipts {
			receiptMap[r.TxHash().Hex()] = r
		}
	}

	// Process all transactions in block
	transactions := make([]Transaction, 0, len(blockTxs))

	for index, tx := range blockTxs {
		txHash := tx.Hash().Hex()
		receipt, ok := receiptMap[txHash]

		if !ok {
			// Fallback: fetch individual receipt
			var receiptErr error

			receipt, receiptErr = node.TransactionReceipt(ctx, tx.Hash().String())
			if receiptErr != nil {
				return fmt.Errorf("failed to get receipt for tx %s: %w", txHash, receiptErr)
			}
		}

		// Build transaction row
		txRow, buildErr := p.buildTransactionRow(block, tx, receipt, uint64(index)) //nolint:gosec // index is bounded
		if buildErr != nil {
			return fmt.Errorf("failed to build transaction row: %w", buildErr)
		}

		transactions = append(transactions, txRow)
	}

	// Insert all transactions
	if len(transactions) > 0 {
		if insertErr := p.insertTransactions(ctx, transactions); insertErr != nil {
			common.TasksErrored.WithLabelValues(
				p.network.Name,
				ProcessorName,
				c.ProcessForwardsQueue(ProcessorName),
				task.Type(),
				"insert_error",
			).Inc()

			return fmt.Errorf("failed to insert transactions: %w", insertErr)
		}
	}

	common.TasksProcessed.WithLabelValues(
		p.network.Name,
		ProcessorName,
		c.ProcessForwardsQueue(ProcessorName),
		task.Type(),
		"success",
	).Inc()

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.Uint64(),
		"tx_count":     len(transactions),
	}).Info("Processed block")

	return nil
}

// buildTransactionRow builds a transaction row from block, tx, and receipt data.
func (p *Processor) buildTransactionRow(
	block execution.Block,
	tx execution.Transaction,
	receipt execution.Receipt,
	index uint64,
) (Transaction, error) {
	// Get sender (from) - computed by the data source
	from := tx.From()

	// Build to address (nil for contract creation)
	var toAddress *string

	if tx.To() != nil {
		addr := tx.To().Hex()
		toAddress = &addr
	}

	// Calculate input byte stats
	inputData := tx.Data()
	callDataSize := uint32(len(inputData)) //nolint:gosec // data length is bounded

	var nInputZeroBytes, nInputNonzeroBytes uint32

	for _, b := range inputData {
		if b == 0 {
			nInputZeroBytes++
		} else {
			nInputNonzeroBytes++
		}
	}

	// Calculate effective gas price
	gasPrice := calculateEffectiveGasPrice(block, tx)

	// Get EIP-1559 fee fields (nullable for legacy transactions)
	var gasTipCap, gasFeeCap *string

	if tx.Type() >= 2 { // EIP-1559 and later
		if tx.GasTipCap() != nil {
			tipCap := tx.GasTipCap().String()
			gasTipCap = &tipCap
		}

		if tx.GasFeeCap() != nil {
			feeCap := tx.GasFeeCap().String()
			gasFeeCap = &feeCap
		}
	}

	// Get transaction value
	value := "0"
	if tx.Value() != nil {
		value = tx.Value().String()
	}

	// Get transaction size
	txSize := uint32(tx.Size()) //nolint:gosec // tx size is bounded

	// Build transaction row
	txRow := Transaction{
		UpdatedDateTime:    structlog.NewClickHouseTime(time.Now()),
		BlockNumber:        block.Number().Uint64(),
		BlockHash:          block.Hash().String(),
		ParentHash:         block.ParentHash().String(),
		Position:           uint32(index), //nolint:gosec // index is bounded by block size
		Hash:               tx.Hash().String(),
		From:               from.Hex(),
		To:                 toAddress,
		Nonce:              tx.Nonce(),
		GasPrice:           gasPrice.String(),
		Gas:                tx.Gas(),
		GasTipCap:          gasTipCap,
		GasFeeCap:          gasFeeCap,
		Value:              value,
		Type:               tx.Type(),
		Size:               txSize,
		CallDataSize:       callDataSize,
		BlobHashes:         []string{}, // Default empty array
		Success:            receipt.Status() == 1,
		NInputBytes:        callDataSize,
		NInputZeroBytes:    nInputZeroBytes,
		NInputNonzeroBytes: nInputNonzeroBytes,
		MetaNetworkID:      p.network.ID,
		MetaNetworkName:    p.network.Name,
	}

	// Handle blob transaction fields (type 3)
	if tx.Type() == execution.BlobTxType {
		blobGas := tx.BlobGas()
		txRow.BlobGas = &blobGas

		if tx.BlobGasFeeCap() != nil {
			blobGasFeeCap := tx.BlobGasFeeCap().String()
			txRow.BlobGasFeeCap = &blobGasFeeCap
		}

		// Get versioned hashes
		blobHashes := tx.BlobHashes()
		txRow.BlobHashes = make([]string, len(blobHashes))

		for i, hash := range blobHashes {
			txRow.BlobHashes[i] = hash.String()
		}
	}

	return txRow, nil
}

// calculateEffectiveGasPrice calculates the effective gas price for a transaction.
// For legacy/access list txs: returns tx.GasPrice().
// For EIP-1559+ txs: returns min(max_fee_per_gas, base_fee + max_priority_fee_per_gas).
func calculateEffectiveGasPrice(block execution.Block, tx execution.Transaction) *big.Int {
	txType := tx.Type()

	// Legacy and access list transactions use GasPrice directly
	if txType == execution.LegacyTxType || txType == execution.AccessListTxType {
		if tx.GasPrice() != nil {
			return tx.GasPrice()
		}

		return big.NewInt(0)
	}

	// EIP-1559, blob, and 7702 transactions need effective gas price calculation
	baseFee := block.BaseFee()
	if baseFee == nil {
		// Fallback for pre-EIP-1559 blocks (shouldn't happen for type 2+ txs)
		if tx.GasPrice() != nil {
			return tx.GasPrice()
		}

		return big.NewInt(0)
	}

	// Effective gas price = min(max_fee_per_gas, base_fee + max_priority_fee_per_gas)
	effectiveGasPrice := new(big.Int).Add(baseFee, tx.GasTipCap())
	if effectiveGasPrice.Cmp(tx.GasFeeCap()) > 0 {
		effectiveGasPrice = tx.GasFeeCap()
	}

	return effectiveGasPrice
}

// insertTransactions inserts transactions into ClickHouse.
func (p *Processor) insertTransactions(ctx context.Context, transactions []Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	if err := p.clickhouse.BulkInsert(ctx, p.config.Table, transactions); err != nil {
		common.ClickHouseInsertsRows.WithLabelValues(
			p.network.Name,
			ProcessorName,
			p.config.Table,
			"failed",
			"",
		).Add(float64(len(transactions)))

		return fmt.Errorf("failed to insert transactions: %w", err)
	}

	common.ClickHouseInsertsRows.WithLabelValues(
		p.network.Name,
		ProcessorName,
		p.config.Table,
		"success",
		"",
	).Add(float64(len(transactions)))

	return nil
}
