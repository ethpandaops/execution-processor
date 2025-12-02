package simple

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
)

// Transaction represents a row in the transaction table.
//
//nolint:tagliatelle // ClickHouse uses snake_case column names
type Transaction struct {
	UpdatedDateTime      structlog.ClickHouseTime `json:"updated_date_time"`
	BlockNumber          uint64                   `json:"block_number"`
	BlockHash            string                   `json:"block_hash"`
	ParentHash           string                   `json:"parent_hash"`
	TransactionIndex     uint64                   `json:"transaction_index"`
	TransactionHash      string                   `json:"transaction_hash"`
	Nonce                uint64                   `json:"nonce"`
	FromAddress          string                   `json:"from_address"`
	ToAddress            *string                  `json:"to_address"`
	Value                *big.Int                 `json:"value"` // Wei as UInt256
	Input                *string                  `json:"input"` // Hex-encoded input data
	GasLimit             uint64                   `json:"gas_limit"`
	GasUsed              uint64                   `json:"gas_used"`
	GasPrice             uint64                   `json:"gas_price"`
	TransactionType      uint32                   `json:"transaction_type"`
	MaxPriorityFeePerGas uint64                   `json:"max_priority_fee_per_gas"`
	MaxFeePerGas         uint64                   `json:"max_fee_per_gas"`
	Success              bool                     `json:"success"`
	NInputBytes          uint32                   `json:"n_input_bytes"`
	NInputZeroBytes      uint32                   `json:"n_input_zero_bytes"`
	NInputNonzeroBytes   uint32                   `json:"n_input_nonzero_bytes"`
	MetaNetworkID        int32                    `json:"meta_network_id"`
	MetaNetworkName      string                   `json:"meta_network_name"`
}

// GetHandlers returns the task handlers for this processor.
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		ProcessForwardsTaskType:  p.handleProcessTask,
		ProcessBackwardsTaskType: p.handleProcessTask,
		VerifyForwardsTaskType:   p.handleVerifyTask,
		VerifyBackwardsTaskType:  p.handleVerifyTask,
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
	receiptMap := make(map[string]*types.Receipt, len(blockTxs))

	receipts, err := node.BlockReceipts(ctx, blockNumber)
	if err == nil {
		// Use batch receipts
		for _, r := range receipts {
			receiptMap[r.TxHash.Hex()] = r
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

	// Enqueue verify task
	verifyPayload := &VerifyPayload{
		BlockNumber:   payload.BlockNumber,
		NetworkID:     payload.NetworkID,
		NetworkName:   payload.NetworkName,
		InsertedCount: len(transactions),
	}

	var verifyTask *asynq.Task

	var queue string

	if payload.ProcessingMode == c.BACKWARDS_MODE {
		verifyTask, err = NewVerifyBackwardsTask(verifyPayload)
		queue = p.getVerifyBackwardsQueue()
	} else {
		verifyTask, err = NewVerifyForwardsTask(verifyPayload)
		queue = p.getVerifyForwardsQueue()
	}

	if err != nil {
		return fmt.Errorf("failed to create verify task: %w", err)
	}

	if err := p.EnqueueTask(ctx, verifyTask,
		asynq.Queue(queue),
		asynq.ProcessIn(10*time.Second),
	); err != nil {
		p.log.WithError(err).Warn("Failed to enqueue verify task")
	}

	common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, verifyTask.Type()).Inc()

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.Uint64(),
		"tx_count":     len(transactions),
	}).Info("Processed block")

	return nil
}

// buildTransactionRow builds a transaction row from block, tx, and receipt data.
func (p *Processor) buildTransactionRow(
	block *types.Block,
	tx *types.Transaction,
	receipt *types.Receipt,
	index uint64,
) (Transaction, error) {
	// Get sender (from) - handle legacy transactions without chain ID
	var signer types.Signer

	chainID := tx.ChainId()
	if chainID == nil || chainID.Sign() == 0 {
		// Legacy transaction without EIP-155 replay protection
		signer = types.HomesteadSigner{}
	} else {
		signer = types.LatestSignerForChainID(chainID)
	}

	from, err := types.Sender(signer, tx)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get sender: %w", err)
	}

	// Build to address (nil for contract creation)
	var toAddress *string

	if tx.To() != nil {
		addr := tx.To().Hex()
		toAddress = &addr
	}

	// Build input data
	var input *string

	inputData := tx.Data()
	if len(inputData) > 0 {
		inputHex := "0x" + hex.EncodeToString(inputData)
		input = &inputHex
	}

	// Calculate input byte stats
	nInputBytes := uint32(len(inputData)) //nolint:gosec // data length is bounded

	var nInputZeroBytes, nInputNonzeroBytes uint32

	for _, b := range inputData {
		if b == 0 {
			nInputZeroBytes++
		} else {
			nInputNonzeroBytes++
		}
	}

	// Get gas price (for legacy transactions)
	var gasPrice uint64
	if tx.GasPrice() != nil {
		gasPrice = tx.GasPrice().Uint64()
	}

	// Get EIP-1559 fee fields
	var maxPriorityFeePerGas, maxFeePerGas uint64

	if tx.GasTipCap() != nil {
		maxPriorityFeePerGas = tx.GasTipCap().Uint64()
	}

	if tx.GasFeeCap() != nil {
		maxFeePerGas = tx.GasFeeCap().Uint64()
	}

	return Transaction{
		UpdatedDateTime:      structlog.NewClickHouseTime(time.Now()),
		BlockNumber:          block.Number().Uint64(),
		BlockHash:            block.Hash().String(),
		ParentHash:           block.ParentHash().String(),
		TransactionIndex:     index,
		TransactionHash:      tx.Hash().String(),
		Nonce:                tx.Nonce(),
		FromAddress:          from.Hex(),
		ToAddress:            toAddress,
		Value:                tx.Value(),
		Input:                input,
		GasLimit:             tx.Gas(),
		GasUsed:              receipt.GasUsed,
		GasPrice:             gasPrice,
		TransactionType:      uint32(tx.Type()),
		MaxPriorityFeePerGas: maxPriorityFeePerGas,
		MaxFeePerGas:         maxFeePerGas,
		Success:              receipt.Status == 1,
		NInputBytes:          nInputBytes,
		NInputZeroBytes:      nInputZeroBytes,
		NInputNonzeroBytes:   nInputNonzeroBytes,
		MetaNetworkID:        p.network.ID,
		MetaNetworkName:      p.network.Name,
	}, nil
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
