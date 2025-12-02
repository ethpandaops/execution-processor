package simple

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
)

// Transaction represents a row in the transaction table
type Transaction struct {
	UpdatedDateTime      time.Time
	BlockNumber          uint64
	BlockHash            string
	ParentHash           string
	TransactionIndex     uint64
	TransactionHash      string
	Nonce                uint64
	FromAddress          string
	ToAddress            *string
	Value                *big.Int // Wei as UInt256
	Input                *string  // Hex-encoded input data
	GasLimit             uint64
	GasUsed              uint64
	GasPrice             uint64
	TransactionType      uint32
	MaxPriorityFeePerGas uint64
	MaxFeePerGas         uint64
	Success              bool
	NInputBytes          uint32
	NInputZeroBytes      uint32
	NInputNonzeroBytes   uint32
	MetaNetworkID        int32
	MetaNetworkName      string
}

// GetHandlers returns the task handlers for this processor
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		ProcessForwardsTaskType:  p.handleProcessTask,
		ProcessBackwardsTaskType: p.handleProcessTask,
		VerifyForwardsTaskType:   p.handleVerifyTask,
		VerifyBackwardsTaskType:  p.handleVerifyTask,
	}
}

// handleProcessTask handles block processing tasks
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
			receipt, err = node.TransactionReceipt(ctx, tx.Hash().String())
			if err != nil {
				return fmt.Errorf("failed to get receipt for tx %s: %w", txHash, err)
			}
		}

		// Build transaction row
		txRow, err := p.buildTransactionRow(block, tx, receipt, uint64(index)) //nolint:gosec // index is bounded
		if err != nil {
			return fmt.Errorf("failed to build transaction row: %w", err)
		}

		transactions = append(transactions, txRow)
	}

	// Insert all transactions
	if len(transactions) > 0 {
		if err := p.insertTransactions(ctx, transactions); err != nil {
			common.TasksErrored.WithLabelValues(
				p.network.Name,
				ProcessorName,
				c.ProcessForwardsQueue(ProcessorName),
				task.Type(),
				"insert_error",
			).Inc()

			return fmt.Errorf("failed to insert transactions: %w", err)
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

// buildTransactionRow builds a transaction row from block, tx, and receipt data
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
		UpdatedDateTime:      time.Now(),
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

// insertTransactions inserts transactions into ClickHouse
func (p *Processor) insertTransactions(ctx context.Context, transactions []Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	db := p.clickhouse.GetDB()
	if db == nil {
		return fmt.Errorf("clickhouse connection not available")
	}

	// Begin transaction
	sqlTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	committed := false

	defer func() {
		if !committed {
			if rollbackErr := sqlTx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
				p.log.WithError(rollbackErr).Warn("Failed to rollback transaction")
			}
		}
	}()

	//nolint:gosec // table name is from config, not user input
	query := fmt.Sprintf(`INSERT INTO %s (
		updated_date_time,
		block_number,
		block_hash,
		parent_hash,
		transaction_index,
		transaction_hash,
		nonce,
		from_address,
		to_address,
		value,
		input,
		gas_limit,
		gas_used,
		gas_price,
		transaction_type,
		max_priority_fee_per_gas,
		max_fee_per_gas,
		success,
		n_input_bytes,
		n_input_zero_bytes,
		n_input_nonzero_bytes,
		meta_network_id,
		meta_network_name
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, p.config.Table)

	stmt, err := sqlTx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}

	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			p.log.WithError(closeErr).Warn("Failed to close prepared statement")
		}
	}()

	for i := range transactions {
		tx := &transactions[i]

		_, err := stmt.ExecContext(ctx,
			tx.UpdatedDateTime,
			tx.BlockNumber,
			tx.BlockHash,
			tx.ParentHash,
			tx.TransactionIndex,
			tx.TransactionHash,
			tx.Nonce,
			tx.FromAddress,
			tx.ToAddress,
			tx.Value,
			tx.Input,
			tx.GasLimit,
			tx.GasUsed,
			tx.GasPrice,
			tx.TransactionType,
			tx.MaxPriorityFeePerGas,
			tx.MaxFeePerGas,
			tx.Success,
			tx.NInputBytes,
			tx.NInputZeroBytes,
			tx.NInputNonzeroBytes,
			tx.MetaNetworkID,
			tx.MetaNetworkName,
		)
		if err != nil {
			return fmt.Errorf("failed to insert transaction %d: %w", i, err)
		}
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	common.ClickHouseInsertsRows.WithLabelValues(
		p.network.Name,
		ProcessorName,
		p.config.Table,
		"success",
		"",
	).Add(float64(len(transactions)))

	return nil
}
