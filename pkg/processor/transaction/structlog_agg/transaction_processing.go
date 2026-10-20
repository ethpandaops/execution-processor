package structlog_agg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	pcommon "github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
)

// Opcode constants for call and create operations.
const (
	opcodeCALL         = "CALL"
	opcodeCALLCODE     = "CALLCODE"
	opcodeDELEGATECALL = "DELEGATECALL"
	opcodeSTATICCALL   = "STATICCALL"
	opcodeCREATE       = "CREATE"
	opcodeCREATE2      = "CREATE2"
)

// ProcessTransaction processes a transaction and inserts aggregated call frame data to ClickHouse.
func (p *Processor) ProcessTransaction(ctx context.Context, block execution.Block, index int, tx execution.Transaction) (int, error) {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		pcommon.TransactionProcessingDuration.WithLabelValues(p.network.Name, "structlog_agg").Observe(duration.Seconds())
	}()

	frames, err := p.ExtractCallFrames(ctx, block, tx)
	if err != nil {
		return 0, err
	}

	if len(frames) == 0 {
		return 0, nil
	}

	if err := p.insertCallFrames(ctx, frames, block.Number().Uint64(), tx.Hash().String(), uint32(index), time.Now()); err != nil { //nolint:gosec // index is bounded by block.Transactions() length
		pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog_agg", "failed").Inc()

		return 0, fmt.Errorf("failed to insert call frames: %w", err)
	}

	pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog_agg", "success").Inc()

	if len(frames) > 100 {
		p.log.WithFields(logrus.Fields{
			"tx_hash":     tx.Hash().String(),
			"frame_count": len(frames),
		}).Debug("Processed transaction with many call frames")
	}

	return len(frames), nil
}

// ExtractCallFrames traces a transaction and returns its aggregated call frame rows without inserting them.
func (p *Processor) ExtractCallFrames(ctx context.Context, block execution.Block, tx execution.Transaction) ([]CallFrameRow, error) {
	trace, err := p.getTransactionTrace(ctx, tx, block)
	if err != nil {
		return nil, fmt.Errorf("failed to get trace: %w", err)
	}

	return p.buildCallFrames(trace, tx), nil
}

// buildCallFrames aggregates a transaction trace into call frame rows.
func (p *Processor) buildCallFrames(trace *execution.TraceTransaction, tx execution.Transaction) []CallFrameRow {
	if len(trace.Structlogs) == 0 {
		// No structlogs means no EVM execution (e.g., simple ETH transfer)
		// We still emit a root frame for consistency, matching SQL simple_transfer_frames logic
		rootFrame := CallFrameRow{
			CallFrameID:   0,
			CallFramePath: []uint32{0},
			Depth:         0,
			CallType:      "", // Root frame has no initiating CALL opcode
			Operation:     "", // Summary row
			OpcodeCount:   0,
			Gas:           0,
			GasCumulative: 0,
			MinDepth:      0,
			MaxDepth:      0,
		}

		// Set target_address from transaction's to_address
		if tx.To() != nil {
			addr := tx.To().Hex()
			rootFrame.TargetAddress = &addr
		}

		// Set error_count: 1 if transaction failed, 0 otherwise
		if trace.Failed {
			rootFrame.ErrorCount = 1
		} else {
			rootFrame.ErrorCount = 0
		}

		// For simple transfers (no EVM execution), all gas is intrinsic.
		// This is true for both successful and failed transactions.
		// trace.Gas is the post-refund receipt gas — see TraceTransaction.Gas doc.
		intrinsicGas := trace.Gas
		rootFrame.IntrinsicGas = &intrinsicGas

		// gas_refund is 0 for simple transfers (no SSTORE operations)

		return []CallFrameRow{rootFrame}
	}

	// Check if GasUsed is pre-computed by the tracer (embedded mode)
	precomputedGasUsed := hasPrecomputedGasUsed(trace.Structlogs)

	var gasUsed []uint64
	if !precomputedGasUsed {
		gasUsed = computeGasUsed(trace.Structlogs)
	} else {
		// Extract pre-computed GasUsed values from structlogs (embedded mode)
		gasUsed = make([]uint64, len(trace.Structlogs))
		for i := range trace.Structlogs {
			gasUsed[i] = trace.Structlogs[i].GasUsed
		}
	}

	// Compute gasSelf: gas excluding child frame gas (for CALL/CREATE opcodes)
	gasSelf := structlog.ComputeGasSelf(trace.Structlogs, gasUsed)

	resolveCallTargets(trace.Structlogs)

	// Compute memory words and expansion gas for resource gas decomposition.
	wordsBefore, wordsAfter := structlog.ComputeMemoryWords(trace.Structlogs)

	var memExpGas []uint64
	if wordsBefore != nil {
		memExpGas = make([]uint64, len(trace.Structlogs))
		for i := range trace.Structlogs {
			memExpGas[i] = structlog.MemoryExpansionGas(wordsBefore[i], wordsAfter[i])
		}
	}

	// Classify cold vs warm access for each opcode.
	coldCounts := structlog.ClassifyColdAccess(trace.Structlogs, gasSelf, memExpGas)

	// Initialize frame aggregator
	aggregator := NewFrameAggregator()

	// Initialize call frame tracker (reusing the same logic as structlog processor)
	callTracker := newCallTracker()

	// Process all structlogs
	var prevStructlog *execution.StructLog

	for i := range trace.Structlogs {
		sl := &trace.Structlogs[i]

		// Track call frame based on depth changes
		frameID, framePath := callTracker.processDepthChange(sl.Depth)

		callToAddr := sl.CallToAddress

		// Before processing parent CALL: detect precompile and compute gas split.
		// Precompile gas = gasSelf minus CALL overhead, adjusted for memory expansion.
		// Precompiles are always warm (EIP-2929 pre-warms them).
		effectiveGasSelf := gasSelf[i]

		var precompileGas uint64

		if isCallOpcode(sl.Op) && callToAddr != nil && i+1 < len(trace.Structlogs) {
			nextDepth := trace.Structlogs[i+1].Depth
			if nextDepth == sl.Depth && isPrecompile(*callToAddr) {
				memExp := uint64(0)
				if memExpGas != nil {
					memExp = memExpGas[i]
				}

				overhead := uint64(100) + memExp

				if gasSelf[i] > overhead {
					precompileGas = gasSelf[i] - overhead
					effectiveGasSelf = overhead
				}
			}
		}

		// Get per-opcode resource gas values (0 if data unavailable).
		var wb, wa uint32

		var cold uint64

		if wordsBefore != nil {
			wb = wordsBefore[i]
			wa = wordsAfter[i]
		}

		var memExp uint64
		if memExpGas != nil {
			memExp = memExpGas[i]
		}

		if coldCounts != nil {
			cold = coldCounts[i]
		}

		// Process this structlog into the aggregator with (possibly reduced) gasSelf
		aggregator.ProcessStructlog(sl, i, frameID, framePath, gasUsed[i], effectiveGasSelf, callToAddr, prevStructlog, wb, wa, memExp, cold)

		// Emit synthetic frame for ALL immediate-return CALLs (EOA + precompile).
		// For EOA calls: precompileGas = 0, so frame has gas=0 (unchanged behavior).
		// For precompile calls: frame has gas=precompileGas.
		if isCallOpcode(sl.Op) && callToAddr != nil {
			if i+1 < len(trace.Structlogs) {
				nextDepth := trace.Structlogs[i+1].Depth
				if nextDepth == sl.Depth {
					synthFrameID, synthFramePath := callTracker.issueFrameID()
					aggregator.ProcessStructlog(&execution.StructLog{
						Op:    "",
						Depth: sl.Depth + 1,
					}, i, synthFrameID, synthFramePath, precompileGas, precompileGas, callToAddr, sl, 0, 0, 0, 0)
				}
			}
		}

		prevStructlog = sl
	}

	// Set root frame's target address from transaction's to_address
	// (root frame has no initiating CALL, so target comes from tx.To())
	if tx.To() != nil {
		addr := tx.To().Hex()
		aggregator.SetRootTargetAddress(&addr)
	}

	// Get receipt gas for intrinsic gas calculation.
	// trace.Gas is the post-refund receipt gas set by the data source (erigone/xatu sets
	// this from ExecutionResult.ReceiptGasUsed; RPC mode gets it from receipt.GasUsed).
	//
	// EIP-7778 context: computeIntrinsicGas() requires the post-refund value because its
	// formula accounts for refunds: intrinsic = receiptGas - gasCumulative + gasRefund.
	// This is correct both pre- and post-EIP-7778 since ReceiptGasUsed preserves the
	// same post-refund semantics that GasUsed had before the split.
	receiptGas := trace.Gas

	return aggregator.Finalize(trace, receiptGas)
}

// getTransactionTrace gets the trace for a transaction.
func (p *Processor) getTransactionTrace(ctx context.Context, tx execution.Transaction, block execution.Block) (*execution.TraceTransaction, error) {
	// Get execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return nil, fmt.Errorf("no healthy execution node available")
	}

	// Process transaction with timeout
	processCtx, cancel := context.WithTimeout(ctx, tracker.DefaultTraceTimeout)
	defer cancel()

	// Get transaction trace
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.CallFrameTraceOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	return trace, nil
}

// resolveCallTargets fills CallToAddress from the stack where the embedded tracer would have set it.
func resolveCallTargets(structlogs []execution.StructLog) {
	var createAddresses map[int]*string

	for i := range structlogs {
		sl := &structlogs[i]
		if sl.CallToAddress != nil {
			continue
		}

		switch sl.Op {
		case opcodeCALL, opcodeCALLCODE, opcodeDELEGATECALL, opcodeSTATICCALL:
			if sl.Stack != nil && len(*sl.Stack) >= 2 {
				addr := formatAddress((*sl.Stack)[len(*sl.Stack)-2])
				sl.CallToAddress = &addr
			}
		case opcodeCREATE, opcodeCREATE2:
			if createAddresses == nil {
				createAddresses = structlog.ComputeCreateAddresses(structlogs)
			}

			sl.CallToAddress = createAddresses[i]
		}
	}
}

// formatAddress normalizes an address to exactly 42 characters (0x + 40 hex).
func formatAddress(addr string) string {
	hex := strings.TrimPrefix(addr, "0x")

	if len(hex) > 40 {
		hex = hex[len(hex)-40:]
	}

	return fmt.Sprintf("0x%040s", hex)
}

// isCallOpcode returns true if the opcode initiates a call that creates a child frame.
func isCallOpcode(op string) bool {
	switch op {
	case opcodeCALL, opcodeCALLCODE, opcodeDELEGATECALL, opcodeSTATICCALL:
		return true
	default:
		return false
	}
}

// isPrecompile delegates to the structlog package's exported IsPrecompile.
func isPrecompile(addr string) bool {
	return structlog.IsPrecompile(addr)
}

// hasPrecomputedGasUsed detects whether GasUsed values are pre-computed by the tracer.
func hasPrecomputedGasUsed(structlogs []execution.StructLog) bool {
	if len(structlogs) == 0 {
		return false
	}

	return structlogs[0].GasUsed > 0
}

// computeGasUsed calculates the actual gas consumed for each structlog.
func computeGasUsed(structlogs []execution.StructLog) []uint64 {
	if len(structlogs) == 0 {
		return nil
	}

	gasUsed := make([]uint64, len(structlogs))

	// Initialize all with pre-calculated cost (fallback)
	for i := range structlogs {
		gasUsed[i] = structlogs[i].GasCost
	}

	// pendingIdx[depth] = index into structlogs for the pending opcode at that depth
	pendingIdx := make([]int, 0, 16)

	for i := range structlogs {
		depth := int(structlogs[i].Depth) //nolint:gosec // EVM depth is capped at 1024

		for len(pendingIdx) <= depth {
			pendingIdx = append(pendingIdx, -1)
		}

		// Clear pending indices from deeper levels
		for d := len(pendingIdx) - 1; d > depth; d-- {
			pendingIdx[d] = -1
		}

		// Update gasUsed for pending log at current depth
		if prevIdx := pendingIdx[depth]; prevIdx >= 0 && prevIdx < len(structlogs) {
			// Guard against underflow: if gas values are corrupted or out of order,
			// fall back to the pre-calculated GasCost instead of underflowing
			if structlogs[prevIdx].Gas >= structlogs[i].Gas {
				gasUsed[prevIdx] = structlogs[prevIdx].Gas - structlogs[i].Gas
			}
			// else: keep the fallback GasCost value set during initialization
		}

		pendingIdx[depth] = i
	}

	return gasUsed
}

// CallTracker tracks call frames during EVM opcode traversal.
type callTracker struct {
	stack  []callFrame
	nextID uint32
	path   []uint32
}

type callFrame struct {
	id    uint32
	depth uint64
}

func newCallTracker() *callTracker {
	return &callTracker{
		stack:  []callFrame{{id: 0, depth: 1}},
		nextID: 1,
		path:   []uint32{0},
	}
}

func (ct *callTracker) processDepthChange(newDepth uint64) (frameID uint32, framePath []uint32) {
	currentDepth := ct.stack[len(ct.stack)-1].depth

	if newDepth > currentDepth {
		newFrame := callFrame{id: ct.nextID, depth: newDepth}
		ct.stack = append(ct.stack, newFrame)
		ct.path = append(ct.path, ct.nextID)
		ct.nextID++
	} else if newDepth < currentDepth {
		for len(ct.stack) > 1 && ct.stack[len(ct.stack)-1].depth > newDepth {
			ct.stack = ct.stack[:len(ct.stack)-1]
			ct.path = ct.path[:len(ct.path)-1]
		}
	}

	pathCopy := make([]uint32, len(ct.path))
	copy(pathCopy, ct.path)

	return ct.stack[len(ct.stack)-1].id, pathCopy
}

func (ct *callTracker) issueFrameID() (frameID uint32, framePath []uint32) {
	newID := ct.nextID
	ct.nextID++

	pathCopy := make([]uint32, len(ct.path)+1)
	copy(pathCopy, ct.path)
	pathCopy[len(ct.path)] = newID

	return newID, pathCopy
}
