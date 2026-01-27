package call_frame

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	pcommon "github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
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
		pcommon.TransactionProcessingDuration.WithLabelValues(p.network.Name, "call_frame").Observe(duration.Seconds())
	}()

	// Get trace from execution node
	trace, err := p.getTransactionTrace(ctx, tx, block)
	if err != nil {
		return 0, fmt.Errorf("failed to get trace: %w", err)
	}

	if len(trace.Structlogs) == 0 {
		// No structlogs means no EVM execution (e.g., simple ETH transfer)
		// We still emit a root frame for consistency
		rootFrame := CallFrameRow{
			CallFrameID:   0,
			Depth:         0,
			CallType:      "", // Root frame has no initiating CALL opcode
			OpcodeCount:   0,
			ErrorCount:    0,
			Gas:           0,
			GasCumulative: 0,
		}

		if err := p.insertCallFrames(ctx, []CallFrameRow{rootFrame}, block.Number().Uint64(), tx.Hash().String(), uint32(index), time.Now()); err != nil { //nolint:gosec // index is bounded by block.Transactions() length
			pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "call_frame", "failed").Inc()

			return 0, fmt.Errorf("failed to insert call frames: %w", err)
		}

		pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "call_frame", "success").Inc()

		return 1, nil
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

	// Check if CREATE/CREATE2 addresses are pre-computed by the tracer
	precomputedCreateAddresses := hasPrecomputedCreateAddresses(trace.Structlogs)

	var createAddresses map[int]*string
	if !precomputedCreateAddresses {
		createAddresses = computeCreateAddresses(trace.Structlogs)
	}

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

		// Get call target address
		callToAddr := p.extractCallAddressWithCreate(sl, i, createAddresses)

		// Process this structlog into the aggregator
		aggregator.ProcessStructlog(sl, i, frameID, framePath, gasUsed[i], callToAddr, prevStructlog)

		// Check for EOA call: CALL-type opcode where depth stays the same (immediate return)
		// and target is not a precompile
		if isCallOpcode(sl.Op) && callToAddr != nil {
			if i+1 < len(trace.Structlogs) {
				nextDepth := trace.Structlogs[i+1].Depth
				if nextDepth == sl.Depth && !isPrecompile(*callToAddr) {
					// Emit synthetic EOA frame
					eoaFrameID, eoaFramePath := callTracker.issueFrameID()
					aggregator.ProcessStructlog(&execution.StructLog{
						Op:    "",
						Depth: sl.Depth + 1,
					}, i, eoaFrameID, eoaFramePath, 0, callToAddr, sl)
				}
			}
		}

		prevStructlog = sl
	}

	// Get receipt gas for intrinsic gas calculation
	// For now, we use trace.Gas as a proxy (TODO: get actual receipt gas from block receipts)
	receiptGas := trace.Gas

	// Finalize aggregation and get call frame rows
	frames := aggregator.Finalize(trace, receiptGas)

	if len(frames) == 0 {
		return 0, nil
	}

	// Insert call frames to ClickHouse
	if err := p.insertCallFrames(ctx, frames, block.Number().Uint64(), tx.Hash().String(), uint32(index), time.Now()); err != nil { //nolint:gosec // index is bounded by block.Transactions() length
		pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "call_frame", "failed").Inc()

		return 0, fmt.Errorf("failed to insert call frames: %w", err)
	}

	// Record success metrics
	pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "call_frame", "success").Inc()
	pcommon.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "success", "").Add(float64(len(frames)))

	// Log progress for transactions with many frames
	if len(frames) > 100 {
		p.log.WithFields(logrus.Fields{
			"tx_hash":     tx.Hash().String(),
			"frame_count": len(frames),
		}).Debug("Processed transaction with many call frames")
	}

	return len(frames), nil
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
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.StackTraceOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	return trace, nil
}

// extractCallAddressWithCreate extracts the call address, using createAddresses map for CREATE/CREATE2 opcodes.
func (p *Processor) extractCallAddressWithCreate(structLog *execution.StructLog, index int, createAddresses map[int]*string) *string {
	// For CREATE/CREATE2, use the pre-computed address from the trace
	if structLog.Op == opcodeCREATE || structLog.Op == opcodeCREATE2 {
		if createAddresses != nil {
			return createAddresses[index]
		}

		return nil
	}

	return p.extractCallAddress(structLog)
}

// extractCallAddress extracts the call address from a structlog for CALL-family opcodes.
func (p *Processor) extractCallAddress(structLog *execution.StructLog) *string {
	// Embedded mode: use pre-extracted CallToAddress
	if structLog.CallToAddress != nil {
		return structLog.CallToAddress
	}

	// RPC mode fallback: extract from Stack for CALL-family opcodes
	if structLog.Stack == nil || len(*structLog.Stack) < 2 {
		return nil
	}

	switch structLog.Op {
	case opcodeCALL, opcodeCALLCODE, opcodeDELEGATECALL, opcodeSTATICCALL:
		stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]
		addr := formatAddress(stackValue)

		return &addr
	default:
		return nil
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

// precompileAddresses contains all known EVM precompile addresses.
var precompileAddresses = map[string]bool{
	"0x0000000000000000000000000000000000000001": true, // ecrecover
	"0x0000000000000000000000000000000000000002": true, // sha256
	"0x0000000000000000000000000000000000000003": true, // ripemd160
	"0x0000000000000000000000000000000000000004": true, // identity (dataCopy)
	"0x0000000000000000000000000000000000000005": true, // modexp (bigModExp)
	"0x0000000000000000000000000000000000000006": true, // bn256Add (ecAdd)
	"0x0000000000000000000000000000000000000007": true, // bn256ScalarMul (ecMul)
	"0x0000000000000000000000000000000000000008": true, // bn256Pairing (ecPairing)
	"0x0000000000000000000000000000000000000009": true, // blake2f
	"0x000000000000000000000000000000000000000a": true, // kzgPointEvaluation (EIP-4844, Cancun)
	"0x000000000000000000000000000000000000000b": true, // bls12381G1Add (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000c": true, // bls12381G1MultiExp (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000d": true, // bls12381G2Add (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000e": true, // bls12381G2MultiExp (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000f": true, // bls12381Pairing (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000010": true, // bls12381MapG1 (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000011": true, // bls12381MapG2 (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000100": true, // p256Verify (EIP-7212, Osaka)
}

// isPrecompile returns true if the address is a known EVM precompile.
func isPrecompile(addr string) bool {
	hex := strings.TrimPrefix(strings.ToLower(addr), "0x")

	for len(hex) < 40 {
		hex = "0" + hex
	}

	return precompileAddresses["0x"+hex]
}

// hasPrecomputedGasUsed detects whether GasUsed values are pre-computed by the tracer.
func hasPrecomputedGasUsed(structlogs []execution.StructLog) bool {
	if len(structlogs) == 0 {
		return false
	}

	return structlogs[0].GasUsed > 0
}

// hasPrecomputedCreateAddresses detects whether CREATE/CREATE2 addresses are pre-computed.
func hasPrecomputedCreateAddresses(structlogs []execution.StructLog) bool {
	for i := range structlogs {
		op := structlogs[i].Op
		if op == opcodeCREATE || op == opcodeCREATE2 {
			return structlogs[i].CallToAddress != nil
		}
	}

	return false
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
			gasUsed[prevIdx] = structlogs[prevIdx].Gas - structlogs[i].Gas
		}

		pendingIdx[depth] = i
	}

	return gasUsed
}

// computeCreateAddresses pre-computes the created contract addresses for all CREATE/CREATE2 opcodes.
func computeCreateAddresses(structlogs []execution.StructLog) map[int]*string {
	result := make(map[int]*string)

	type pendingCreate struct {
		index int
		depth uint64
	}

	var pending []pendingCreate

	for i, log := range structlogs {
		// Resolve pending CREATEs that have completed
		for len(pending) > 0 {
			last := pending[len(pending)-1]

			if log.Depth <= last.depth && i > last.index {
				if log.Stack != nil && len(*log.Stack) > 0 {
					addr := formatAddress((*log.Stack)[len(*log.Stack)-1])
					result[last.index] = &addr
				}

				pending = pending[:len(pending)-1]
			} else {
				break
			}
		}

		if log.Op == opcodeCREATE || log.Op == opcodeCREATE2 {
			pending = append(pending, pendingCreate{index: i, depth: log.Depth})
		}
	}

	return result
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
