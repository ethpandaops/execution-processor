package structlog

import (
	"errors"
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// ErrCallFramesMismatch means the call frames from TraceCallFrames do not line up with the structlog stream.
var ErrCallFramesMismatch = errors.New("call frames do not match the structlog trace")

// rowBuilder turns opcodes, one at a time, into structlog rows, holding at most one unfinished row per call depth.
// return_data is set only on the first opcode after a CALL/CREATE-family opcode returns.
type rowBuilder struct {
	base Structlog
	emit func(*Structlog) error

	callToAddress func(*execution.StructLog) *string

	// precomputedGasUsed means the tracer filled GasUsed (embedded mode).
	precomputedGasUsed bool

	// frames, when set, supplies post-call return data (RPC mode); otherwise it comes from each opcode's ReturnData.
	frames    []execution.CallFrame
	useFrames bool
	nextFrame int

	tracker *CallTracker
	pending []*pendingRow
	last    *pendingRow
	index   int
	emitted int
}

type pendingRow struct {
	row           Structlog
	callOrCreate  bool
	call          bool
	childGas      uint64
	frame         int
	createPending bool
}

type rowBuilderConfig struct {
	base               Structlog
	emit               func(*Structlog) error
	callToAddress      func(*execution.StructLog) *string
	precomputedGasUsed bool
	frames             []execution.CallFrame
	useFrames          bool
}

func newRowBuilder(cfg rowBuilderConfig) *rowBuilder {
	return &rowBuilder{
		base:               cfg.base,
		emit:               cfg.emit,
		callToAddress:      cfg.callToAddress,
		precomputedGasUsed: cfg.precomputedGasUsed,
		frames:             cfg.frames,
		useFrames:          cfg.useFrames,
		tracker:            NewCallTracker(),
	}
}

// add consumes the next opcode. sl may be reused by the caller once add returns.
func (b *rowBuilder) add(sl *execution.StructLog) error {
	depth := sl.Depth

	if err := b.followUpPrevious(sl); err != nil {
		return err
	}

	// Frames deeper than this opcode have returned: their last rows keep gas_cost as gas_used.
	for d := len(b.pending) - 1; d > int(depth); d-- { //nolint:gosec // EVM depth is capped at 1024
		p := b.pending[d]
		if p == nil {
			continue
		}

		b.resolveCreate(p, sl)

		if err := b.finalize(d); err != nil {
			return err
		}
	}

	var returnData *string

	if int(depth) < len(b.pending) { //nolint:gosec // EVM depth is capped at 1024
		if p := b.pending[depth]; p != nil {
			if !b.precomputedGasUsed && p.row.Gas >= sl.Gas {
				p.row.GasUsed = p.row.Gas - sl.Gas
			}

			b.resolveCreate(p, sl)

			if p.callOrCreate {
				returnData = b.postCallReturnData(p, sl)
			}

			if err := b.finalize(int(depth)); err != nil { //nolint:gosec // EVM depth is capped at 1024
				return err
			}
		}
	}

	frameID, framePath := b.tracker.ProcessDepthChange(depth)

	row := b.base
	row.Index = uint32(b.index) //nolint:gosec // opcode count fits uint32
	row.Operation = sl.Op
	row.Gas = sl.Gas
	row.GasCost = sl.GasCost
	row.GasUsed = sl.GasCost
	row.Depth = depth
	row.ReturnData = returnData
	row.Refund = sl.Refund
	row.Error = sl.Error
	row.CallFrameID = frameID
	row.CallFramePath = framePath

	if b.precomputedGasUsed {
		row.GasUsed = sl.GasUsed
	}

	// The embedded tracer fills a CREATE's address once its constructor returns; RPC traces resolve it from the stack.
	isCreate := sl.Op == OpcodeCREATE || sl.Op == OpcodeCREATE2
	if isCreate {
		row.CallToAddress = sl.CallToAddress
	} else {
		row.CallToAddress = b.callToAddress(sl)
	}

	p := &pendingRow{
		row:           row,
		callOrCreate:  isCallOrCreateOpcode(sl.Op),
		call:          isCallOpcode(sl.Op),
		frame:         -1,
		createPending: isCreate && sl.CallToAddress == nil,
	}

	if b.useFrames && p.callOrCreate {
		if err := b.matchFrame(p); err != nil {
			return err
		}
	}

	for len(b.pending) <= int(depth) { //nolint:gosec // EVM depth is capped at 1024
		b.pending = append(b.pending, nil)
	}

	b.pending[depth] = p
	b.last = p
	b.index++

	return nil
}

// followUpPrevious checks the previous CALL/CREATE got the frame it must have opened, and emits the
// synthetic row for a CALL to an EOA, which returns without running code.
func (b *rowBuilder) followUpPrevious(sl *execution.StructLog) error {
	prev := b.last
	if prev == nil || !prev.callOrCreate {
		return nil
	}

	entered := sl.Depth == prev.row.Depth+1
	immediate := sl.Depth == prev.row.Depth

	// Only a create that fails its own checks may return without a frame.
	if b.useFrames && prev.frame < 0 && (entered || (immediate && prev.call)) {
		return fmt.Errorf("%w: %s at opcode %d depth %d has no frame", ErrCallFramesMismatch, prev.row.Operation, prev.row.Index, prev.row.Depth)
	}

	if !prev.call || prev.row.CallToAddress == nil || !immediate || IsPrecompile(*prev.row.CallToAddress) {
		return nil
	}

	eoaFrameID, eoaFramePath := b.tracker.IssueFrameID()

	eoa := b.base
	eoa.Index = prev.row.Index
	eoa.Depth = prev.row.Depth + 1
	eoa.Error = prev.row.Error
	eoa.CallToAddress = prev.row.CallToAddress
	eoa.CallFrameID = eoaFrameID
	eoa.CallFramePath = eoaFramePath
	eoa.Synthetic = true

	return b.emitRow(&eoa)
}

// matchFrame assigns the frame the opcode at the row's index opened; an opcode that opened none leaves the buffer empty.
func (b *rowBuilder) matchFrame(p *pendingRow) error {
	if b.nextFrame >= len(b.frames) || b.frames[b.nextFrame].Index != p.row.Index {
		return nil
	}

	f := b.frames[b.nextFrame]
	if f.Type != p.row.Operation {
		return fmt.Errorf("%w: frame %d is a %s but opcode %d is %s", ErrCallFramesMismatch, b.nextFrame, f.Type, p.row.Index, p.row.Operation)
	}

	p.frame = b.nextFrame
	b.nextFrame++

	return nil
}

func (b *rowBuilder) postCallReturnData(p *pendingRow, sl *execution.StructLog) *string {
	if !b.useFrames {
		return sl.ReturnData
	}

	if p.frame < 0 {
		return nil
	}

	return b.frames[p.frame].ReturnData
}

// resolveCreate takes a CREATE's address from the top of the stack of the first later opcode at or above its depth.
func (b *rowBuilder) resolveCreate(p *pendingRow, sl *execution.StructLog) {
	if !p.createPending || sl.Depth > p.row.Depth {
		return
	}

	p.createPending = false

	if sl.Stack != nil && len(*sl.Stack) > 0 {
		addr := formatAddress((*sl.Stack)[len(*sl.Stack)-1])
		p.row.CallToAddress = &addr
	}
}

// finalize emits the pending row at depth and credits its gas to the CALL/CREATE that opened its frame.
func (b *rowBuilder) finalize(depth int) error {
	p := b.pending[depth]
	b.pending[depth] = nil

	p.row.GasSelf = p.row.GasUsed

	if p.callOrCreate {
		if p.row.GasUsed >= p.childGas {
			p.row.GasSelf = p.row.GasUsed - p.childGas
		} else {
			p.row.GasSelf = 0
		}
	}

	if depth > 0 && depth-1 < len(b.pending) {
		if parent := b.pending[depth-1]; parent != nil && parent.callOrCreate {
			parent.childGas += p.row.GasUsed
		}
	}

	return b.emitRow(&p.row)
}

func (b *rowBuilder) emitRow(row *Structlog) error {
	b.emitted++

	return b.emit(row)
}

// finish emits every remaining row; rows still waiting for a later opcode keep their fallbacks.
func (b *rowBuilder) finish() error {
	for d := len(b.pending) - 1; d >= 0; d-- {
		if b.pending[d] == nil {
			continue
		}

		if err := b.finalize(d); err != nil {
			return err
		}
	}

	if b.useFrames && b.nextFrame != len(b.frames) {
		return fmt.Errorf("%w: matched %d of %d frames, next at opcode %d", ErrCallFramesMismatch, b.nextFrame, len(b.frames), b.frames[b.nextFrame].Index)
	}

	return nil
}
