package structlog

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// legacyRows is the per-opcode derivation that structlog used before change-point return data.
func legacyRows(trace *execution.TraceTransaction) []Structlog {
	p := &Processor{}

	gasUsed := ComputeGasUsed(trace.Structlogs)
	if hasPrecomputedGasUsed(trace.Structlogs) {
		for i := range trace.Structlogs {
			gasUsed[i] = trace.Structlogs[i].GasUsed
		}
	}

	gasSelf := ComputeGasSelf(trace.Structlogs, gasUsed)
	callTracker := NewCallTracker()

	createAddresses := ComputeCreateAddresses(trace.Structlogs)

	rows := make([]Structlog, 0, len(trace.Structlogs))

	for i := range trace.Structlogs {
		sl := &trace.Structlogs[i]
		frameID, framePath := callTracker.ProcessDepthChange(sl.Depth)

		callToAddr := p.extractCallAddress(sl)
		if sl.Op == OpcodeCREATE || sl.Op == OpcodeCREATE2 {
			callToAddr = createAddresses[i]
		}

		rows = append(rows, Structlog{
			TransactionGas:         trace.Gas,
			TransactionFailed:      trace.Failed,
			TransactionReturnValue: trace.ReturnValue,
			Index:                  uint32(i),
			Operation:              sl.Op,
			Gas:                    sl.Gas,
			GasCost:                sl.GasCost,
			GasUsed:                gasUsed[i],
			GasSelf:                gasSelf[i],
			Depth:                  sl.Depth,
			ReturnData:             sl.ReturnData,
			Refund:                 sl.Refund,
			Error:                  sl.Error,
			CallToAddress:          callToAddr,
			CallFrameID:            frameID,
			CallFramePath:          framePath,
		})

		if isCallOpcode(sl.Op) && callToAddr != nil && i+1 < len(trace.Structlogs) &&
			trace.Structlogs[i+1].Depth == sl.Depth && !IsPrecompile(*callToAddr) {
			eoaFrameID, eoaFramePath := callTracker.IssueFrameID()

			rows = append(rows, Structlog{
				TransactionGas:         trace.Gas,
				TransactionFailed:      trace.Failed,
				TransactionReturnValue: trace.ReturnValue,
				Index:                  uint32(i),
				Depth:                  sl.Depth + 1,
				Error:                  sl.Error,
				CallToAddress:          callToAddr,
				CallFrameID:            eoaFrameID,
				CallFramePath:          eoaFramePath,
				Synthetic:              true,
			})
		}
	}

	return rows
}

func buildRows(t *testing.T, trace *execution.TraceTransaction, frames []execution.CallFrame, useFrames bool) []Structlog {
	t.Helper()

	p := &Processor{}

	var rows []Structlog

	b := newRowBuilder(rowBuilderConfig{
		base: Structlog{
			TransactionGas:         trace.Gas,
			TransactionFailed:      trace.Failed,
			TransactionReturnValue: trace.ReturnValue,
		},
		emit: func(row *Structlog) error {
			rows = append(rows, *row)

			return nil
		},
		callToAddress:      p.extractCallAddress,
		precomputedGasUsed: hasPrecomputedGasUsed(trace.Structlogs),
		frames:             frames,
		useFrames:          useFrames,
	})

	for i := range trace.Structlogs {
		sl := trace.Structlogs[i]
		if useFrames {
			sl.ReturnData = nil
		}

		require.NoError(t, b.add(&sl))
	}

	require.NoError(t, b.finish())
	require.Equal(t, len(rows), b.emitted)

	return rows
}

// sortRows orders rows like the table does: by index, synthetic EOA rows after their CALL.
func sortRows(rows []Structlog) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Index != rows[j].Index {
			return rows[i].Index < rows[j].Index
		}

		return rows[i].Operation != "" && rows[j].Operation == ""
	})
}

// reconstructReturnData rebuilds each opcode's return-data buffer from change-point rows using only table columns:
// the buffer is the return_data of the latest row in the same call frame whose previous row in that frame was a
// CALL/CREATE-family opcode, or empty if there is none.
func reconstructReturnData(rows []Structlog) map[uint32]*string {
	sortRows(rows)

	current := map[uint32]*string{}
	lastOp := map[uint32]string{}
	out := map[uint32]*string{}

	for i := range rows {
		r := &rows[i]
		if r.Operation == "" {
			continue
		}

		if isCallOrCreateOpcode(lastOp[r.CallFrameID]) {
			current[r.CallFrameID] = r.ReturnData
		}

		lastOp[r.CallFrameID] = r.Operation
		out[r.Index] = current[r.CallFrameID]
	}

	return out
}

func requireSameRowsIgnoringReturnData(t *testing.T, want, got []Structlog) {
	t.Helper()

	sortRows(want)
	sortRows(got)
	require.Len(t, got, len(want))

	for i := range want {
		w, g := want[i], got[i]
		w.ReturnData, g.ReturnData = nil, nil
		require.Equal(t, w, g, "row %d (index %d op %q)", i, want[i].Index, want[i].Operation)
	}
}

func strPtr(s string) *string { return &s }

// traceGen produces structlog traces that follow EVM return-data rules, with the call frames a frame tracer reports.
type traceGen struct {
	r      *rand.Rand
	logs   []execution.StructLog
	frames []execution.CallFrame
	gas    uint64
	budget int
}

func word(addr string) string {
	return "0x000000000000000000000000" + addr
}

func (g *traceGen) randomBuffer() *string {
	if g.r.IntN(3) == 0 {
		return nil
	}

	b := make([]byte, 1+g.r.IntN(64))
	for i := range b {
		b[i] = byte(g.r.IntN(256))
	}

	s := hex.EncodeToString(b)

	return &s
}

func (g *traceGen) emit(op string, depth uint64, buf *string, stack []string) {
	cost := uint64(3 + g.r.IntN(10))
	g.logs = append(g.logs, execution.StructLog{Op: op, Gas: g.gas, GasCost: cost, Depth: depth, ReturnData: buf, Stack: &stack})
	g.gas -= cost
	g.budget--
}

// run executes one frame at depth and returns the buffer it leaves and whether it reverted.
func (g *traceGen) run(depth uint64) (*string, bool) {
	var buf *string

	for g.budget > 0 {
		switch choice := g.r.IntN(12); {
		case choice < 5:
			g.emit("ADD", depth, buf, []string{"0x1", "0x2"})
		case choice < 7 && depth < 6:
			op := []string{"CALL", "STATICCALL", "DELEGATECALL", "CALLCODE"}[g.r.IntN(4)]
			addr := fmt.Sprintf("%040x", 0x1000+g.r.IntN(50))
			g.emit(op, depth, buf, []string{"0x0", "0x0", "0x0", "0x0", "0x0", word(addr), "0x5000"})

			idx := len(g.frames)
			g.frames = append(g.frames, execution.CallFrame{Index: uint32(len(g.logs) - 1), Type: op})
			out, reverted := g.run(depth + 1)

			buf = out
			if !reverted && g.r.IntN(5) == 0 {
				buf = nil
			}

			g.frames[idx].ReturnData = buf
		case choice < 8:
			op := []string{"CALL", "STATICCALL"}[g.r.IntN(2)]

			addr := fmt.Sprintf("%040x", 0x2000+g.r.IntN(50))
			if g.r.IntN(2) == 0 {
				addr = fmt.Sprintf("%040x", 1+g.r.IntN(9))
			}

			g.emit(op, depth, buf, []string{"0x0", "0x0", "0x0", "0x0", "0x0", word(addr), "0x5000"})

			buf = nil
			if IsPrecompile("0x" + addr) {
				buf = g.randomBuffer()
			}

			g.frames = append(g.frames, execution.CallFrame{Index: uint32(len(g.logs) - 1), Type: op, ReturnData: buf})
		case choice < 9 && depth < 6:
			op := []string{"CREATE", "CREATE2"}[g.r.IntN(2)]
			g.emit(op, depth, buf, []string{"0x0", "0x0", "0x0"})

			idx := len(g.frames)
			g.frames = append(g.frames, execution.CallFrame{Index: uint32(len(g.logs) - 1), Type: op})
			out, reverted := g.run(depth + 1)

			buf = nil
			if reverted {
				buf = out
			}

			g.frames[idx].ReturnData = buf

			created := fmt.Sprintf("%040x", 0x3000+g.r.IntN(50))
			if reverted {
				created = fmt.Sprintf("%040x", 0)
			}

			g.emit("SWAP1", depth, buf, []string{"0x1", word(created)})
		case choice < 10:
			g.emit("CREATE", depth, buf, []string{"0x0", "0x0", "0x0"})

			buf = nil
			g.emit("POP", depth, buf, []string{word(fmt.Sprintf("%040x", 0))})
		case choice < 11 && depth > 1:
			out := g.randomBuffer()
			g.emit("REVERT", depth, buf, []string{"0x0", "0x0"})

			return out, true
		default:
			if depth > 1 {
				out := g.randomBuffer()
				g.emit("RETURN", depth, buf, []string{"0x0", "0x0"})

				return out, false
			}

			g.emit("ADD", depth, buf, []string{"0x1", "0x2"})
		}
	}

	g.emit("STOP", depth, buf, nil)

	return nil, false
}

func generateTrace(seed uint64, budget int) (*execution.TraceTransaction, []execution.CallFrame) {
	g := &traceGen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), gas: 1 << 40, budget: budget}
	g.run(1)

	return &execution.TraceTransaction{Gas: 123456, ReturnValue: strPtr("cafe"), Structlogs: g.logs}, g.frames
}

func TestRowBuilderMatchesLegacyRowsAndKeepsReturnData(t *testing.T) {
	for seed := uint64(1); seed <= 300; seed++ {
		trace, frames := generateTrace(seed, 40+int(seed%200))

		legacy := legacyRows(trace)
		embedded := buildRows(t, trace, nil, false)
		rpc := buildRows(t, trace, frames, true)

		requireSameRowsIgnoringReturnData(t, legacy, embedded)
		sortRows(rpc)
		require.Equal(t, embedded, rpc, "seed %d: RPC and embedded rows differ", seed)

		got := reconstructReturnData(rpc)

		for i := range trace.Structlogs {
			want := trace.Structlogs[i].ReturnData

			require.Equal(t, want, got[uint32(i)], "seed %d index %d op %s", seed, i, trace.Structlogs[i].Op)
		}
	}
}

func TestRowBuilderSetsReturnDataOnlyAfterCalls(t *testing.T) {
	trace, frames := generateTrace(42, 400)
	rows := buildRows(t, trace, frames, true)
	sortRows(rows)

	lastOp := map[uint32]string{}

	for i := range rows {
		r := &rows[i]
		if r.Operation == "" {
			require.Nil(t, r.ReturnData)

			continue
		}

		if r.ReturnData != nil {
			require.True(t, isCallOrCreateOpcode(lastOp[r.CallFrameID]), "index %d op %s carries return data but does not follow a call", r.Index, r.Operation)
		}

		lastOp[r.CallFrameID] = r.Operation
	}
}

func TestRowBuilderRejectsMismatchedFrames(t *testing.T) {
	trace, frames := generateTrace(7, 300)

	dropped := -1

	for i, f := range frames {
		if isCallOpcode(f.Type) {
			dropped = i

			break
		}
	}

	require.GreaterOrEqual(t, dropped, 0)

	p := &Processor{}
	b := newRowBuilder(rowBuilderConfig{
		emit:          func(*Structlog) error { return nil },
		callToAddress: p.extractCallAddress,
		frames:        append(append([]execution.CallFrame{}, frames[:dropped]...), frames[dropped+1:]...),
		useFrames:     true,
	})

	var err error

	for i := range trace.Structlogs {
		if err = b.add(&trace.Structlogs[i]); err != nil {
			break
		}
	}

	if err == nil {
		err = b.finish()
	}

	require.ErrorIs(t, err, ErrCallFramesMismatch)
}

// TestRowBuilderHoldsOnlyPendingRows checks the builder keeps one row per depth however long the trace is.
func TestRowBuilderHoldsOnlyPendingRows(t *testing.T) {
	p := &Processor{}
	emitted := 0
	b := newRowBuilder(rowBuilderConfig{
		emit: func(*Structlog) error {
			emitted++

			return nil
		},
		callToAddress: p.extractCallAddress,
	})

	sl := execution.StructLog{Op: "ADD", Gas: 1 << 40, GasCost: 3, Depth: 1}

	for i := 0; i < 200000; i++ {
		require.NoError(t, b.add(&sl))

		sl.Gas -= 3

		require.LessOrEqual(t, b.index-emitted, 2)
	}

	require.NoError(t, b.finish())
	require.Equal(t, 200000, emitted)
}
