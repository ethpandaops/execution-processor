package structlog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/internal/testutil"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// framesFromTrace lists a frame for every CALL/CREATE-family opcode, as the RPC frame tracer reports them.
func framesFromTrace(trace *execution.TraceTransaction) []execution.CallFrame {
	var frames []execution.CallFrame

	for i := range trace.Structlogs {
		if isCallOrCreateOpcode(trace.Structlogs[i].Op) {
			frames = append(frames, execution.CallFrame{Index: uint32(i), Type: trace.Structlogs[i].Op})
		}
	}

	return frames
}

// withoutRefund drops refund, which the RPC fixture's node omits and the embedded tracer always reports.
func withoutRefund(rows []Structlog) []Structlog {
	for i := range rows {
		rows[i].Refund = nil
	}

	return rows
}

func TestFixtureRowsMatchAcrossTraceModes(t *testing.T) {
	for _, name := range []string{testutil.TraceFixtureCreate, testutil.TraceFixtureEOACall} {
		t.Run(name, func(t *testing.T) {
			fx := testutil.LoadTraceFixture(t, name)
			rpc := fx.RPCTrace()

			rpcRows := withoutRefund(buildRows(t, rpc, nil, false))
			streamedRows := withoutRefund(buildRows(t, rpc, framesFromTrace(rpc), true))
			embeddedRows := withoutRefund(buildRows(t, fx.EmbeddedTrace(), nil, false))

			sortRows(rpcRows)
			sortRows(streamedRows)
			sortRows(embeddedRows)

			require.Equal(t, rpcRows, streamedRows)
			require.Equal(t, rpcRows, embeddedRows)
		})
	}
}

func TestFixtureCreateRowsCarryCreatedAddress(t *testing.T) {
	fx := testutil.LoadTraceFixture(t, testutil.TraceFixtureCreate)
	want := fx.CreatedAddresses()
	require.Equal(t, []string{"0x98684bdd077dfec4a8352f9892d6cf46d57fc61b", "0x21f6b5fb6113933db404697ea300c7414ecc295a"}, want)

	for mode, trace := range map[string]*execution.TraceTransaction{"rpc": fx.RPCTrace(), "embedded": fx.EmbeddedTrace()} {
		rows := buildRows(t, trace, nil, false)
		sortRows(rows)

		var got []string

		for i := range rows {
			if rows[i].Operation == OpcodeCREATE || rows[i].Operation == OpcodeCREATE2 {
				require.NotNil(t, rows[i].CallToAddress, "%s: %s at index %d", mode, rows[i].Operation, rows[i].Index)
				got = append(got, *rows[i].CallToAddress)
			}
		}

		require.Equal(t, want, got, mode)
	}
}

func TestFixtureSyntheticRowsHaveTheirOwnKey(t *testing.T) {
	fx := testutil.LoadTraceFixture(t, testutil.TraceFixtureEOACall)

	type key struct {
		index     uint32
		synthetic bool
	}

	for mode, trace := range map[string]*execution.TraceTransaction{"rpc": fx.RPCTrace(), "embedded": fx.EmbeddedTrace()} {
		rows := buildRows(t, trace, nil, false)
		byIndex := map[uint32][]Structlog{}
		keys := map[key]bool{}
		synthetic := 0

		for _, r := range rows {
			k := key{index: r.Index, synthetic: r.Synthetic}
			require.False(t, keys[k], "%s: duplicate key %+v", mode, k)

			keys[k] = true

			byIndex[r.Index] = append(byIndex[r.Index], r)

			if r.Synthetic {
				synthetic++
			}
		}

		require.Equal(t, 1, synthetic, mode)

		for idx, same := range byIndex {
			if len(same) == 1 {
				continue
			}

			require.Len(t, same, 2, "%s: index %d", mode, idx)

			call, eoa := same[0], same[1]
			if call.Synthetic {
				call, eoa = eoa, call
			}

			require.Equal(t, OpcodeCALL, call.Operation, mode)
			require.Empty(t, eoa.Operation, mode)
			require.Equal(t, call.Depth+1, eoa.Depth, mode)
			require.Equal(t, call.CallToAddress, eoa.CallToAddress, mode)
			require.NotEqual(t, call.CallFrameID, eoa.CallFrameID, mode)
		}
	}
}
