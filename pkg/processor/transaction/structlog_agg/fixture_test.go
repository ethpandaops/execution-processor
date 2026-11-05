package structlog_agg

import (
	"encoding/hex"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/internal/testutil"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

func sortFrames(rows []CallFrameRow) []CallFrameRow {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CallFrameID != rows[j].CallFrameID {
			return rows[i].CallFrameID < rows[j].CallFrameID
		}

		return rows[i].Operation < rows[j].Operation
	})

	return rows
}

func TestFixtureCallFramesMatchAcrossTraceModes(t *testing.T) {
	for _, name := range []string{testutil.TraceFixtureCreate, testutil.TraceFixtureEOACall} {
		t.Run(name, func(t *testing.T) {
			fx := testutil.LoadTraceFixture(t, name)
			tx := fx.Tx(t)
			p := &Processor{}

			rpc := sortFrames(p.buildCallFrames(fx.RPCTrace(), tx))
			embedded := sortFrames(p.buildCallFrames(fx.EmbeddedTrace(), tx))

			require.Equal(t, rpc, embedded)

			for _, row := range rpc {
				require.NotNil(t, row.TargetAddress, "frame %d %s has no target", row.CallFrameID, row.CallType)
			}
		})
	}
}

func TestFixtureCreateFramesTargetCreatedAddress(t *testing.T) {
	fx := testutil.LoadTraceFixture(t, testutil.TraceFixtureCreate)
	tx := fx.Tx(t)
	p := &Processor{}

	for mode, trace := range map[string]*execution.TraceTransaction{"rpc": fx.RPCTrace(), "embedded": fx.EmbeddedTrace()} {
		var got []string

		for _, row := range sortFrames(p.buildCallFrames(trace, tx)) {
			if row.Operation == "" && (row.CallType == opcodeCREATE || row.CallType == opcodeCREATE2) {
				require.NotNil(t, row.TargetAddress, mode)
				got = append(got, *row.TargetAddress)
			}
		}

		require.Equal(t, fx.CreatedAddresses(), got, mode)
	}
}

func TestFixtureIntrinsicGasReconcilesReceipt(t *testing.T) {
	// Each intrinsic value is the gas limit minus the gas left at the first opcode. The floor
	// transaction runs no code, so its value is 21000 + 4 per calldata token (10,561 tokens).
	tests := []struct {
		fixture   string
		intrinsic uint64
	}{
		{testutil.TraceFixtureCreate, 27944},
		{testutil.TraceFixtureEOACall, 21264},
		{testutil.TraceFixtureAccessList, 55124},
		{testutil.TraceFixtureRefund, 21264},
		{testutil.TraceFixtureAuthorization, 46064},
		{testutil.TraceFixtureCalldataFloor, 63244},
		{testutil.TraceFixtureContractCreation, 68894},
	}

	for _, tt := range tests {
		t.Run(tt.fixture, func(t *testing.T) {
			fx := testutil.LoadTraceFixture(t, tt.fixture)
			tx := fx.Tx(t)
			p := &Processor{}

			for mode, trace := range map[string]*execution.TraceTransaction{"rpc": fx.RPCTrace(), "embedded": fx.EmbeddedTrace()} {
				if len(trace.Structlogs) > 0 {
					require.Equal(t, tx.Gas()-trace.Structlogs[0].Gas, tt.intrinsic, mode)
				}

				// Geth reports the receipt's gas here and erigone the gas before refunds and the
				// calldata floor; intrinsic gas must depend on neither.
				trace.Gas = 0

				root := getSummaryRow(p.buildCallFrames(trace, tx), 0)
				require.NotNil(t, root, mode)
				require.NotNil(t, root.IntrinsicGas, mode)
				require.Equal(t, tt.intrinsic, *root.IntrinsicGas, mode)

				used := *root.IntrinsicGas + root.GasCumulative

				if tx.To() == nil {
					// A creation's code deposit, 200 gas per deployed byte, is charged after its
					// init code returns, outside every opcode.
					code, err := hex.DecodeString(strings.TrimPrefix(fx.CallTrace.Output, "0x"))
					require.NoError(t, err)

					used += 200 * uint64(len(code))
				}

				paid := max(used-min(root.GasRefund, used/5), calldataFloor(tx.Data()))
				require.Equal(t, fx.ReceiptGasUsed(t), paid, mode)
			}
		})
	}
}

// calldataFloor is the EIP-7623 minimum a transaction pays: 21000 + 10 per calldata token.
func calldataFloor(data []byte) uint64 {
	tokens := uint64(len(data))

	for _, b := range data {
		if b != 0 {
			tokens += 3
		}
	}

	return 21000 + 10*tokens
}
