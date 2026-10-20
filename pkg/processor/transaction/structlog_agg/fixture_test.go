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

type fixtureTx struct {
	execution.Transaction

	to *execution.Address
}

func (tx fixtureTx) To() *execution.Address { return tx.to }

func newFixtureTx(t *testing.T, fx *testutil.TraceFixture) fixtureTx {
	t.Helper()

	b, err := hex.DecodeString(strings.TrimPrefix(fx.CallTrace.To, "0x"))
	require.NoError(t, err)

	var to execution.Address

	copy(to[:], b)

	return fixtureTx{to: &to}
}

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
			tx := newFixtureTx(t, fx)
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
	tx := newFixtureTx(t, fx)
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
