package structlog_agg

import (
	"context"
	"math/big"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// optionsRecordingNode is a ready execution node that records the trace options it is asked for.
type optionsRecordingNode struct {
	execution.Node

	opts []execution.TraceOptions
}

func (n *optionsRecordingNode) Name() string { return "recorder" }

func (n *optionsRecordingNode) Start(context.Context) error { return nil }

func (n *optionsRecordingNode) OnReady(ctx context.Context, callback func(context.Context) error) {
	_ = callback(ctx)
}

func (n *optionsRecordingNode) DebugTraceTransaction(_ context.Context, _ string, _ *big.Int, opts execution.TraceOptions) (*execution.TraceTransaction, error) {
	n.opts = append(n.opts, opts)

	return &execution.TraceTransaction{}, nil
}

type stubTransaction struct {
	execution.Transaction
}

func (stubTransaction) Hash() execution.Hash { return execution.Hash{} }

type stubBlock struct {
	execution.Block
}

func (stubBlock) Number() *big.Int { return big.NewInt(1) }

func TestGetTransactionTraceRequestsNoReturnData(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node := &optionsRecordingNode{}
	pool := ethereum.NewPoolWithNodes(log, "structlog_agg_test", []execution.Node{node}, nil)
	pool.Start(context.Background())

	p := &Processor{pool: pool}

	_, err := p.getTransactionTrace(context.Background(), stubTransaction{}, stubBlock{})
	require.NoError(t, err)
	require.Len(t, node.opts, 1)

	opts := node.opts[0]
	require.False(t, opts.EnableReturnData, "return data is copied into every opcode and the aggregation never reads it")
	require.True(t, opts.DisableMemory)
	require.True(t, opts.DisableStorage)
	require.False(t, opts.DisableStack, "RPC-mode call and create targets come from the stack")
}
