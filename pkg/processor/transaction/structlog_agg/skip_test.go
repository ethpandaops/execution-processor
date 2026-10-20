package structlog_agg

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

// refusingNode is a ready execution node whose traces are always refused for size.
type refusingNode struct {
	execution.Node
}

func (refusingNode) Name() string                { return "refusing" }
func (refusingNode) Start(context.Context) error { return nil }

func (refusingNode) OnReady(ctx context.Context, callback func(context.Context) error) {
	_ = callback(ctx)
}

func (refusingNode) DebugTraceTransaction(context.Context, string, *big.Int, execution.TraceOptions) (*execution.TraceTransaction, error) {
	return nil, fmt.Errorf("rpc: %w", execution.ErrResponseTooLarge)
}

func TestProcessTransactionSkipsOversizedTrace(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	pool := ethereum.NewPoolWithNodes(log, "structlog_agg_skip_test", []execution.Node{refusingNode{}}, nil)
	pool.Start(context.Background())

	network := "structlog-agg-skip-test"
	p := &Processor{log: log, pool: pool, network: &ethereum.Network{Name: network}}

	skipped := common.TransactionsSkipped.WithLabelValues(network, "structlog_agg", tracker.SkipReasonTraceTooLarge)
	before := counterValue(t, skipped)

	frames, err := p.ProcessTransaction(context.Background(), stubBlock{}, 0, stubTransaction{})
	require.NoError(t, err)
	require.Zero(t, frames)
	require.Equal(t, before+1, counterValue(t, skipped))
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()

	var m dto.Metric
	require.NoError(t, c.Write(&m))

	return m.GetCounter().GetValue()
}
