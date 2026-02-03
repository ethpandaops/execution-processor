//go:build !embedded

package ethereum

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution/geth"
	"github.com/sirupsen/logrus"
)

// NewPool creates a new pool from config, using RPC nodes.
// This function imports go-ethereum types through the geth package.
// For embedded mode (no go-ethereum dependency), use NewPoolWithNodes instead.
func NewPool(log logrus.FieldLogger, namespace string, config *Config) *Pool {
	namespace = fmt.Sprintf("%s_ethereum", namespace)
	p := &Pool{
		log:                   log,
		executionNodes:        make([]execution.Node, 0, len(config.Execution)),
		healthyExecutionNodes: make(map[execution.Node]bool, len(config.Execution)),
		metrics:               GetMetricsInstance(namespace),
		config:                config,
	}

	for _, execCfg := range config.Execution {
		node := geth.NewRPCNode(log, execCfg)
		p.executionNodes = append(p.executionNodes, node)
	}

	return p
}
