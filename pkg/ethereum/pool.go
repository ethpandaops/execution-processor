package ethereum

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Pool struct {
	log            logrus.FieldLogger
	executionNodes []execution.Node
	metrics        *Metrics
	config         *Config

	mu sync.RWMutex

	healthyExecutionNodes map[execution.Node]bool

	// Goroutine management
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewPoolWithNodes creates a pool with pre-created Node implementations.
// Use this when embedding execution-processor as a library where the host
// provides custom Node implementations (e.g., EmbeddedNode with DataSource).
//
// Parameters:
//   - log: Logger for pool operations
//   - namespace: Metrics namespace prefix (will have "_ethereum" appended)
//   - nodes: Pre-created Node implementations
//   - config: Optional configuration (nil creates empty config with defaults)
//
// Example:
//
//	// Create embedded node with custom data source
//	dataSource := &MyDataSource{client: myClient}
//	node := execution.NewEmbeddedNode(log, "my-node", dataSource)
//
//	// Create pool with the embedded node
//	pool := ethereum.NewPoolWithNodes(log, "processor", []execution.Node{node}, nil)
//	pool.Start(ctx)
//
//	// Mark ready when data source is ready
//	node.MarkReady(ctx)
func NewPoolWithNodes(log logrus.FieldLogger, namespace string, nodes []execution.Node, config *Config) *Pool {
	namespace = fmt.Sprintf("%s_ethereum", namespace)

	// If config is nil, create an empty config
	if config == nil {
		config = &Config{}
	}

	return &Pool{
		log:                   log,
		executionNodes:        nodes,
		healthyExecutionNodes: make(map[execution.Node]bool, len(nodes)),
		metrics:               GetMetricsInstance(namespace),
		config:                config,
	}
}

func (p *Pool) HasExecutionNodes() bool {
	return len(p.executionNodes) > 0
}

func (p *Pool) HasHealthyExecutionNodes() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, isHealthy := range p.healthyExecutionNodes {
		if isHealthy {
			return true
		}
	}

	return false
}

func (p *Pool) GetHealthyExecutionNodes() []execution.Node {
	p.mu.RLock()
	defer p.mu.RUnlock()

	healthyNodes := make([]execution.Node, 0, len(p.healthyExecutionNodes))

	for node, healthy := range p.healthyExecutionNodes {
		if healthy {
			healthyNodes = append(healthyNodes, node)
		}
	}

	return healthyNodes
}

func (p *Pool) GetHealthyExecutionNode() execution.Node {
	p.mu.RLock()
	defer p.mu.RUnlock()

	healthyNodes := make([]execution.Node, 0, len(p.healthyExecutionNodes))

	for node, healthy := range p.healthyExecutionNodes {
		if healthy {
			healthyNodes = append(healthyNodes, node)
		}
	}

	if len(healthyNodes) == 0 {
		return nil
	}

	//nolint:gosec // doesn't matter
	return healthyNodes[rand.IntN(len(healthyNodes))]
}

func (p *Pool) WaitForHealthyExecutionNode(ctx context.Context) (execution.Node, error) {
	// Check if we have any execution nodes configured
	if len(p.executionNodes) == 0 {
		return nil, fmt.Errorf("no execution nodes configured")
	}

	attemptCount := 0
	startTime := time.Now()

	// Log initial state
	p.log.WithField("total_nodes", len(p.executionNodes)).Info("Waiting for healthy execution node")

	// Create a ticker for logging status
	statusLogTicker := time.NewTicker(10 * time.Second)
	defer statusLogTicker.Stop()

	// Create a channel to receive status updates
	statusChan := make(chan struct{})

	// Start a goroutine to log status periodically
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-ctx.Done():
				return
			case <-statusChan:
				// Status channel closed, exit
				return
			case <-statusLogTicker.C:
				totalNodes := len(p.executionNodes)

				// Get count of currently healthy nodes
				p.mu.RLock()

				healthyCount := 0

				for _, isHealthy := range p.healthyExecutionNodes {
					if isHealthy {
						healthyCount++
					}
				}

				p.mu.RUnlock()

				p.log.WithFields(logrus.Fields{
					"healthy_nodes": healthyCount,
					"total_nodes":   totalNodes,
					"attempts":      attemptCount,
					"waiting_for":   time.Since(startTime).Round(time.Second),
				}).Info("Waiting for healthy execution node...")
			}
		}
	}()

	// Main waiting loop
	for {
		attemptCount++

		// Log every 30 attempts (every 30 seconds)
		if attemptCount%30 == 1 {
			p.log.WithField("attempt", attemptCount).Debug("Checking for healthy execution node")
		}

		if node := p.GetHealthyExecutionNode(); node != nil {
			close(statusChan)

			p.log.WithFields(logrus.Fields{
				"attempts": attemptCount,
				"duration": time.Since(startTime).Round(time.Millisecond),
			}).Info("Found healthy execution node")

			return node, nil
		}

		select {
		case <-ctx.Done():
			// Context cancelled or timed out
			close(statusChan)

			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (p *Pool) Start(ctx context.Context) {
	// Create a new error group that doesn't propagate cancellation to children
	g := new(errgroup.Group)

	p.UpdateNodeMetrics()

	for _, node := range p.executionNodes {
		g.Go(func() error {
			node.OnReady(ctx, func(innerCtx context.Context) error {
				p.mu.Lock()
				p.healthyExecutionNodes[node] = true
				p.mu.Unlock()

				return nil
			})

			return node.Start(ctx)
		})
	}

	// Start status reporting goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.mu.RLock()
				healthyExec := len(p.healthyExecutionNodes)
				totalExec := len(p.executionNodes)
				p.mu.RUnlock()

				p.log.WithFields(logrus.Fields{
					"healthy_execution_nodes": fmt.Sprintf("%d/%d", healthyExec, totalExec),
				}).Info("Pool status")
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.UpdateNodeMetrics()
			}
		}
	}()

	go func() {
		if err := g.Wait(); err != nil {
			if ctx.Err() != nil {
				return
			}

			p.log.WithError(err).Error("error in pool")
		}
	}()
}

func (p *Pool) UpdateNodeMetrics() {
	p.mu.Lock()
	healthyExec := len(p.healthyExecutionNodes)
	unhealthyExec := len(p.executionNodes) - healthyExec
	p.mu.Unlock()

	p.metrics.SetNodesTotal(float64(healthyExec), []string{"execution", "healthy"})
	p.metrics.SetNodesTotal(float64(unhealthyExec), []string{"execution", "unhealthy"})
}

// Stop gracefully shuts down the pool.
func (p *Pool) Stop(ctx context.Context) error {
	p.log.Info("Stopping pool")

	// Cancel the pool context to signal all goroutines to stop
	if p.cancel != nil {
		p.cancel()
	}

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.log.Info("All pool goroutines stopped gracefully")
	case <-ctx.Done():
		p.log.Warn("Timeout waiting for pool goroutines to stop")
	}

	// Stop all execution nodes
	for _, node := range p.executionNodes {
		if err := node.Stop(ctx); err != nil {
			p.log.WithError(err).Error("Failed to stop execution node")
		}
	}

	return nil
}

// GetNetworkByChainID returns the network information for the given chain ID.
// If overrideNetworkName is set in config, it returns that name instead of using networkMap.
func (p *Pool) GetNetworkByChainID(chainID int32) (*Network, error) {
	// If override is set, use it instead of the networkMap
	if p.config.OverrideNetworkName != nil && *p.config.OverrideNetworkName != "" {
		return &Network{
			ID:   chainID,
			Name: *p.config.OverrideNetworkName,
		}, nil
	}

	// Otherwise use the standard networkMap lookup
	return GetNetworkByChainID(chainID)
}
