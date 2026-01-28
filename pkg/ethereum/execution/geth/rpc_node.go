//go:build !embedded

package geth

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution/geth/services"
)

// Compile-time check that RPCNode implements execution.Node interface.
var _ execution.Node = (*RPCNode)(nil)

// headerTransport adds custom headers to requests and respects context cancellation.
type headerTransport struct {
	headers map[string]string
	base    http.RoundTripper
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Add custom headers
	for key, value := range t.headers {
		req.Header.Set(key, value)
	}

	// Check if context is already cancelled before making request
	if req.Context().Err() != nil {
		return nil, req.Context().Err()
	}

	// Make the request with context
	return t.base.RoundTrip(req)
}

// RPCNode implements execution.Node using JSON-RPC connections.
type RPCNode struct {
	config    *execution.Config
	log       logrus.FieldLogger
	client    *ethclient.Client
	rpcClient *rpc.Client
	name      string

	services []services.Service

	onReadyCallbacks []func(ctx context.Context) error

	// Goroutine management
	mu     sync.RWMutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewRPCNode creates a new RPC-based execution node.
func NewRPCNode(log logrus.FieldLogger, conf *execution.Config) *RPCNode {
	return &RPCNode{
		config:   conf,
		log:      log.WithFields(logrus.Fields{"type": "execution", "source": conf.Name}),
		services: []services.Service{},
	}
}

func (n *RPCNode) OnReady(_ context.Context, callback func(ctx context.Context) error) {
	n.onReadyCallbacks = append(n.onReadyCallbacks, callback)
}

func (n *RPCNode) Start(ctx context.Context) error {
	n.log.WithFields(logrus.Fields{
		"node_name": n.name,
	}).Info("Starting execution node")

	// Create internal context for node lifecycle
	nodeCtx, cancel := context.WithCancel(ctx)

	n.mu.Lock()
	n.cancel = cancel
	n.mu.Unlock()

	// Create HTTP client without fixed timeout - let context handle it
	httpClient := http.Client{
		// Remove Timeout to let context control request lifecycle
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			// Connection timeouts
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 0, // No timeout - let context handle it
			ExpectContinueTimeout: 1 * time.Second,
			// Allow more idle connections for better performance
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	httpClient.Transport = &headerTransport{
		headers: n.config.NodeHeaders,
		base:    httpClient.Transport,
	}

	// Create raw RPC client with custom HTTP client
	rpcClient, err := rpc.DialOptions(nodeCtx, n.config.NodeAddress, rpc.WithHTTPClient(&httpClient))
	if err != nil {
		n.log.WithError(err).Error("Failed to create RPC client")

		return fmt.Errorf("failed to create RPC client for %s: %w", n.config.NodeAddress, err)
	}

	// Create ethclient wrapper
	client := ethclient.NewClient(rpcClient)

	metadata := services.NewMetadataService(n.log, rpcClient)

	svcs := []services.Service{
		&metadata,
	}

	n.client = client
	n.rpcClient = rpcClient
	n.services = svcs

	errs := make(chan error, 1)

	go func() {
		wg := sync.WaitGroup{}

		for _, service := range n.services {
			serviceName := service.Name()

			wg.Add(1)

			// Create a new context specifically for the OnReady callback
			readyCtx, readyCancel := context.WithTimeout(context.Background(), 30*time.Second)

			service.OnReady(readyCtx, func(_ context.Context) error {
				n.log.WithField("service", serviceName).Info("Service is ready")

				wg.Done()

				return nil
			})

			n.log.WithField("service", serviceName).Info("Starting service")

			// Use a separate goroutine to start the service to avoid blocking
			n.wg.Add(1)

			go func() {
				defer n.wg.Done()

				if err := service.Start(nodeCtx); err != nil {
					if nodeCtx.Err() == nil { // Only log if the error isn't due to context cancellation
						n.log.WithError(err).WithField("service", serviceName).
							Error("Failed to start service")

						errs <- fmt.Errorf("failed to start service %s: %w", serviceName, err)
					}
				}
			}()

			wg.Wait()

			readyCancel() // Move cancel here after wg.Wait()
		}

		n.log.Info("All services are ready")

		// Log the detected client type
		clientType := n.Metadata().Client(ctx)

		n.log.WithField("client_type", clientType).Info("Detected execution client type")

		for _, callback := range n.onReadyCallbacks {
			// Create a new context for each callback
			callbackCtx, callbackCancel := context.WithTimeout(context.Background(), 10*time.Second)

			if err := callback(callbackCtx); err != nil {
				n.log.WithError(err).Error("Failed to run on ready callback")

				errs <- fmt.Errorf("failed to run on ready callback: %w", err)
			}

			callbackCancel() // Call cancel immediately after callback completes
		}

		n.log.Info("Node initialization completed")
	}()

	return nil
}

func (n *RPCNode) Stop(ctx context.Context) error {
	n.log.Info("Stopping execution node")

	// Cancel the node context to signal all goroutines to stop
	n.mu.Lock()

	if n.cancel != nil {
		n.cancel()
	}

	n.mu.Unlock()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})

	go func() {
		n.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		n.log.Info("All node goroutines stopped gracefully")
	case <-ctx.Done():
		n.log.Warn("Timeout waiting for node goroutines to stop")
	}

	// Stop all services
	for _, service := range n.services {
		if err := service.Stop(ctx); err != nil {
			n.log.WithError(err).WithField("service", service.Name()).Error("Failed to stop service")
		}
	}

	return nil
}

func (n *RPCNode) getServiceByName(name services.Name) (services.Service, error) {
	for _, service := range n.services {
		if service.Name() == name {
			return service, nil
		}
	}

	return nil, errors.New("service not found")
}

// Metadata returns the metadata service for this node.
func (n *RPCNode) Metadata() *services.MetadataService {
	service, err := n.getServiceByName("metadata")
	if err != nil {
		// This should never happen. If it does, good luck.
		return nil
	}

	svc, ok := service.(*services.MetadataService)
	if !ok {
		return nil
	}

	return svc
}

// Name returns the configured name for this node.
func (n *RPCNode) Name() string {
	return n.config.Name
}

// ChainID returns the chain ID from the metadata service.
func (n *RPCNode) ChainID() int64 {
	if meta := n.Metadata(); meta != nil {
		return meta.ChainID()
	}

	return 0
}

// ClientType returns the client type from the metadata service.
func (n *RPCNode) ClientType() string {
	if meta := n.Metadata(); meta != nil {
		return meta.ClientVersion()
	}

	return ""
}

// IsSynced returns true if the node is synced.
func (n *RPCNode) IsSynced() bool {
	if meta := n.Metadata(); meta != nil {
		return meta.IsSynced()
	}

	return false
}

// BlockNumber returns the current block number.
func (n *RPCNode) BlockNumber(ctx context.Context) (*uint64, error) {
	return n.blockNumber(ctx)
}

// BlockByNumber returns the block at the given number.
func (n *RPCNode) BlockByNumber(ctx context.Context, number *big.Int) (execution.Block, error) {
	return n.blockByNumber(ctx, number)
}

// BlockReceipts returns all receipts for the block at the given number.
func (n *RPCNode) BlockReceipts(ctx context.Context, number *big.Int) ([]execution.Receipt, error) {
	return n.blockReceipts(ctx, number)
}

// TransactionReceipt returns the receipt for the transaction with the given hash.
func (n *RPCNode) TransactionReceipt(ctx context.Context, hash string) (execution.Receipt, error) {
	return n.transactionReceipt(ctx, hash)
}

// DebugTraceTransaction returns the execution trace for the transaction.
func (n *RPCNode) DebugTraceTransaction(
	ctx context.Context,
	hash string,
	blockNumber *big.Int,
	opts execution.TraceOptions,
) (*execution.TraceTransaction, error) {
	return n.debugTraceTransaction(ctx, hash, blockNumber, opts)
}
