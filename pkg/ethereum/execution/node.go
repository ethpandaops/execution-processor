package execution

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/0xsequence/ethkit/ethrpc"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution/services"
	"github.com/sirupsen/logrus"
)

// headerTransport adds custom headers to requests and respects context cancellation
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

type Node struct {
	config *Config
	log    logrus.FieldLogger
	rpc    *ethrpc.Provider
	name   string

	services []services.Service

	onReadyCallbacks []func(ctx context.Context) error

	// Goroutine management
	mu     sync.RWMutex
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewNode(log logrus.FieldLogger, conf *Config) *Node {
	return &Node{
		config:   conf,
		log:      log.WithFields(logrus.Fields{"type": "execution", "source": conf.Name}),
		services: []services.Service{},
	}
}

func (n *Node) OnReady(_ context.Context, callback func(ctx context.Context) error) {
	n.onReadyCallbacks = append(n.onReadyCallbacks, callback)
}

func (n *Node) Start(ctx context.Context) error {
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

	rpc, err := ethrpc.NewProvider(n.config.NodeAddress, ethrpc.WithHTTPClient(&httpClient))
	if err != nil {
		n.log.WithError(err).Error("Failed to create RPC provider")

		return fmt.Errorf("failed to create RPC provider for %s: %w", n.config.NodeAddress, err)
	}

	metadata := services.NewMetadataService(n.log, rpc)

	svcs := []services.Service{
		&metadata,
	}

	n.rpc = rpc
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

func (n *Node) Stop(ctx context.Context) error {
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

func (n *Node) getServiceByName(name services.Name) (services.Service, error) {
	for _, service := range n.services {
		if service.Name() == name {
			return service, nil
		}
	}

	return nil, errors.New("service not found")
}

func (n *Node) Metadata() *services.MetadataService {
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

func (n *Node) Name() string {
	return n.config.Name
}
