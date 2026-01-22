//go:build !embedded

package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/rpc"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/go-co-op/gocron"
	"github.com/sirupsen/logrus"
)

type MetadataService struct {
	rpcClient *rpc.Client
	log       logrus.FieldLogger

	onReadyCallbacks []func(context.Context) error

	nodeVersion string
	chainID     int32

	synced bool

	mu sync.Mutex
}

func NewMetadataService(log logrus.FieldLogger, rpcClient *rpc.Client) MetadataService {
	return MetadataService{
		rpcClient:        rpcClient,
		log:              log.WithField("module", "ethereum/execution/metadata"),
		onReadyCallbacks: []func(context.Context) error{},
		mu:               sync.Mutex{},
	}
}

func (m *MetadataService) Start(ctx context.Context) error {
	m.log.Info("Starting metadata service")

	go func() {
		// Configure the exponential backoff
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = 500 * time.Millisecond
		b.MaxInterval = 5 * time.Second
		b.MaxElapsedTime = 2 * time.Minute

		attemptCount := 0

		operation := func() error {
			attemptCount++

			if err := m.RefreshAll(ctx); err != nil {
				m.log.WithError(err).Warn("Failed to refresh metadata, will retry")

				return err
			}

			// Check if we're ready
			if err := m.Ready(ctx); err != nil {
				m.log.WithError(err).Warn("Metadata not ready yet, will retry")

				return err
			}

			m.log.WithFields(logrus.Fields{
				"node_ver": m.nodeVersion,
				"chain_id": m.chainID,
			}).Info("Metadata initialized successfully")

			return nil
		}

		if err := backoff.Retry(operation, b); err != nil {
			m.log.WithError(err).Error("Failed to refresh metadata after retries")

			return
		}

		// Now execute callbacks
		for _, cb := range m.onReadyCallbacks {
			if err := cb(ctx); err != nil {
				m.log.WithError(err).Warn("Failed to execute onReady callback")
			}
		}

		m.log.WithFields(logrus.Fields{
			"node_version": m.nodeVersion,
			"chain_id":     m.chainID,
		}).Info("Metadata service initialization completed")
	}()

	s := gocron.NewScheduler(time.Local)

	if _, err := s.Every("5m").Do(func() {
		// Create a new context with timeout for this specific operation
		refreshCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_ = m.RefreshAll(refreshCtx)
	}); err != nil {
		return err
	}

	if _, err := s.Every("15s").Do(func() {
		// Create a new context with timeout for this specific operation
		syncCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := m.updateSyncStatus(syncCtx); err != nil {
			m.log.WithError(err).Warn("Failed to update sync status")
		}
	}); err != nil {
		return err
	}

	s.StartAsync()

	return nil
}

func (m *MetadataService) Name() Name {
	return "metadata"
}

func (m *MetadataService) Stop(ctx context.Context) error {
	return nil
}

func (m *MetadataService) OnReady(ctx context.Context, cb func(context.Context) error) {
	m.onReadyCallbacks = append(m.onReadyCallbacks, cb)
}

func (m *MetadataService) Ready(ctx context.Context) error {
	if m.nodeVersion == "" {
		return errors.New("node version is not available")
	}

	if m.chainID == 0 {
		return errors.New("chain ID is not available")
	}

	return nil
}

func (m *MetadataService) web3ClientVersion(ctx context.Context) (string, error) {
	var version string

	err := m.rpcClient.CallContext(ctx, &version, "web3_clientVersion")
	if err != nil {
		return "", err
	}

	return version, nil
}

func (m *MetadataService) GetChainID(ctx context.Context) (*int32, error) {
	var chainID string

	err := m.rpcClient.CallContext(ctx, &chainID, "eth_chainId")
	if err != nil {
		return nil, err
	}

	m.log.WithField("raw_chain_id", chainID).Debug("Retrieved chain ID from RPC")

	// Remove "0x" prefix if present for proper parsing
	chainIDStr := strings.TrimPrefix(chainID, "0x")

	// Parse the hex string to int64
	chainIDInt, err := strconv.ParseInt(chainIDStr, 16, 32)
	if err != nil {
		m.log.WithFields(logrus.Fields{
			"raw_chain_id": chainID,
			"parsed_value": chainIDStr,
			"error":        err,
		}).Error("Failed to parse chain ID as hex")

		return nil, fmt.Errorf("failed to parse chain ID %s: %w", chainID, err)
	}

	chainIDInt32 := int32(chainIDInt)

	return &chainIDInt32, nil
}

func (m *MetadataService) RefreshAll(ctx context.Context) error {
	// Fetch client version
	version, err := m.web3ClientVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get client version: %w", err)
	}

	m.nodeVersion = version

	// Fetch chain ID
	chainID, err := m.GetChainID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %w", err)
	}

	if chainID == nil {
		return errors.New("chain ID is not available")
	}

	m.chainID = *chainID

	return nil
}

func (m *MetadataService) Client(ctx context.Context) string {
	return string(ClientFromString(m.nodeVersion))
}

func (m *MetadataService) ClientVersion() string {
	return m.nodeVersion
}

func (m *MetadataService) updateSyncStatus(ctx context.Context) error {
	var raw interface{}

	err := m.rpcClient.CallContext(ctx, &raw, "eth_syncing")
	if err != nil {
		return err
	}

	// eth_syncing returns false when not syncing, or an object when syncing
	switch v := raw.(type) {
	case bool:
		m.synced = !v
	case map[string]interface{}:
		// Still syncing - has sync progress info
		m.synced = false
	default:
		return ethereum.NotFound
	}

	return nil
}

func (m *MetadataService) IsSynced() bool {
	return m.synced
}

func (m *MetadataService) ChainID() int32 {
	return m.chainID
}
