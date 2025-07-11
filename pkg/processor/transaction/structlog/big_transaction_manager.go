package structlog

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// BigTransactionManager coordinates processing of large transactions to prevent OOM
type BigTransactionManager struct {
	// Active big transaction count
	currentBigCount atomic.Int32
	pauseNewWork    atomic.Bool

	// Configuration
	threshold int // Default: 500k structlogs
	log       logrus.FieldLogger
}

// NewBigTransactionManager creates a new manager for big transaction coordination
func NewBigTransactionManager(threshold int, log logrus.FieldLogger) *BigTransactionManager {
	return &BigTransactionManager{
		threshold: threshold,
		log:       log,
	}
}

// RegisterBigTransaction marks a big transaction as actively processing
func (btm *BigTransactionManager) RegisterBigTransaction(txHash string) {
	count := btm.currentBigCount.Add(1)

	// Set pause flag when ANY big transaction is active
	btm.pauseNewWork.Store(true)
	btm.log.WithFields(logrus.Fields{
		"tx_hash":       txHash,
		"total_big_txs": count,
	}).Debug("Registered big transaction")
}

// UnregisterBigTransaction marks a big transaction as complete
func (btm *BigTransactionManager) UnregisterBigTransaction(txHash string) {
	newCount := btm.currentBigCount.Add(-1)

	// Only clear pause when NO big transactions remain
	if newCount == 0 {
		btm.pauseNewWork.Store(false)
		btm.log.Info("All big transactions complete, resuming normal processing")
	} else {
		btm.log.WithFields(logrus.Fields{
			"tx_hash":           txHash,
			"remaining_big_txs": newCount,
		}).Debug("Big transaction complete")
	}
}

// ShouldPauseNewWork returns true if new work should be paused
func (btm *BigTransactionManager) ShouldPauseNewWork() bool {
	return btm.pauseNewWork.Load()
}

// GetThreshold returns the threshold for big transactions
func (btm *BigTransactionManager) GetThreshold() int {
	return btm.threshold
}
