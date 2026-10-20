package tracker

import (
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// SkipReasonTraceTooLarge labels transactions whose trace response passed the node's size limit.
const SkipReasonTraceTooLarge = "trace_too_large"

// TraceFailure counts and logs a trace refused for size and returns nil for it, so the transaction is skipped rather than retried; other errors come back wrapped.
func TraceFailure(log logrus.FieldLogger, network, processor string, blockNumber uint64, txHash string, err error) error {
	if !errors.Is(err, execution.ErrResponseTooLarge) {
		return fmt.Errorf("failed to get trace: %w", err)
	}

	common.TransactionsSkipped.WithLabelValues(network, processor, SkipReasonTraceTooLarge).Inc()

	log.WithError(err).WithFields(logrus.Fields{
		"network":          network,
		"processor":        processor,
		"block_number":     blockNumber,
		"transaction_hash": txHash,
	}).Error("Skipped transaction: trace response exceeds the size limit")

	return nil
}
