package cryo

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

// GetHandlers returns the task handlers for this processor.
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		p.processForwardsTaskType():  p.handleProcessTask,
		p.processBackwardsTaskType(): p.handleProcessTask,
	}
}

// handleProcessTask collects one block for the whole group and writes every
// dataset it produced. The invocation succeeds or fails as a unit, so the block
// is only marked complete once every dataset has been flushed.
func (p *Processor) handleProcessTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()
	queue := p.queueFor(p.processingMode)

	defer func() {
		common.TaskProcessingDuration.WithLabelValues(
			p.network.Name, p.name, queue, task.Type(),
		).Observe(time.Since(start).Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(
			p.network.Name, p.name, queue, task.Type(), "unmarshal_error",
		).Inc()

		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	blockNumber := payload.BlockNumber.Uint64()

	if blockNumber < p.group.MinBlock {
		p.log.WithFields(logrus.Fields{
			"block_number": blockNumber,
			"min_block":    p.group.MinBlock,
		}).Debug("Block below the group's floor, marking complete without collecting")

		return p.completeBlock(ctx, blockNumber, payload.ProcessingMode)
	}

	tables, err := p.fetch(ctx, blockNumber)
	if err != nil {
		common.TasksErrored.WithLabelValues(
			p.network.Name, p.name, queue, task.Type(), "fetch_error",
		).Inc()

		return err
	}

	if coverageErr := verifyCoverage(p.group, tables, blockNumber, blockNumber); coverageErr != nil {
		CryoFetchErrors.WithLabelValues(p.network.Name, p.name, "coverage").Inc()
		common.TasksErrored.WithLabelValues(
			p.network.Name, p.name, queue, task.Type(), "coverage_error",
		).Inc()

		return fmt.Errorf("block %d: %w", blockNumber, coverageErr)
	}

	rowCount, err := p.consume(ctx, tables)
	if err != nil {
		common.TasksErrored.WithLabelValues(
			p.network.Name, p.name, queue, task.Type(), "insert_error",
		).Inc()

		return err
	}

	common.TasksProcessed.WithLabelValues(
		p.network.Name, p.name, queue, task.Type(), "success",
	).Inc()

	if err := p.completeBlock(ctx, blockNumber, payload.ProcessingMode); err != nil {
		return err
	}

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"rows":         rowCount,
		"duration":     time.Since(start),
	}).Info("Processed block")

	return nil
}

// fetch runs cryo for one block and returns the decoded tables per dataset.
func (p *Processor) fetch(ctx context.Context, blockNumber uint64) (map[string]*decode.Table, error) {
	start := time.Now()

	tables, err := p.fetcher.Fetch(ctx, p.group, blockNumber, blockNumber)

	CryoFetchDuration.WithLabelValues(p.network.Name, p.name).Observe(time.Since(start).Seconds())

	if err != nil {
		CryoFetchErrors.WithLabelValues(p.network.Name, p.name, "collect").Inc()

		return nil, fmt.Errorf("collect block %d: %w", blockNumber, err)
	}

	return tables, nil
}

// consume maps every dataset's table into its row buffer.
//
// The datasets are submitted concurrently because each Submit blocks until its
// own buffer flushes. Sequentially, a block would wait one flush interval per
// dataset — sixteen of them, dwarfing the cryo invocation itself.
func (p *Processor) consume(ctx context.Context, tables map[string]*decode.Table) (int, error) {
	meta := rowMeta{
		network: p.network.Name,
		updated: time.Now().UTC().Truncate(time.Second),
	}

	start := time.Now()

	for _, s := range p.sinks {
		if _, ok := tables[s.Dataset()]; !ok {
			return 0, fmt.Errorf("cryo produced no output for dataset %s", s.Dataset())
		}
	}

	var (
		group  errgroup.Group
		counts = make([]int, len(p.sinks))
	)

	for i, s := range p.sinks {
		group.Go(func() error {
			rows, err := s.Consume(ctx, tables[s.Dataset()], meta)
			if err != nil {
				return err
			}

			counts[i] = rows

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return 0, err
	}

	var total int

	for i, s := range p.sinks {
		if counts[i] > 0 {
			CryoRowsDecoded.WithLabelValues(p.network.Name, p.name, s.Dataset()).Add(float64(counts[i]))
		}

		total += counts[i]
	}

	CryoSubmitDuration.WithLabelValues(p.network.Name, p.name).Observe(time.Since(start).Seconds())

	return total, nil
}

// completeBlock records the single task this block had as done.
func (p *Processor) completeBlock(ctx context.Context, blockNumber uint64, mode string) error {
	taskID := GenerateTaskID(p.name, p.network.Name, blockNumber)

	allComplete, err := p.completionTracker.TrackTaskCompletion(ctx, taskID, blockNumber, p.network.Name, p.name, mode)
	if err != nil {
		// Non-fatal: stale detection will pick the block up.
		p.log.WithError(err).WithFields(logrus.Fields{
			"block_number": blockNumber,
			"task_id":      taskID,
		}).Warn("Failed to track task completion")
	}

	if !allComplete {
		return nil
	}

	if err := p.completionTracker.MarkBlockComplete(ctx, blockNumber, p.network.Name, p.name, mode); err != nil {
		return fmt.Errorf("mark block %d complete: %w", blockNumber, err)
	}

	return nil
}
