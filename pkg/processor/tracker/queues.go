package tracker

import "fmt"

// ProcessQueue returns the process queue name for a processor (deprecated - use mode-specific queues).
func ProcessQueue(processorName string) string {
	return fmt.Sprintf("%s:process", processorName)
}

// ProcessForwardsQueue returns the forwards process queue name for a processor.
func ProcessForwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:forwards", processorName)
}

// ProcessBackwardsQueue returns the backwards process queue name for a processor.
func ProcessBackwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:backwards", processorName)
}

// PrefixedProcessForwardsQueue returns the forwards process queue name with prefix.
func PrefixedProcessForwardsQueue(processorName, prefix string) string {
	queue := ProcessForwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}

// PrefixedProcessBackwardsQueue returns the backwards process queue name with prefix.
func PrefixedProcessBackwardsQueue(processorName, prefix string) string {
	queue := ProcessBackwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}

// ProcessReprocessForwardsQueue returns the reprocess forwards queue name for a processor.
func ProcessReprocessForwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:reprocess:forwards", processorName)
}

// PrefixedProcessReprocessForwardsQueue returns the reprocess forwards queue name with prefix.
func PrefixedProcessReprocessForwardsQueue(processorName, prefix string) string {
	queue := ProcessReprocessForwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}

// ProcessReprocessBackwardsQueue returns the reprocess backwards queue name for a processor.
func ProcessReprocessBackwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:reprocess:backwards", processorName)
}

// PrefixedProcessReprocessBackwardsQueue returns the reprocess backwards queue name with prefix.
func PrefixedProcessReprocessBackwardsQueue(processorName, prefix string) string {
	queue := ProcessReprocessBackwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}
