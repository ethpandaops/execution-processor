package common

import "fmt"

// ProcessQueue returns the process queue name for a processor (deprecated - use mode-specific queues).
func ProcessQueue(processorName string) string {
	return fmt.Sprintf("%s:process", processorName)
}

// VerifyQueue returns the verify queue name for a processor (deprecated - use mode-specific queues).
func VerifyQueue(processorName string) string {
	return fmt.Sprintf("%s:verify", processorName)
}

// ProcessForwardsQueue returns the forwards process queue name for a processor.
func ProcessForwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:forwards", processorName)
}

// ProcessBackwardsQueue returns the backwards process queue name for a processor.
func ProcessBackwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:process:backwards", processorName)
}

// VerifyForwardsQueue returns the forwards verify queue name for a processor.
func VerifyForwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:verify:forwards", processorName)
}

// VerifyBackwardsQueue returns the backwards verify queue name for a processor.
func VerifyBackwardsQueue(processorName string) string {
	return fmt.Sprintf("%s:verify:backwards", processorName)
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

// PrefixedVerifyForwardsQueue returns the forwards verify queue name with prefix.
func PrefixedVerifyForwardsQueue(processorName, prefix string) string {
	queue := VerifyForwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}

// PrefixedVerifyBackwardsQueue returns the backwards verify queue name with prefix.
func PrefixedVerifyBackwardsQueue(processorName, prefix string) string {
	queue := VerifyBackwardsQueue(processorName)
	if prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", prefix, queue)
}
