package structlog

import (
	"fmt"
	"time"
)

// ClickHouseTime wraps time.Time to provide custom JSON marshaling for ClickHouse.
type ClickHouseTime struct {
	time.Time
}

// MarshalJSON formats the time for ClickHouse DateTime format.
func (t ClickHouseTime) MarshalJSON() ([]byte, error) {
	// ClickHouse expects DateTime in the format: "2006-01-02 15:04:05"
	formatted := fmt.Sprintf("%q", t.Format("2006-01-02 15:04:05"))

	return []byte(formatted), nil
}

// UnmarshalJSON parses the time from ClickHouse DateTime format.
func (t *ClickHouseTime) UnmarshalJSON(data []byte) error {
	// Remove quotes
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return fmt.Errorf("invalid time format")
	}

	timeStr := string(data[1 : len(data)-1])

	parsed, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		return err
	}

	t.Time = parsed

	return nil
}

// NewClickHouseTime creates a new ClickHouseTime from a time.Time.
func NewClickHouseTime(t time.Time) ClickHouseTime {
	return ClickHouseTime{Time: t}
}
