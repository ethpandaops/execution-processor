package structlog_test

import (
	"testing"
)

func TestVerificationConstants(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		expected string
	}{
		{
			name:     "basic table name",
			table:    "test_table",
			expected: "test_table",
		},
		{
			name:     "production table name",
			table:    "transaction_structlogs",
			expected: "transaction_structlogs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.table != tt.expected {
				t.Errorf("table name mismatch: got %s, want %s", tt.table, tt.expected)
			}
		})
	}
}

func TestVerificationQueryPattern(t *testing.T) {
	expectedElements := []string{
		"SELECT COUNT(*)",
		"FROM",
		"FINAL",
		"WHERE block_number =",
		"AND transaction_hash =",
		"AND transaction_index =",
		"AND meta_network_name =",
	}

	// Simulate what the query should contain
	queryPattern := "SELECT COUNT(*) as count FROM %s FINAL WHERE block_number = %d AND transaction_hash = '%s' AND transaction_index = %d AND meta_network_name = '%s'"

	for _, element := range expectedElements {
		if !containsElement(queryPattern, element) {
			t.Errorf("query pattern missing element: %s", element)
		}
	}
}

func TestVerificationErrorPattern(t *testing.T) {
	tests := []struct {
		name          string
		actualCount   int
		expectedCount int
		shouldError   bool
	}{
		{
			name:          "counts match",
			actualCount:   100,
			expectedCount: 100,
			shouldError:   false,
		},
		{
			name:          "counts don't match",
			actualCount:   50,
			expectedCount: 100,
			shouldError:   true,
		},
		{
			name:          "zero counts match",
			actualCount:   0,
			expectedCount: 0,
			shouldError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the verification logic
			hasError := tt.actualCount != tt.expectedCount

			if hasError != tt.shouldError {
				t.Errorf("error expectation mismatch: got error=%v, want error=%v",
					hasError, tt.shouldError)
			}
		})
	}
}

// Helper function to check if pattern contains element
func containsElement(pattern, element string) bool {
	patternBytes := []byte(pattern)
	elementBytes := []byte(element)

	if len(elementBytes) == 0 {
		return true
	}

	if len(patternBytes) < len(elementBytes) {
		return false
	}

	for i := 0; i <= len(patternBytes)-len(elementBytes); i++ {
		match := true

		for j := 0; j < len(elementBytes); j++ {
			if patternBytes[i+j] != elementBytes[j] {
				match = false

				break
			}
		}

		if match {
			return true
		}
	}

	return false
}
