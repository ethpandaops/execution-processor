package execution

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeGasCost(t *testing.T) {
	tests := []struct {
		name            string
		gas             uint64
		gasCost         uint64
		expectedGasCost uint64
	}{
		{
			name:            "normal gasCost unchanged",
			gas:             10000,
			gasCost:         3,
			expectedGasCost: 3,
		},
		{
			name:            "gasCost equals gas unchanged",
			gas:             5058,
			gasCost:         5058,
			expectedGasCost: 5058,
		},
		{
			name:            "corrupted gasCost from Erigon underflow bug",
			gas:             5058,
			gasCost:         18158513697557845033, // 0xfc00000000001429
			expectedGasCost: 5058,
		},
		{
			name:            "another corrupted value",
			gas:             9974,
			gasCost:         18158513697557850263, // From test case
			expectedGasCost: 9974,
		},
		{
			name:            "max uint64 corrupted",
			gas:             1000,
			gasCost:         ^uint64(0), // max uint64
			expectedGasCost: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &StructLog{
				Gas:     tt.gas,
				GasCost: tt.gasCost,
			}

			SanitizeGasCost(log)

			assert.Equal(t, tt.expectedGasCost, log.GasCost)
		})
	}
}

func TestSanitizeStructLogs(t *testing.T) {
	logs := []StructLog{
		{Op: "PUSH1", Gas: 10000, GasCost: 3},
		{Op: "CALL", Gas: 5058, GasCost: 18158513697557845033}, // Corrupted
		{Op: "STOP", Gas: 100, GasCost: 0},
	}

	SanitizeStructLogs(logs)

	assert.Equal(t, uint64(3), logs[0].GasCost, "normal gasCost should be unchanged")
	assert.Equal(t, uint64(5058), logs[1].GasCost, "corrupted gasCost should be sanitized")
	assert.Equal(t, uint64(0), logs[2].GasCost, "zero gasCost should be unchanged")
}
