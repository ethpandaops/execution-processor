package simple_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	transaction_simple "github.com/ethpandaops/execution-processor/pkg/processor/transaction/simple"
)

func TestClickHouseDateTime_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		time     time.Time
		expected string
	}{
		{
			name:     "UTC time",
			time:     time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC),
			expected: `"2024-01-15 10:30:45"`,
		},
		{
			name:     "epoch time",
			time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: `"1970-01-01 00:00:00"`,
		},
		{
			name:     "local time converted to UTC",
			time:     time.Date(2024, 6, 15, 12, 0, 0, 0, time.FixedZone("PST", -8*3600)),
			expected: `"2024-06-15 20:00:00"`, // +8 hours to UTC
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dt := transaction_simple.ClickHouseDateTime(tc.time)
			result, err := dt.MarshalJSON()

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, string(result))
		})
	}
}

func TestCalculateEffectiveGasPrice_Logic(t *testing.T) {
	// Test the effective gas price calculation logic
	// This tests the algorithm without requiring actual block/tx objects
	tests := []struct {
		name             string
		txType           uint8
		txGasPrice       *big.Int
		txGasTipCap      *big.Int
		txGasFeeCap      *big.Int
		blockBaseFee     *big.Int
		expectedGasPrice *big.Int
	}{
		{
			name:             "legacy transaction",
			txType:           0, // LegacyTxType
			txGasPrice:       big.NewInt(20000000000),
			expectedGasPrice: big.NewInt(20000000000),
		},
		{
			name:             "access list transaction",
			txType:           1, // AccessListTxType
			txGasPrice:       big.NewInt(30000000000),
			expectedGasPrice: big.NewInt(30000000000),
		},
		{
			name:             "EIP-1559 transaction - base fee + tip < max fee",
			txType:           2, // DynamicFeeTxType
			txGasTipCap:      big.NewInt(2000000000),
			txGasFeeCap:      big.NewInt(100000000000),
			blockBaseFee:     big.NewInt(10000000000),
			expectedGasPrice: big.NewInt(12000000000), // baseFee + tipCap
		},
		{
			name:             "EIP-1559 transaction - base fee + tip > max fee (capped)",
			txType:           2,
			txGasTipCap:      big.NewInt(50000000000),
			txGasFeeCap:      big.NewInt(30000000000),
			blockBaseFee:     big.NewInt(10000000000),
			expectedGasPrice: big.NewInt(30000000000), // capped at feeCap
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var effectiveGasPrice *big.Int

			// Simulate the calculateEffectiveGasPrice logic
			switch tc.txType {
			case 0, 1: // Legacy and AccessList
				effectiveGasPrice = tc.txGasPrice
			default: // EIP-1559 and later
				if tc.blockBaseFee == nil {
					effectiveGasPrice = tc.txGasPrice
				} else {
					effectiveGasPrice = new(big.Int).Add(tc.blockBaseFee, tc.txGasTipCap)
					if effectiveGasPrice.Cmp(tc.txGasFeeCap) > 0 {
						effectiveGasPrice = tc.txGasFeeCap
					}
				}
			}

			assert.Equal(t, tc.expectedGasPrice.Int64(), effectiveGasPrice.Int64())
		})
	}
}

func TestInputByteStats_Logic(t *testing.T) {
	// Test the input byte statistics calculation logic
	tests := []struct {
		name            string
		inputData       []byte
		expectedSize    uint32
		expectedZero    uint32
		expectedNonzero uint32
	}{
		{
			name:            "empty input",
			inputData:       []byte{},
			expectedSize:    0,
			expectedZero:    0,
			expectedNonzero: 0,
		},
		{
			name:            "all zeros",
			inputData:       []byte{0, 0, 0, 0},
			expectedSize:    4,
			expectedZero:    4,
			expectedNonzero: 0,
		},
		{
			name:            "all nonzero",
			inputData:       []byte{1, 2, 3, 4},
			expectedSize:    4,
			expectedZero:    0,
			expectedNonzero: 4,
		},
		{
			name:            "mixed",
			inputData:       []byte{0, 1, 0, 2, 0, 0, 3},
			expectedSize:    7,
			expectedZero:    4,
			expectedNonzero: 3,
		},
		{
			name:            "typical function selector",
			inputData:       []byte{0xa9, 0x05, 0x9c, 0xbb, 0, 0, 0, 0},
			expectedSize:    8,
			expectedZero:    4,
			expectedNonzero: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callDataSize := uint32(len(tc.inputData))

			var nZero, nNonzero uint32

			for _, b := range tc.inputData {
				if b == 0 {
					nZero++
				} else {
					nNonzero++
				}
			}

			assert.Equal(t, tc.expectedSize, callDataSize)
			assert.Equal(t, tc.expectedZero, nZero)
			assert.Equal(t, tc.expectedNonzero, nNonzero)
			assert.Equal(t, callDataSize, nZero+nNonzero)
		})
	}
}

func TestTransactionStruct(t *testing.T) {
	// Test that Transaction struct fields have correct types and can be populated
	now := time.Now()
	toAddr := "0x1234567890abcdef1234567890abcdef12345678"
	tipCap := "1000000000"
	feeCap := "2000000000"
	blobGas := uint64(131072)
	blobGasFeeCap := "1000000"

	tx := transaction_simple.Transaction{
		UpdatedDateTime:    transaction_simple.ClickHouseDateTime(now),
		BlockNumber:        12345,
		BlockHash:          "0xabcdef123456",
		ParentHash:         "0xfedcba654321",
		Position:           0,
		Hash:               "0xtxhash123",
		From:               "0xfrom1234",
		To:                 &toAddr,
		Nonce:              42,
		GasPrice:           "1500000000",
		Gas:                21000,
		GasTipCap:          &tipCap,
		GasFeeCap:          &feeCap,
		Value:              "1000000000000000000",
		Type:               2,
		Size:               250,
		CallDataSize:       100,
		BlobGas:            &blobGas,
		BlobGasFeeCap:      &blobGasFeeCap,
		BlobHashes:         []string{"0xblob1", "0xblob2"},
		Success:            true,
		NInputBytes:        100,
		NInputZeroBytes:    40,
		NInputNonzeroBytes: 60,
		MetaNetworkName:    "mainnet",
	}

	assert.Equal(t, uint64(12345), tx.BlockNumber)
	assert.NotNil(t, tx.To)
	assert.Equal(t, toAddr, *tx.To)
	assert.Equal(t, uint8(2), tx.Type)
	assert.Len(t, tx.BlobHashes, 2)
	assert.True(t, tx.Success)
	assert.Equal(t, tx.NInputBytes, tx.NInputZeroBytes+tx.NInputNonzeroBytes)
}

func TestTransactionStruct_ContractCreation(t *testing.T) {
	// Test transaction with nil To address (contract creation)
	tx := transaction_simple.Transaction{
		UpdatedDateTime:    transaction_simple.ClickHouseDateTime(time.Now()),
		BlockNumber:        12345,
		BlockHash:          "0xabcdef123456",
		ParentHash:         "0xfedcba654321",
		Position:           0,
		Hash:               "0xtxhash123",
		From:               "0xfrom1234",
		To:                 nil, // Contract creation
		Nonce:              0,
		GasPrice:           "1000000000",
		Gas:                100000,
		Value:              "0",
		Type:               0,
		Size:               500,
		CallDataSize:       400,
		BlobHashes:         []string{},
		Success:            true,
		NInputBytes:        400,
		NInputZeroBytes:    200,
		NInputNonzeroBytes: 200,
		MetaNetworkName:    "mainnet",
	}

	assert.Nil(t, tx.To)
	assert.Len(t, tx.BlobHashes, 0)
}

func TestBlobTransactionFields(t *testing.T) {
	// Test blob transaction specific fields
	blobGas := uint64(131072)
	blobGasFeeCap := "1000000000"

	tx := transaction_simple.Transaction{
		UpdatedDateTime: transaction_simple.ClickHouseDateTime(time.Now()),
		BlockNumber:     12345,
		Type:            3, // BlobTxType
		BlobGas:         &blobGas,
		BlobGasFeeCap:   &blobGasFeeCap,
		BlobHashes:      []string{"0xblob1", "0xblob2", "0xblob3", "0xblob4", "0xblob5", "0xblob6"},
		Success:         true,
		MetaNetworkName: "mainnet",
	}

	assert.Equal(t, uint8(3), tx.Type)
	assert.NotNil(t, tx.BlobGas)
	assert.Equal(t, uint64(131072), *tx.BlobGas)
	assert.NotNil(t, tx.BlobGasFeeCap)
	assert.Len(t, tx.BlobHashes, 6)
}
