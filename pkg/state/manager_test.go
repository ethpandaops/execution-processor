package state

import (
	"context"
	"math/big"
	"testing"

	"github.com/ClickHouse/ch-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClickHouseClient is a mock implementation of clickhouse.ClientInterface.
type MockClickHouseClient struct {
	mock.Mock
}

func (m *MockClickHouseClient) QueryUInt64(ctx context.Context, query string, columnName string) (*uint64, error) {
	args := m.Called(ctx, query, columnName)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, _ := args.Get(0).(*uint64)

	return val, args.Error(1)
}

func (m *MockClickHouseClient) QueryMinMaxUInt64(ctx context.Context, query string) (*uint64, *uint64, error) {
	args := m.Called(ctx, query)

	var minResult, maxResult *uint64

	if args.Get(0) != nil {
		minResult = args.Get(0).(*uint64) //nolint:errcheck // test mock
	}

	if args.Get(1) != nil {
		maxResult = args.Get(1).(*uint64) //nolint:errcheck // test mock
	}

	return minResult, maxResult, args.Error(2)
}

func (m *MockClickHouseClient) Execute(ctx context.Context, query string) error {
	args := m.Called(ctx, query)

	return args.Error(0)
}

func (m *MockClickHouseClient) Start() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockClickHouseClient) Stop() error {
	args := m.Called()

	return args.Error(0)
}

func (m *MockClickHouseClient) IsStorageEmpty(ctx context.Context, table string, conditions map[string]interface{}) (bool, error) {
	args := m.Called(ctx, table, conditions)

	return args.Bool(0), args.Error(1)
}

func (m *MockClickHouseClient) SetNetwork(network string) {
	m.Called(network)
}

func (m *MockClickHouseClient) Do(ctx context.Context, query ch.Query) error {
	args := m.Called(ctx, query)

	return args.Error(0)
}

// Helper to create uint64 pointer.
func uint64Ptr(v uint64) *uint64 {
	return &v
}

func TestNextBlock_NoResultsVsBlock0(t *testing.T) {
	ctx := context.Background()
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name              string
		setupMock         func(*MockClickHouseClient)
		expectedNextBlock *big.Int
		expectError       bool
	}{
		{
			name: "No results - should return chain head",
			setupMock: func(m *MockClickHouseClient) {
				// QueryUInt64 returns nil (no rows found)
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(nil, nil)
				// IsStorageEmpty is called when result is nil
				m.On("IsStorageEmpty", ctx, "test_table", map[string]interface{}{
					"processor":         "test_processor",
					"meta_network_name": "mainnet",
				}).Return(true, nil)
			},
			expectedNextBlock: big.NewInt(1000), // Chain head
			expectError:       false,
		},
		{
			name: "Block 0 found - should return 1",
			setupMock: func(m *MockClickHouseClient) {
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(uint64Ptr(0), nil)
			},
			expectedNextBlock: big.NewInt(1), // Next block after 0
			expectError:       false,
		},
		{
			name: "Block 100 found - should return 101",
			setupMock: func(m *MockClickHouseClient) {
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(uint64Ptr(100), nil)
			},
			expectedNextBlock: big.NewInt(101), // Next block after 100
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockClickHouseClient)
			tt.setupMock(mockClient)

			manager := &Manager{
				log:           log.WithField("test", tt.name),
				storageClient: mockClient,
				storageTable:  "test_table",
			}

			chainHead := big.NewInt(1000)
			nextBlock, err := manager.NextBlock(ctx, "test_processor", "mainnet", "forwards", chainHead)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedNextBlock, nextBlock)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestNextBlock_EmptyStorageWithLimiter(t *testing.T) {
	ctx := context.Background()
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name           string
		chainHead      *big.Int
		limiterMax     *uint64
		limiterEnabled bool
		expectedNext   *big.Int
	}{
		{
			name:           "empty storage with limiter and high chain head",
			chainHead:      big.NewInt(5000),
			limiterMax:     uint64Ptr(1000),
			limiterEnabled: true,
			expectedNext:   big.NewInt(999), // limiterMax - 1
		},
		{
			name:           "empty storage with limiter and nil chain head",
			chainHead:      nil,
			limiterMax:     uint64Ptr(1000),
			limiterEnabled: true,
			expectedNext:   big.NewInt(999), // limiterMax - 1
		},
		{
			name:           "empty storage with limiter and zero chain head",
			chainHead:      big.NewInt(0),
			limiterMax:     uint64Ptr(1000),
			limiterEnabled: true,
			expectedNext:   big.NewInt(999), // limiterMax - 1
		},
		{
			name:           "empty storage without limiter",
			chainHead:      big.NewInt(5000),
			limiterMax:     nil,
			limiterEnabled: false,
			expectedNext:   big.NewInt(5000), // start from chain head when available
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock storage that reports empty
			mockStorage := new(MockClickHouseClient)
			mockStorage.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(nil, nil)
			mockStorage.On("IsStorageEmpty", ctx, "test_table", map[string]interface{}{
				"processor":         "test-processor",
				"meta_network_name": "mainnet",
			}).Return(true, nil)

			// Create mock limiter
			var mockLimiter *MockClickHouseClient
			if tt.limiterEnabled {
				mockLimiter = new(MockClickHouseClient)
				mockLimiter.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(tt.limiterMax, nil)
			}

			// Create manager
			manager := &Manager{
				log:            log.WithField("test", tt.name),
				storageClient:  mockStorage,
				storageTable:   "test_table",
				limiterEnabled: tt.limiterEnabled,
				limiterClient:  mockLimiter,
				limiterTable:   "limiter_table",
			}

			// Test NextBlock
			next, err := manager.NextBlock(ctx, "test-processor", "mainnet", "forwards", tt.chainHead)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedNext, next)

			mockStorage.AssertExpectations(t)

			if mockLimiter != nil {
				mockLimiter.AssertExpectations(t)
			}
		})
	}
}

func TestGetProgressiveNextBlock_EmptyStorage(t *testing.T) {
	ctx := context.Background()
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name            string
		storageEmpty    bool
		storedBlock     uint64
		chainHead       *big.Int
		expectedNext    *big.Int
		expectedIsEmpty bool
	}{
		{
			name:            "empty storage with chain head",
			storageEmpty:    true,
			storedBlock:     0,
			chainHead:       big.NewInt(5000),
			expectedNext:    big.NewInt(5000),
			expectedIsEmpty: true,
		},
		{
			name:            "empty storage without chain head",
			storageEmpty:    true,
			storedBlock:     0,
			chainHead:       nil,
			expectedNext:    big.NewInt(0),
			expectedIsEmpty: true,
		},
		{
			name:            "block 0 actually processed",
			storageEmpty:    false,
			storedBlock:     0,
			chainHead:       big.NewInt(5000),
			expectedNext:    big.NewInt(1), // next after block 0
			expectedIsEmpty: false,
		},
		{
			name:            "normal case with blocks",
			storageEmpty:    false,
			storedBlock:     1000,
			chainHead:       big.NewInt(5000),
			expectedNext:    big.NewInt(1001),
			expectedIsEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockStorage := new(MockClickHouseClient)

			if tt.storageEmpty {
				// Empty storage case
				mockStorage.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(nil, nil)
				mockStorage.On("IsStorageEmpty", ctx, "test_table", map[string]interface{}{
					"processor":         "test-processor",
					"meta_network_name": "mainnet",
				}).Return(true, nil)
			} else {
				// Non-empty storage case
				mockStorage.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(uint64Ptr(tt.storedBlock), nil)
			}

			manager := &Manager{
				log:           log.WithField("test", tt.name),
				storageClient: mockStorage,
				storageTable:  "test_table",
			}

			next, isEmpty, err := manager.getProgressiveNextBlock(ctx, "test-processor", "mainnet", tt.chainHead)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedNext, next)
			assert.Equal(t, tt.expectedIsEmpty, isEmpty)

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestGetProgressiveNextBlock_NoResultsVsBlock0(t *testing.T) {
	ctx := context.Background()
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	tests := []struct {
		name              string
		setupMock         func(*MockClickHouseClient)
		chainHead         *big.Int
		expectedNextBlock *big.Int
		expectError       bool
	}{
		{
			name: "No results with chain head - should return chain head",
			setupMock: func(m *MockClickHouseClient) {
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(nil, nil)
				// IsStorageEmpty is called when result is nil
				m.On("IsStorageEmpty", ctx, "test_table", map[string]interface{}{
					"processor":         "test_processor",
					"meta_network_name": "mainnet",
				}).Return(true, nil)
			},
			chainHead:         big.NewInt(1000),
			expectedNextBlock: big.NewInt(1000),
			expectError:       false,
		},
		{
			name: "No results without chain head - should return 0",
			setupMock: func(m *MockClickHouseClient) {
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(nil, nil)
				// IsStorageEmpty is called when result is nil
				m.On("IsStorageEmpty", ctx, "test_table", map[string]interface{}{
					"processor":         "test_processor",
					"meta_network_name": "mainnet",
				}).Return(true, nil)
			},
			chainHead:         nil,
			expectedNextBlock: big.NewInt(0),
			expectError:       false,
		},
		{
			name: "Block 0 found - should return 1",
			setupMock: func(m *MockClickHouseClient) {
				m.On("QueryUInt64", ctx, mock.AnythingOfType("string"), "block_number").Return(uint64Ptr(0), nil)
			},
			chainHead:         big.NewInt(1000),
			expectedNextBlock: big.NewInt(1),
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockClickHouseClient)
			tt.setupMock(mockClient)

			manager := &Manager{
				log:           log.WithField("test", tt.name),
				storageClient: mockClient,
				storageTable:  "test_table",
			}

			nextBlock, _, err := manager.getProgressiveNextBlock(ctx, "test_processor", "mainnet", tt.chainHead)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedNextBlock, nextBlock)
			}

			mockClient.AssertExpectations(t)
		})
	}
}
