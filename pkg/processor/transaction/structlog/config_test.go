package structlog

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfigKeepsTaskAndClickHouseRetriesApart(t *testing.T) {
	var cfg Config

	require.NoError(t, yaml.Unmarshal([]byte("max_retries: 5\nmaxTaskRetries: 7\n"), &cfg))
	require.Equal(t, 5, cfg.Config.MaxRetries)
	require.Equal(t, 7, cfg.MaxTaskRetries)
}
