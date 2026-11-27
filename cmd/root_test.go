package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadServerConfigFromFileSubstitutesEnvVars(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	content := "logging: debug\nredis:\n  address: \"redis://:${EP_TEST_REDIS_PASSWORD}@localhost:6379\"\n  prefix: ${EP_TEST_REDIS_PREFIX:-execution-processor}\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	t.Run("set", func(t *testing.T) {
		t.Setenv("EP_TEST_REDIS_PASSWORD", "p@ss")

		cfg, err := loadServerConfigFromFile(path)
		require.NoError(t, err)
		assert.Equal(t, "debug", cfg.LoggingLevel)
		assert.Equal(t, "redis://:p@ss@localhost:6379", cfg.Redis.Address)
		assert.Equal(t, "execution-processor", cfg.Redis.Prefix)
		assert.Equal(t, ":9090", cfg.MetricsAddr)
	})

	t.Run("unset", func(t *testing.T) {
		_, err := loadServerConfigFromFile(path)
		require.ErrorContains(t, err, "EP_TEST_REDIS_PASSWORD")
	})
}
