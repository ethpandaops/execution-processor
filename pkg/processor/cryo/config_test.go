package cryo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testConfig is the smallest configuration that validates: one enabled group
// collecting one datatype.
func testConfig() *Config {
	cfg := &Config{
		Enabled: true,
		Groups:  []GroupConfig{{Name: "t", Enabled: true, Datatypes: []string{"blocks"}}},
	}
	cfg.Addr = "localhost:9000"

	return cfg
}

func TestValidateAppliesDefaults(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	require.NoError(t, cfg.Validate())

	require.Equal(t, DefaultBinaryPath, cfg.BinaryPath)
	require.Equal(t, DefaultFetchTimeout, cfg.FetchTimeout)
	require.Equal(t, DefaultBufferMaxRows, cfg.BufferMaxRows)
	require.Equal(t, DefaultBufferFlushInterval, cfg.BufferFlushInterval)
}

// TestValidateDefaultsMaxPendingBlockRange covers the setting that decides how
// many blocks the fleet may work on at once: one cryo task is a whole block, so
// the shared tracker default would leave nearly every worker idle.
func TestValidateDefaultsMaxPendingBlockRange(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	require.NoError(t, cfg.Validate())

	require.Equal(t, DefaultMaxPendingBlockRange, cfg.MaxPendingBlockRange)
	require.Equal(t, DefaultMaxPendingBlockRange, cfg.Groups[0].MaxPendingBlockRange,
		"a group without an override inherits the processor-wide value")
	require.Greater(t, DefaultMaxPendingBlockRange, 2, "the shared default is sized for per-transaction fan-out")
}

func TestValidateKeepsMaxPendingBlockRangeOverrides(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.MaxPendingBlockRange = 20
	cfg.Groups = append(cfg.Groups, GroupConfig{
		Name: "u", Enabled: true, Datatypes: []string{"logs"}, MaxPendingBlockRange: 5,
	})
	require.NoError(t, cfg.Validate())

	require.Equal(t, 20, cfg.MaxPendingBlockRange)
	require.Equal(t, 20, cfg.Groups[0].MaxPendingBlockRange)
	require.Equal(t, 5, cfg.Groups[1].MaxPendingBlockRange)
}

// TestValidateGlobalArgsAreAdditive is the one that matters for correctness of
// the data: dropping --hex or --u256-types leaves the tables unchanged, so the
// startup schema check still passes while every mapping misreads the output.
func TestValidateGlobalArgsAreAdditive(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.GlobalArgs = []string{"--verbose"}
	require.NoError(t, cfg.Validate())

	require.Equal(t, []string{"--hex", "--u256-types", "string", "--verbose"}, cfg.GlobalArgs)
}

func TestValidateGlobalArgsDefaultWhenUnset(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	require.NoError(t, cfg.Validate())

	require.Equal(t, defaultGlobalArgs, cfg.GlobalArgs)
}

// TestValidateDoesNotMutateDefaultGlobalArgs guards the package-level slice: a
// merge that appended in place would leak one operator's arguments into the
// defaults every later caller sees.
func TestValidateDoesNotMutateDefaultGlobalArgs(t *testing.T) {
	t.Parallel()

	before := append([]string(nil), defaultGlobalArgs...)

	cfg := testConfig()
	cfg.GlobalArgs = []string{"--verbose"}
	require.NoError(t, cfg.Validate())

	cfg.GlobalArgs[0] = "--clobbered"

	require.Equal(t, before, defaultGlobalArgs)
}

func TestValidateRejectsReservedArgs(t *testing.T) {
	t.Parallel()

	for _, arg := range []string{
		"--blocks", "-b", "--output-dir", "-o", "--no-report", "--rpc", "-r",
		"--blocks=1:2", "--output-dir=/tmp/elsewhere", "--rpc=http://other",
	} {
		t.Run("globalArgs "+arg, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.GlobalArgs = []string{arg}

			err := cfg.Validate()

			require.Error(t, err)
			require.Contains(t, err.Error(), "globalArgs")
		})

		t.Run("extraArgs "+arg, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.Groups[0].ExtraArgs = []string{arg}

			err := cfg.Validate()

			require.Error(t, err)
			require.Contains(t, err.Error(), "extraArgs")
		})
	}
}

func TestValidateAllowsUnreservedArgs(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.GlobalArgs = []string{"--verbose"}
	cfg.Groups[0].ExtraArgs = []string{"--include-columns", "gas_limit", "--requests-per-second=50"}

	require.NoError(t, cfg.Validate())
}

func TestValidateGroupNames(t *testing.T) {
	t.Parallel()

	valid := []string{"blocks", "state_diffs", "erc20", "a1_b2"}
	for _, name := range valid {
		t.Run("valid "+name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.Groups[0].Name = name

			require.NoError(t, cfg.Validate())
		})
	}

	invalid := []string{"State Diffs", "state:diffs", "state-diffs", "StateDiffs", "stäte", "", "state.diffs", "state/diffs"}
	for _, name := range invalid {
		t.Run("invalid "+name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.Groups[0].Name = name

			require.Error(t, cfg.Validate(), "a group name becomes a queue name and a permanent checkpoint key")
		})
	}
}

// TestValidateRejectsFetchTimeoutAtSweepAge covers the cross-replica hazard: a
// fetch allowed to outlive the orphan sweep age would have its scratch
// directory deleted by a booting replica sharing the temp filesystem.
func TestValidateRejectsFetchTimeoutAtSweepAge(t *testing.T) {
	t.Parallel()

	for _, timeout := range []time.Duration{sweepMinAge, sweepMinAge + time.Second, 3 * sweepMinAge} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()

			cfg := testConfig()
			cfg.FetchTimeout = timeout

			err := cfg.Validate()

			require.Error(t, err)
			require.Contains(t, err.Error(), timeout.String())
			require.Contains(t, err.Error(), sweepMinAge.String())
		})
	}
}

func TestValidateAllowsFetchTimeoutBelowSweepAge(t *testing.T) {
	t.Parallel()

	cfg := testConfig()
	cfg.FetchTimeout = sweepMinAge - time.Second

	require.NoError(t, cfg.Validate())
}

func TestValidateSkipsDisabledConfig(t *testing.T) {
	t.Parallel()

	cfg := &Config{Groups: []GroupConfig{{Name: "not valid at all"}}}

	require.NoError(t, cfg.Validate(), "a disabled processor is not configured at all")
}
