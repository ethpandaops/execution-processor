package cryo

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// Default buffer and execution configuration values.
const (
	DefaultBufferMaxRows       = 100000
	DefaultBufferFlushInterval = 3 * time.Second
	DefaultBinaryPath          = "cryo"
	DefaultFetchTimeout        = 2 * time.Minute

	// DefaultMaxPendingBlockRange is deliberately far above the shared
	// tracker.DefaultMaxPendingBlockRange. That default sizes the transaction
	// processors, which fan out one task per transaction, so a handful of
	// blocks already saturates the fleet. Cryo emits one task per block, so the
	// pending range is the entire fan-out: at the shared default only two tasks
	// would ever be in flight and every other worker would idle.
	DefaultMaxPendingBlockRange = 50
)

// defaultGlobalArgs are passed to every cryo invocation.
//
// --hex emits 0x-prefixed lowercase strings for every byte column, and
// --u256-types string emits U256 values only as decimal strings. Both are safe
// for all datasets and remove the encoding work the target schema would
// otherwise need done after decoding.
//
// Every dataset mapping assumes this encoding, so these are applied before any
// operator arguments rather than being replaceable by them. A replacement would
// change how cryo encodes values without changing any table, so the startup
// schema check would still pass and every mapping would misread the output.
var defaultGlobalArgs = []string{"--hex", "--u256-types", "string"}

// groupNamePattern constrains a group name to what is safe in a Redis key, four
// asynq queue names, a Prometheus label and a ClickHouse column value.
var groupNamePattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// reservedArgs are the arguments the fetcher sets itself. clap resolves
// repeated arguments last-wins, so an operator repeating one of these would
// silently take over the invocation: --output-dir would write the parquet
// outside the scratch directory, where the fetch cannot find it and the sweep
// cannot remove it, and --blocks would decouple the output from the block the
// task believes it collected.
var reservedArgs = map[string]struct{}{
	"--blocks":     {},
	"-b":           {},
	"--output-dir": {},
	"-o":           {},
	"--no-report":  {},
	"--rpc":        {},
	"-r":           {},
}

// Config holds configuration for the cryo processors.
//
// Unlike the other processors this is one config to many processors: each
// enabled group becomes its own processor with its own checkpoint, because a
// group is one cryo invocation and therefore one unit of success or failure.
type Config struct {
	clickhouse.Config `yaml:",inline"`

	Enabled bool `yaml:"enabled"`

	// BinaryPath is the cryo executable, resolved on PATH when not absolute.
	BinaryPath string `yaml:"binaryPath"`

	// TempDir is the parent directory for per-task scratch space. Empty uses
	// the operating system default.
	TempDir string `yaml:"tempDir"`

	// GlobalArgs are appended to the default cryo arguments applied to every
	// group. Validate resolves this to the effective list, defaults first.
	GlobalArgs []string `yaml:"globalArgs"`

	// FetchTimeout bounds a single cryo invocation.
	FetchTimeout time.Duration `yaml:"fetchTimeout"`

	// BufferMaxRows and BufferFlushInterval apply per dataset, not per group,
	// because one invocation writes to several tables with volumes an order of
	// magnitude apart.
	BufferMaxRows       int           `yaml:"bufferMaxRows"`
	BufferFlushInterval time.Duration `yaml:"bufferFlushInterval"`

	// MaxPendingBlockRange bounds how far ahead of the oldest incomplete block
	// the processor may enqueue. It defaults to this package's
	// DefaultMaxPendingBlockRange rather than the shared tracker default,
	// because one cryo task is a whole block instead of a single transaction,
	// so this value is the fan-out rather than a multiplier on it.
	MaxPendingBlockRange int `yaml:"maxPendingBlockRange"`

	Groups []GroupConfig `yaml:"groups"`
}

// GroupConfig describes one cryo invocation and the datasets it writes.
type GroupConfig struct {
	// Name is the checkpoint key in admin.execution_block, via the processor
	// column, and is therefore permanent: renaming a group orphans its
	// progress, and moving a datatype between groups is a re-seed.
	Name string `yaml:"name"`

	Enabled bool `yaml:"enabled"`

	// Datatypes are the cryo arguments for this invocation. They are not
	// always dataset names: state_diffs is one argument yielding three.
	Datatypes []string `yaml:"datatypes"`

	// ExtraArgs are appended to the invocation after the global arguments.
	ExtraArgs []string `yaml:"extraArgs"`

	// TablePrefix is prepended to each dataset's table name.
	TablePrefix string `yaml:"tablePrefix"`

	// MinBlock overrides the lowest block this group will collect. The
	// effective floor is the highest of this and its datasets' own floors.
	MinBlock uint64 `yaml:"minBlock"`

	// MaxPendingBlockRange overrides the processor-wide setting.
	MaxPendingBlockRange int `yaml:"maxPendingBlockRange"`
}

// Validate checks the configuration and applies defaults.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if err := c.Config.Validate(); err != nil {
		return fmt.Errorf("clickhouse config validation failed: %w", err)
	}

	if c.BinaryPath == "" {
		c.BinaryPath = DefaultBinaryPath
	}

	if err := checkReservedArgs("globalArgs", c.GlobalArgs); err != nil {
		return err
	}

	// Additive, not a replacement: the defaults describe the encoding every
	// mapping reads, and a fresh slice keeps defaultGlobalArgs immutable.
	globalArgs := make([]string, 0, len(defaultGlobalArgs)+len(c.GlobalArgs))
	globalArgs = append(globalArgs, defaultGlobalArgs...)
	globalArgs = append(globalArgs, c.GlobalArgs...)
	c.GlobalArgs = globalArgs

	if c.FetchTimeout <= 0 {
		c.FetchTimeout = DefaultFetchTimeout
	}

	// Replicas can share a temp filesystem, and the startup sweep deletes any
	// scratch directory older than sweepMinAge. A fetch allowed to run that long
	// would have its working directory removed underneath it by a booting
	// replica.
	if c.FetchTimeout >= sweepMinAge {
		return fmt.Errorf(
			"fetchTimeout %s must be below the orphan sweep age %s, or a restarting replica will delete an in-flight fetch's scratch directory",
			c.FetchTimeout, sweepMinAge,
		)
	}

	if c.MaxPendingBlockRange <= 0 {
		c.MaxPendingBlockRange = DefaultMaxPendingBlockRange
	}

	if c.BufferMaxRows <= 0 {
		c.BufferMaxRows = DefaultBufferMaxRows
	}

	if c.BufferFlushInterval <= 0 {
		c.BufferFlushInterval = DefaultBufferFlushInterval
	}

	enabled := 0
	seen := make(map[string]struct{}, len(c.Groups))
	claimed := make(map[string]string, len(datasets))

	for i := range c.Groups {
		group := &c.Groups[i]

		if err := group.validate(); err != nil {
			return fmt.Errorf("group %d: %w", i, err)
		}

		if _, dup := seen[group.Name]; dup {
			return fmt.Errorf("duplicate cryo group name %q", group.Name)
		}

		seen[group.Name] = struct{}{}

		if !group.Enabled {
			continue
		}

		enabled++

		if group.MaxPendingBlockRange <= 0 {
			group.MaxPendingBlockRange = c.MaxPendingBlockRange
		}

		members, err := datasetsFor(group.Datatypes)
		if err != nil {
			return fmt.Errorf("group %q: %w", group.Name, err)
		}

		// Two groups writing the same table would race on the same rows while
		// tracking progress under separate checkpoints.
		for _, ds := range members {
			if owner, taken := claimed[ds.Name]; taken {
				return fmt.Errorf("dataset %q is produced by both group %q and group %q", ds.Name, owner, group.Name)
			}

			claimed[ds.Name] = group.Name
		}
	}

	if enabled == 0 {
		return fmt.Errorf("cryo is enabled but no group is")
	}

	return nil
}

func (g *GroupConfig) validate() error {
	if g.Name == "" {
		return fmt.Errorf("name is required")
	}

	if !groupNamePattern.MatchString(g.Name) {
		return fmt.Errorf(
			"name %q must match %s: it becomes the permanent checkpoint key in admin.execution_block, a Redis key component, four queue names and a metrics label",
			g.Name, groupNamePattern,
		)
	}

	if !g.Enabled {
		return nil
	}

	if len(g.Datatypes) == 0 {
		return fmt.Errorf("group %q must list at least one datatype", g.Name)
	}

	return checkReservedArgs(fmt.Sprintf("group %q extraArgs", g.Name), g.ExtraArgs)
}

// checkReservedArgs rejects operator arguments that would override what the
// fetcher sets itself. Both the bare flag and its --flag=value form are
// matched, in long and short form.
func checkReservedArgs(field string, args []string) error {
	for _, arg := range args {
		flag, _, _ := strings.Cut(arg, "=")

		if _, reserved := reservedArgs[flag]; reserved {
			return fmt.Errorf("%s must not set %s: the fetcher sets it and cryo takes the last value", field, flag)
		}
	}

	return nil
}
