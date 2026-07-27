package cryo

import (
	"fmt"
	"slices"
)

// Group is one cryo invocation: the datatypes collected together, and the
// datasets that come back. It is also the unit of progress tracking, because
// an invocation succeeds or fails as a whole and one completion flag should
// represent one thing that actually happened.
type Group struct {
	// Name is the suffix of the processor name, and therefore the checkpoint
	// key. It is permanent for the lifetime of the data.
	Name string
	// Datatypes are the arguments passed to cryo.
	Datatypes []string
	// Datasets are the parquet files that come back, one per entry.
	Datasets []*Dataset
	// ExtraArgs are appended to the invocation.
	ExtraArgs []string
	// TablePrefix is prepended to each dataset's table name.
	TablePrefix string
	// MinBlock is the highest floor among the group's datasets, since one
	// invocation cannot start at two different heights.
	MinBlock uint64
	// Canary is collected and checked but not written, when the group does not
	// already produce the dataset that serves as its coverage proof.
	Canary *Dataset
}

// newGroup resolves a group's configuration into its datasets.
//
// Every group collects the blocks dataset, because it is the only one whose row
// count is knowable in advance: exactly one per block, whatever the block
// contains. Every other dataset can legitimately be empty, and cryo reports a
// block it cannot serve by writing an empty parquet and exiting zero — which is
// indistinguishable from an empty block, and is how production came to hold
// whole ranges with no rows at all. A group that does not write blocks still
// collects it as a canary.
func newGroup(cfg *GroupConfig) (*Group, error) {
	members, err := datasetsFor(cfg.Datatypes)
	if err != nil {
		return nil, err
	}

	minBlock := cfg.MinBlock

	var canary *Dataset

	writesBlocks := false

	for _, ds := range members {
		if ds.MinBlock > minBlock {
			minBlock = ds.MinBlock
		}

		if ds.Name == blocksDataset.Name {
			writesBlocks = true
		}
	}

	datatypes := append([]string(nil), cfg.Datatypes...)

	if !writesBlocks {
		canary = blocksDataset
		datatypes = append(datatypes, blocksDataset.Name)
	}

	return &Group{
		Name:        cfg.Name,
		Datatypes:   datatypes,
		Datasets:    members,
		ExtraArgs:   append([]string(nil), cfg.ExtraArgs...),
		TablePrefix: cfg.TablePrefix,
		MinBlock:    minBlock,
		Canary:      canary,
	}, nil
}

// RequiredColumns returns the columns cryo must be asked for, gathered from
// the group's datasets. These are not operator choices: a mapping that reads a
// column cryo did not emit fails on every block, and ClickHouse would fill the
// gap with a type default rather than complain.
func (g *Group) RequiredColumns() []string {
	var cols []string

	for _, ds := range g.collected() {
		for _, col := range ds.RequiredColumns {
			if !slices.Contains(cols, col) {
				cols = append(cols, col)
			}
		}
	}

	return cols
}

// collected returns every dataset the invocation produces that is worth
// decoding, which is the written members plus any canary.
func (g *Group) collected() []*Dataset {
	if g.Canary == nil {
		return g.Datasets
	}

	return append(append(make([]*Dataset, 0, len(g.Datasets)+1), g.Datasets...), g.Canary)
}

// ProcessorName is the group's identity in queue names, metrics labels and the
// processor column of the block ledger.
func (g *Group) ProcessorName() string {
	return fmt.Sprintf("cryo_%s", g.Name)
}

// TableFor returns a dataset's target table under this group's prefix.
func (g *Group) TableFor(ds *Dataset) string {
	return g.TablePrefix + ds.Table
}
