package cryo

import (
	"fmt"
	"sort"
)

// Dataset describes one cryo datatype and how its rows land in ClickHouse.
type Dataset struct {
	// Name is the cryo dataset name, which is also the parquet file's infix.
	Name string
	// Table is the target ClickHouse table.
	Table string
	// InternalIndex marks datasets whose target table carries internal_index.
	InternalIndex bool
	// MinBlock is the lowest block the dataset can be collected for. Trace
	// based datasets cannot trace the genesis block.
	MinBlock uint64
	// RequiredColumns are parquet columns cryo only emits when explicitly
	// asked, which the group must therefore request.
	RequiredColumns []string
	// newSink builds the per-dataset row buffer and column mapping.
	newSink func(sinkDeps) sink
}

// Datatype is a cryo command-line argument, which is not always a dataset: the
// state diff datatypes can only be collected through the state_diffs
// meta-datatype, which yields three datasets from one collection.
type Datatype struct {
	// Name is the argument passed to cryo.
	Name string
	// Datasets are the dataset names cryo writes for this datatype.
	Datasets []string
}

// datatypes lists every cryo datatype the processor knows how to collect.
//
// Naming two members of the state diff family in one invocation fails inside
// cryo with "Collect failed: schema not provided", so the meta-datatype is the
// only way to collect them alongside anything else.
var datatypes = []Datatype{
	{Name: "blocks", Datasets: []string{"blocks"}},
	{Name: "transactions", Datasets: []string{"transactions"}},
	{Name: "logs", Datasets: []string{"logs"}},
	{Name: "erc20_transfers", Datasets: []string{"erc20_transfers"}},
	{Name: "erc721_transfers", Datasets: []string{"erc721_transfers"}},
	{Name: "traces", Datasets: []string{"traces"}},
	{Name: "native_transfers", Datasets: []string{"native_transfers"}},
	{Name: "contracts", Datasets: []string{"contracts"}},
	{Name: "address_appearances", Datasets: []string{"address_appearances"}},
	{Name: "balance_reads", Datasets: []string{"balance_reads"}},
	{Name: "nonce_reads", Datasets: []string{"nonce_reads"}},
	{Name: "storage_reads", Datasets: []string{"storage_reads"}},
	{Name: "four_byte_counts", Datasets: []string{"four_byte_counts"}},
	{Name: "state_diffs", Datasets: []string{"balance_diffs", "nonce_diffs", "storage_diffs"}},
}

// datatypeByName indexes datatypes for config validation.
var datatypeByName = func() map[string]Datatype {
	byName := make(map[string]Datatype, len(datatypes))
	for _, dt := range datatypes {
		byName[dt.Name] = dt
	}

	return byName
}()

// datasetByName indexes every dataset the processor can write.
var datasetByName = func() map[string]*Dataset {
	byName := make(map[string]*Dataset, len(datasets))
	for _, ds := range datasets {
		byName[ds.Name] = ds
	}

	return byName
}()

// DatatypeNames returns every collectable cryo datatype, sorted.
func DatatypeNames() []string {
	names := make([]string, 0, len(datatypes))
	for _, dt := range datatypes {
		names = append(names, dt.Name)
	}

	sort.Strings(names)

	return names
}

// datasetsFor expands datatype names into the datasets they produce, in the
// order the datatypes were listed.
func datasetsFor(datatypeNames []string) ([]*Dataset, error) {
	out := make([]*Dataset, 0, len(datatypeNames))
	seen := make(map[string]struct{}, len(datatypeNames))

	for _, name := range datatypeNames {
		dt, ok := datatypeByName[name]
		if !ok {
			return nil, fmt.Errorf("unknown cryo datatype %q, expected one of %v", name, DatatypeNames())
		}

		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("cryo datatype %q listed more than once", name)
		}

		seen[name] = struct{}{}

		for _, dsName := range dt.Datasets {
			ds, known := datasetByName[dsName]
			if !known {
				return nil, fmt.Errorf("datatype %q yields unregistered dataset %q", name, dsName)
			}

			out = append(out, ds)
		}
	}

	return out, nil
}
