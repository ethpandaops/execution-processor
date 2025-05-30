package main

import (
	"github.com/ethpandaops/execution-processor/cmd"

	_ "github.com/lib/pq"
)

func main() {
	cmd.Execute()
}
