package cmd

import (
	"fmt"
	"runtime"

	"github.com/ethpandaops/execution-processor/internal/version"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Prints the version of execution-processor.",
	Long:  `Prints the version of execution-processor.`,
	Run: func(cmd *cobra.Command, args []string) {
		initCommon()

		fmt.Printf("Version: %s\nCommit: %s\nOS/Arch: %s/%s\n",
			version.Release, version.GitCommit, runtime.GOOS, runtime.GOARCH)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
