package main

import (
	"github.com/spf13/cobra"

	"github.com/cosmos/iavl/v2/cmd/rollback"
	"github.com/cosmos/iavl/v2/cmd/scan"
	"github.com/cosmos/iavl/v2/cmd/snapshot"
)

func RootCommand() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "iavl",
		Short: "benchmark cosmos/iavl",
	}
	cmd.AddCommand(
		//gen.Command(),
		snapshot.Command(),
		rollback.Command(),
		scan.Command(),
		//bench.Command(),
		latestCommand(),
	)
	return cmd, nil
}
