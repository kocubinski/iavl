package scan

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/cosmos/iavl/v2"
	"github.com/spf13/cobra"
)

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
	}
	cmd.AddCommand(probeCommand(), rootsCommand())
	return cmd
}

func probeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "prob sqlite cgo configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			f, err := os.CreateTemp("", "iavl-v2-probe.sqlite")
			if err != nil {
				return err
			}
			fn := f.Name()
			fmt.Println("fn:", fn)
			conn, err := sql.Open("sqlite", fn)
			if err != nil {
				return err
			}

			_, err = conn.Exec("PRAGMA mmap_size=1000000000000")
			if err != nil {
				return err
			}

			var sz string
			row := conn.QueryRow("PRAGMA mmap_size")
			err = row.Scan(&sz)
			if err != nil {
				return err
			}
			fmt.Println("mmap:", sz)

			if err = conn.Close(); err != nil {
				return err
			}
			if err = os.Remove(f.Name()); err != nil {
				return err
			}
			return nil
		},
	}
	return cmd
}

func rootsCommand() *cobra.Command {
	var (
		dbPath  string
		version int64
	)
	cmd := &cobra.Command{
		Use:   "roots",
		Short: "list roots",
		RunE: func(cmd *cobra.Command, args []string) error {
			sql, err := iavl.NewSqliteDb(iavl.NewNodePool(), iavl.SqliteDbOptions{Path: dbPath})
			if err != nil {
				return err
			}
			node, err := sql.LoadRoot(version)
			if err != nil {
				return err
			}
			fmt.Printf("root: %+v\n", node)
			return sql.Close()
		},
	}
	cmd.Flags().StringVar(&dbPath, "db", "", "path to sqlite db")
	cmd.Flags().Int64Var(&version, "version", 0, "version to query")
	cmd.MarkFlagRequired("db")
	cmd.MarkFlagRequired("version")
	return cmd
}
