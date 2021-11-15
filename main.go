package main

import (
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "csv2sql-go",
	Short: "csv+mysql commands",
	Long:  `csv+mysql commands`,
}

func Execute() {
	v := ImportCmd{}
	importCmd := &cobra.Command{
		Use:   "import",
		Short: "import csv into mysql",
		Long:  `import a csv file into a mysql db `,
		Run: func(cmd *cobra.Command, args []string) {
			if err := v.ValidateFlags(); err != nil {
				log.Fatalf("err=%v\n", err)
			}
			v.Execute(cmd, args)
		},
	}
	v.Flags(importCmd.Flags())
	rootCmd.AddCommand(importCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	Execute()
}
