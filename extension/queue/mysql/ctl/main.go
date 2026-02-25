package main

import (
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
