package main

import (
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
