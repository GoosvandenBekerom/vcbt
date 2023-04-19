package main

import (
	"cloud.google.com/go/bigtable"
	"context"
	"flag"
	"log"
	"math"
	"os"
	"vcbt/representation"
)

type flags struct {
	project     string
	instance    string
	table       string
	prefix      string
	limit       int64
	maxCellSize int
}

func main() {
	var cfg flags
	flag.StringVar(&cfg.project, "p", "local", "google cloud project")
	flag.StringVar(&cfg.instance, "i", "local", "bigtable instance")
	flag.StringVar(&cfg.table, "t", "", "table to read from")
	flag.StringVar(&cfg.prefix, "prefix", "", "return only rows that have this key prefix")
	flag.Int64Var(&cfg.limit, "limit", 1, "amount of rows to return")
	flag.IntVar(&cfg.maxCellSize, "max-cell-size", math.MaxInt, "cut off cell values after this amount of bytes")
	flag.Parse()

	if cfg.table == "" {
		log.Fatal("please supply a table name with -t")
	}

	ctx := context.Background()

	client, err := bigtable.NewClient(ctx, cfg.project, cfg.instance)
	if err != nil {
		log.Fatal(err)
	}

	err = representation.WriteTable(ctx, os.Stdout, client.Open(cfg.table), cfg.prefix, cfg.limit, cfg.maxCellSize,
		func(data []byte) (representation string, mapped bool) {
			if data == nil {
				return "<deleted>", true
			} else if data[0] == 0x0 {
				return "<pending>", true
			}
			return "", false
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
