package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/jedib0t/go-pretty/v6/table"
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

	err = WriteVisualRepresentation(ctx, os.Stdout, client.Open(cfg.table), cfg.prefix, cfg.limit, cfg.maxCellSize)
	if err != nil {
		log.Fatal(err)
	}
}

func WriteVisualRepresentation(
	ctx context.Context,
	w io.Writer,
	tbl *bigtable.Table,
	prefix string,
	limitRows int64,
	limitCellSize int,
) error {
	columns := make(map[string]map[string]int) // family -> column -> columnIndex
	rows := make(map[string]map[int][]byte)    // rowkey -> columnIndex -> data
	columnIndex := 0

	err := tbl.ReadRows(ctx, bigtable.PrefixRange(prefix), func(row bigtable.Row) bool {
		for family, items := range row {
			columns[family] = make(map[string]int)
			for _, cell := range items {
				if _, exists := columns[family][cell.Column]; !exists {
					columnIndex++
				}
				columns[family][cell.Column] = columnIndex
				if _, exists := rows[cell.Row]; !exists {
					rows[cell.Row] = make(map[int][]byte)
				}
				if len(cell.Value) > limitCellSize {
					rows[cell.Row][columnIndex] = cell.Value[:limitCellSize]
				} else {
					rows[cell.Row][columnIndex] = cell.Value
				}
			}
		}
		return true
	}, bigtable.RowFilter(bigtable.LatestNFilter(1)), bigtable.LimitRows(limitRows))
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		return fmt.Errorf("no rows returned for prefix: %s", prefix)
	}

	t := table.NewWriter()
	t.SetOutputMirror(w)

	familyRow := make(table.Row, columnIndex+1)
	columnRow := make(table.Row, columnIndex+1)
	familyRow[0] = ""
	columnRow[0] = ""

	for family, columnToIndex := range columns {
		for column, index := range columnToIndex {
			familyRow[index] = family
			columnRow[index] = strings.TrimPrefix(column, family+":")
		}
	}

	t.AppendHeader(familyRow, table.RowConfig{AutoMerge: true})
	t.AppendRow(columnRow)
	t.AppendSeparator()

	for rowKey, columnIndexToData := range rows {
		tableRow := make(table.Row, columnIndex+1)
		tableRow[0] = rowKey
		for index, data := range columnIndexToData {
			if data == nil {
				tableRow[index] = "<deleted>"
			} else if data[0] == 0x0 {
				tableRow[index] = "<pending>"
			} else {
				tableRow[index] = string(data)
			}
		}
		t.AppendRow(tableRow)
	}

	t.Render()
	return nil
}
