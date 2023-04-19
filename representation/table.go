package representation

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/jedib0t/go-pretty/v6/table"
)

// ValueMapperFunc can be used to change the visual representation for specific values.
// it receives a []byte representing the current data to be rendered, and it should return 2 values,
// the visual representation of the data and a boolean indicating whether the value was mapped.
type ValueMapperFunc func(data []byte) (representation string, mapped bool)

// WriteTable writes a pretty printed table to the given io.Writer
// of rows in the given *bigtable.Table that have the given prefix in their row key.
// the maximum amount of rows can be limited by the limitRows parameter.
// the size of data shown per cell in the visual representation can be limited by the limitCellSize parameter.
// mapValues can be used to map values to a human-readable representation,
// for example []byte{0x0} in one of the projects I worked in means that the data for that cell hasn't been received yet
func WriteTable(
	ctx context.Context,
	w io.Writer,
	tbl *bigtable.Table,
	prefix string,
	limitRows int64,
	limitCellSize int,
	mapValues ValueMapperFunc,
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
			if mapValues != nil {
				if representation, mapped := mapValues(data); mapped {
					tableRow[index] = representation
					continue
				}
			}
			tableRow[index] = string(data)
		}
		t.AppendRow(tableRow)
	}

	t.Render()
	return nil
}
