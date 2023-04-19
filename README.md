# vcbt - Visual Cloud Bigtable CLI

`vcbt` is a cli tool inspired by [cbt](https://cloud.google.com/bigtable/docs/cbt-reference) that aims to give a nicer visual representation of data in a Google Cloud Bigtable table.

## Installing vcbt

for now, there is nothing in place for fancy installing. Simply install from source to try.

```shell
$ git clone git@github.com:GoosvandenBekerom/vcbt.git
$ cd vcbt
$ go mod tidy
$ go build
$ ./vcbt -help
```

## Using as library

Pretty print table

```go
package example

import (
	"context"
	"cloud.google.com/go/bigtable"
	"github.com/GoosvandenBekerom/vcbt/representation"
)

func Example(ctx context.Context, client *bigtable.Client, cfg Config) error {
	return representation.WriteTable(ctx, os.Stdout, client.Open(cfg.table), cfg.prefix, cfg.limit, cfg.maxCellSize,
		func(data []byte) (representation string, mapped bool) {
			if data == nil {
				return "<deleted>", true
			} else if data[0] == 0x0 {
				return "<pending>", true
			}
			return "", false
		},
	)
}
```