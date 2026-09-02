package main

import (
	"columndb/columndb"
	"fmt"
	"os"
	"time"
)

func main() {
	os.MkdirAll("data", 0755)

	catalog, err := columndb.NewCatalog("data/catalog.wal")
	if err != nil {
		panic(err)
	}
	defer catalog.Close()

	buf := columndb.NewBuffer("data", 2, 10*time.Second, catalog)
	buf.Add(columndb.Row{Timestamp: 100, MetricName: "cpu.usage", Value: 1, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 101, MetricName: "cpu.usage", Value: 2, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 200, MetricName: "cpu.usage", Value: 3, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 201, MetricName: "cpu.usage", Value: 4, Host: "host-a"})
	buf.Flush()

	before := catalog.Prune(0, 999999, "", "")
	fmt.Printf("before compaction: %d segments\n", len(before))

	if err := columndb.Compact(catalog, "data", before); err != nil {
		panic(err)
	}

	after := catalog.Prune(0, 999999, "", "")
	fmt.Printf("after compaction: %d segments\n", len(after))

	results, _ := columndb.Query(catalog, columndb.QueryParams{From: 0, To: 999999})
	fmt.Printf("query still returns %d rows\n", len(results))
}
