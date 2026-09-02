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

	// prove replay worked: how many segments does the catalog already know about?
	existing := catalog.Prune(0, 999999, "", "")
	fmt.Printf("catalog recovered %d segment(s) from disk on startup\n", len(existing))

	buf := columndb.NewBuffer("data", 2, 10*time.Second, catalog)

	buf.Add(columndb.Row{Timestamp: 100, MetricName: "cpu.usage", Value: 1, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 101, MetricName: "cpu.usage", Value: 2, Host: "host-a"})
	buf.Flush()

	fmt.Println("wrote and registered a segment. now check data/catalog.wal")
}
