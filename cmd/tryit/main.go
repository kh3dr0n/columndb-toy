package main

import (
	"columndb/columndb"
	"fmt"
	"os"
	"time"
)

func main() {
	os.Mkdir("data", 0755)

	catalog := columndb.NewCatalog()
	buf := columndb.NewBuffer("data", 2, 10*time.Second, catalog)

	buf.Add(columndb.Row{Timestamp: 100, MetricName: "cpu.usage", Value: 1, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 101, MetricName: "cpu.usage", Value: 2, Host: "host-a"})
	buf.Add(columndb.Row{Timestamp: 102, MetricName: "mem.usage", Value: 3, Host: "host-b"})
	buf.Add(columndb.Row{Timestamp: 103, MetricName: "mem.usage", Value: 4, Host: "host-b"})
	buf.Add(columndb.Row{Timestamp: 900, MetricName: "cpu.usage", Value: 99, Host: "host-a"})
	buf.Flush()

	results, err := columndb.Query(catalog, columndb.QueryParams{
		From:   0,
		To:     200,
		Metric: "cpu.usage",
	})

	if err != nil {
		panic(err)
	}

	fmt.Println("query results:")
	for _, r := range results {
		fmt.Printf("  ts=%d metric=%s value=%.1f host=%s\n", r.Timestamp, r.MetricName, r.Value, r.Host)
	}
}
