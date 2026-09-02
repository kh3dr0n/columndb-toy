package main

import (
	"columndb/columndb"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

func main() {
	os.RemoveAll("data") // clean slate
	os.MkdirAll("data", 0755)

	catalog, err := columndb.NewCatalog("data/catalog.wal")
	if err != nil {
		panic(err)
	}
	defer catalog.Close()

	buf := columndb.NewBuffer("data", 50, 2*time.Second, catalog)

	const numWriters = 10
	const rowsPerWriter = 200

	var wg sync.WaitGroup

	// Writers: many goroutines calling Add concurrently.
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < rowsPerWriter; i++ {
				ts := int64(writerID*rowsPerWriter + i)
				err := buf.Add(columndb.Row{
					Timestamp:  ts,
					MetricName: "cpu.usage",
					Value:      rand.Float64() * 100,
					Host:       fmt.Sprintf("host-%d", writerID%3),
				})
				if err != nil {
					fmt.Println("ADD ERROR:", err)
				}
			}
		}(w)
	}

	// A reader hammering Query concurrently with the writers above.
	var readerWG sync.WaitGroup
	stop := make(chan struct{})
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				results, err := columndb.Query(catalog, columndb.QueryParams{From: 0, To: 999999})
				if err != nil {
					fmt.Println("QUERY ERROR:", err)
				}
				_ = results
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	compactStop := make(chan struct{})
	var compactWG sync.WaitGroup
	compactWG.Add(1)
	go func() {
		defer compactWG.Done()
		for {
			select {
			case <-compactStop:
				return
			case <-time.After(20 * time.Millisecond):
				segs := catalog.Prune(0, 999999, "", "")
				fmt.Printf("[compactor] tick, saw %d segments\n", len(segs))
				if len(segs) >= 2 {
					fmt.Printf("[compactor] merging %s + %s\n", segs[0].Path, segs[1].Path)
					if err := columndb.Compact(catalog, "data", segs[:2]); err != nil {
						fmt.Println("COMPACT ERROR:", err)
					}
				}
			}
		}
	}()

	wg.Wait() // all writers done
	buf.Flush()
	close(stop)
	readerWG.Wait()

	close(compactStop)
	compactWG.Wait()

	// Final correctness check: did we lose any rows?
	final, err := columndb.Query(catalog, columndb.QueryParams{From: 0, To: 999999})
	if err != nil {
		panic(err)
	}
	expected := numWriters * rowsPerWriter
	fmt.Printf("expected %d rows total, got %d\n", expected, len(final))
	if len(final) != expected {
		fmt.Println("!!! ROW COUNT MISMATCH — investigate a race !!!")
	} else {
		fmt.Println("row counts match — no rows lost under concurrent load")
	}
}
