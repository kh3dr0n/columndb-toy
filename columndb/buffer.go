package columndb

import (
	"fmt"
	"sync"
	"time"
)

type Buffer struct {
	mu        sync.Mutex
	rows      []Row
	maxRows   int
	maxAge    time.Duration
	lastFlush time.Time

	segmentDir string
	segmentNum int
	catalog    *Catalog
}

func NewBuffer(segmentDir string, maxRows int, maxAge time.Duration, catalog *Catalog) *Buffer {
	return &Buffer{
		maxRows:    maxRows,
		maxAge:     maxAge,
		lastFlush:  time.Now(),
		segmentDir: segmentDir,
		catalog:    catalog,
	}
}

func (b *Buffer) Add(r Row) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.rows = append(b.rows, r)

	sizeTriggered := len(b.rows) >= b.maxRows
	timeTriggered := time.Since(b.lastFlush) >= b.maxAge

	if sizeTriggered || timeTriggered {
		return b.flushLocked()
	}

	return nil
}

func (b *Buffer) flushLocked() error {

	if len(b.rows) == 0 {
		return nil
	}

	b.segmentNum++
	path := fmt.Sprintf("%s/segement_%04d.parket", b.segmentDir, b.segmentNum)

	if err := WriteSegment(path, b.rows); err != nil {
		return err

	}

	minT, maxT := b.rows[0].Timestamp, b.rows[0].Timestamp
	metrics := make(map[string]struct{})
	hosts := make(map[string]struct{})
	for _, r := range b.rows {
		if r.Timestamp < minT {
			minT = r.Timestamp
		}
		if r.Timestamp > maxT {
			maxT = r.Timestamp
		}
		metrics[r.MetricName] = struct{}{}
		hosts[r.Host] = struct{}{}
	}

	if err := b.catalog.Register(SegmentMeta{
		Path:     path,
		MinTime:  minT,
		MaxTime:  maxT,
		RowCount: len(b.rows),
		Metrics:  metrics,
		Hosts:    hosts,
	}); err != nil {
		return err
	}

	fmt.Printf("flushed %d rows -> %s\n", len(b.rows), path)

	b.rows = nil
	b.lastFlush = time.Now()
	return nil
}

func (b *Buffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked()
}
