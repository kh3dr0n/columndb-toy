package columndb

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type SegmentMeta struct {
	Path     string
	MinTime  int64
	MaxTime  int64
	RowCount int
	Metrics  map[string]struct{}
	Hosts    map[string]struct{}
}

type segmentMetaJSON struct {
	Path     string   `json:"path"`
	MinTime  int64    `json:"min_time"`
	MaxTime  int64    `json:"max_time"`
	RowCount int      `json:"row_count"`
	Metrics  []string `json:"metrics"`
	Hosts    []string `json:"hosts"`
}

type Catalog struct {
	mu       sync.Mutex
	segments []SegmentMeta
	walFile  *os.File
}

type walRecord struct {
	Type    string          `json:"type"`
	Segment segmentMetaJSON `json:"segment,omitempty"`
	Paths   []string        `json:"paths,omitempty"`
}

func NewCatalog(walPath string) (*Catalog, error) {
	c := &Catalog{}

	if f, err := os.Open(walPath); err == nil {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var rec walRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				f.Close()
				return nil, err
			}
			switch rec.Type {
			case "add":
				c.segments = append(c.segments, fromJSON(rec.Segment))
			case "retire":
				c.removeByPaths(rec.Paths)
			}
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	c.walFile = walFile

	return c, nil
}

func toJSON(m SegmentMeta) segmentMetaJSON {
	metrics := make([]string, 0, len(m.Metrics))
	for k := range m.Metrics {
		metrics = append(metrics, k)
	}
	hosts := make([]string, 0, len(m.Hosts))
	for k := range m.Hosts {
		hosts = append(hosts, k)
	}
	return segmentMetaJSON{
		Path: m.Path, MinTime: m.MinTime, MaxTime: m.MaxTime, RowCount: m.RowCount,
		Metrics: metrics, Hosts: hosts,
	}
}

func fromJSON(r segmentMetaJSON) SegmentMeta {
	metrics := make(map[string]struct{}, len(r.Metrics))
	for _, v := range r.Metrics {
		metrics[v] = struct{}{}
	}
	hosts := make(map[string]struct{}, len(r.Hosts))
	for _, v := range r.Hosts {
		hosts[v] = struct{}{}
	}
	return SegmentMeta{
		Path: r.Path, MinTime: r.MinTime, MaxTime: r.MaxTime, RowCount: r.RowCount,
		Metrics: metrics, Hosts: hosts,
	}
}

func (c *Catalog) Register(meta SegmentMeta) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec := walRecord{Type: "add", Segment: toJSON(meta)}
	if err := c.appendLocked(rec); err != nil {
		return err
	}
	c.segments = append(c.segments, meta)
	return nil
}

func (c *Catalog) Prune(from, to int64, metric, host string) []SegmentMeta {
	c.mu.Lock()
	defer c.mu.Unlock()

	var matches []SegmentMeta
	for _, s := range c.segments {
		if s.MaxTime < from || s.MinTime > to {
			continue
		}
		if !s.hasMetric(metric) {
			continue
		}

		if !s.hasHost(host) {
			continue
		}

		matches = append(matches, s)
	}
	return matches
}

func (m SegmentMeta) hasMetric(name string) bool {
	if name == "" {
		return true
	}
	_, ok := m.Metrics[name]
	return ok
}

func (m SegmentMeta) hasHost(host string) bool {
	if host == "" {
		return true
	}
	_, ok := m.Hosts[host]
	return ok
}

func (c *Catalog) Close() error {
	return c.walFile.Close()
}
func (c *Catalog) SegmentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.segments)
}

func (c *Catalog) Retire(paths []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec := walRecord{Type: "retire", Paths: paths}
	if err := c.appendLocked(rec); err != nil {
		return err
	}
	c.removeByPaths(paths)
	return nil
}

func (c *Catalog) appendLocked(rec walRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := c.walFile.Write(append(line, '\n')); err != nil {
		return err
	}
	return c.walFile.Sync()
}

func (c *Catalog) removeByPaths(paths []string) {
	remove := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		remove[p] = struct{}{}
	}
	var kept []SegmentMeta
	for _, s := range c.segments {
		if _, dead := remove[s.Path]; !dead {
			kept = append(kept, s)
		}
	}
	c.segments = kept
}
