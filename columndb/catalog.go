package columndb

import "sync"

type SegementMeta struct {
	Path     string
	MinTime  int64
	MaxTime  int64
	RowCount int
	Metrics  map[string]struct{}
	Hosts    map[string]struct{}
}

type Catalog struct {
	mu       sync.Mutex
	segments []SegementMeta
}

func NewCatalog() *Catalog {
	return &Catalog{}
}

func (c *Catalog) Register(meta SegementMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.segments = append(c.segments, meta)
}

func (c *Catalog) Prune(from, to int64, metric, host string) []SegementMeta {
	c.mu.Lock()
	defer c.mu.Unlock()

	var matches []SegementMeta
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

func (m SegementMeta) hasMetric(name string) bool {
	if name == "" {
		return true
	}
	_, ok := m.Metrics[name]
	return ok
}

func (m SegementMeta) hasHost(host string) bool {
	if host == "" {
		return true
	}
	_, ok := m.Hosts[host]
	return ok
}
