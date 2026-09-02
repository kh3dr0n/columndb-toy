package columndb

import (
	"os"

	"github.com/parquet-go/parquet-go"
)

type QueryParams struct {
	From   int64
	To     int64
	Metric string
	Host   string
}

func Query(catalog *Catalog, params QueryParams) ([]Row, error) {
	candidates := catalog.Prune(params.From, params.To, params.Metric, params.Host)

	var results []Row
	for _, seg := range candidates {
		rows, err := readSegment(seg.Path)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if matchesRow(r, params) {
				results = append(results, r)
			}
		}

	}
	return results, nil
}

func readSegment(path string) ([]Row, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := parquet.NewGenericReader[Row](f)

	defer reader.Close()

	rows := make([]Row, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && n == 0 {
		return nil, err
	}
	return rows[:n], nil
}

func matchesRow(r Row, p QueryParams) bool {
	if r.Timestamp < p.From || r.Timestamp > p.To {
		return false
	}
	if p.Metric != "" && r.MetricName != p.Metric {
		return false
	}
	if p.Host != "" && r.Host != p.Host {
		return false
	}
	return true
}
