package columndb

import (
	"os"
	"time"
)

func Compact(catalog *Catalog, segmentDir string, toMerge []SegmentMeta) error {
	if len(toMerge) < 2 {
		return nil
	}

	allRows, err := getAllRows(toMerge)
	if err != nil {
		return err
	}

	newPath := generateNewPath(segmentDir)

	if err := WriteSegment(newPath, allRows); err != nil {
		return err
	}

	newMeta := createSegmentMetaFromRows(allRows, newPath)

	if err := catalog.Register(newMeta); err != nil {
		return err
	}

	oldPaths := getOldPaths(toMerge)

	if err := catalog.Retire(oldPaths); err != nil {
		return err
	}

	cleanUpOldPaths(oldPaths)

	return nil

}

func cleanUpOldPaths(oldPaths []string) {
	for _, p := range oldPaths {
		os.Remove(p)
	}
}

func getOldPaths(toMerge []SegmentMeta) []string {
	oldPaths := make([]string, len(toMerge))
	for i, s := range toMerge {
		oldPaths[i] = s.Path
	}
	return oldPaths
}

func createSegmentMetaFromRows(allRows []Row, newPath string) SegmentMeta {
	minT, maxT, metrics, hosts := extractSegmentAttributes(allRows)

	newMeta := SegmentMeta{
		Path: newPath, MinTime: minT, MaxTime: maxT, RowCount: len(allRows),
		Metrics: metrics, Hosts: hosts,
	}
	return newMeta
}

func extractSegmentAttributes(allRows []Row) (int64, int64, map[string]struct{}, map[string]struct{}) {
	minT, maxT := allRows[0].Timestamp, allRows[0].Timestamp
	metrics := make(map[string]struct{})
	hosts := make(map[string]struct{})
	for _, r := range allRows {
		if r.Timestamp < minT {
			minT = r.Timestamp
		}
		if r.Timestamp > maxT {
			maxT = r.Timestamp
		}
		metrics[r.MetricName] = struct{}{}
		hosts[r.Host] = struct{}{}
	}
	return minT, maxT, metrics, hosts
}

func getAllRows(toMerge []SegmentMeta) ([]Row, error) {
	var allRows []Row
	for _, seg := range toMerge {
		rows, err := readSegemt(seg.Path)
		if err != nil {
			return nil, err
		}
		allRows = append(allRows, rows...)
	}
	return allRows, nil
}

func generateNewPath(segmentDir string) string {
	return segmentDir + "/compacted_" + newSegmentID() + ".parquet"
}

func newSegmentID() string {
	return time.Now().Format("20060102150405.000000")
}
