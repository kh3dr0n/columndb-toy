package columndb

type Row struct {
	Timestamp  int64   `parquet:"timestamp,timestamp"`
	MetricName string  `parquet:"metric_name,dict"`
	Value      float64 `parquet:"value"`
	Host       string  `parquet:"host,dict"`
}
