package columndb

import (
	"os"

	"github.com/parquet-go/parquet-go"
)

func WriteSegment(path string, rows []Row) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := parquet.NewGenericWriter[Row](f)
	if _, err := writer.Write(rows); err != nil {
		return err
	}
	return writer.Close()
}
