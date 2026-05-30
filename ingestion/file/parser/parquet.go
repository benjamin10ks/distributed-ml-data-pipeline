package parser

import (
	"bytes"
	"io"
	"maps"

	"github.com/parquet-go/parquet-go"
)

type ParquetParser struct{}

func (p *ParquetParser) Parse(r io.Reader) ([]Record, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return p.ParseByBytes(b)
}

func (p *ParquetParser) ParseByBytes(b []byte) ([]Record, error) {
	file, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, err
	}

	schema := file.Schema()
	fields := schema.Fields()

	var records []Record

	reader := parquet.NewGenericReader[map[string]any](file)
	defer reader.Close()

	rows := make([]map[string]any, 128)
	for {
		n, err := reader.Read(rows)
		for i := 0; i < n; i++ {
			record := make(Record, len(fields))
			maps.Copy(record, rows[i])
			records = append(records, record)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}
