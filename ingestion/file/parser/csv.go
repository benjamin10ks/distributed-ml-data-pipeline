package parser

import (
	"encoding/csv"
	"fmt"
	"io"
)

type CSVParser struct{}

func (p *CSVParser) Parse(r io.Reader) ([]Record, error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true // reduce allocations by reusing the same record slice

	headers, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	headers = append([]string(nil), headers...) // copy headers to avoid reuse issues

	var records []Record
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}
		if len(row) != len(headers) {
			return nil, fmt.Errorf("row has %d fields but expected %d based on headers", len(row), len(headers))
		}

		record := make(Record, len(headers))
		for i, header := range headers {
			record[header] = row[i]
		}
		records = append(records, record)
	}

	return records, nil
}
