package file

import (
	"bytes"
	"fmt"

	"github.com/benjamin10ks/distributed-ml-data-pipeline/ingestion/file/parser"
)

func parse(event RawEvent) ([]parser.Record, error) {
	if event.Format == "unknown" {
		return nil, fmt.Errorf("cannot parse unknown format for file: %s", event.Path)
	}

	p, err := parser.For(event.Format)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser for format %s: %w", event.Format, err)
	}

	records, err := p.Parse(bytes.NewReader(event.Payload))
	if err != nil {
		return nil, fmt.Errorf("failed to parse file: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in file")
	}

	return records, nil
}
