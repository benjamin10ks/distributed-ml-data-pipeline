// Package parser provides an interface for parsing different file formats.
package parser

import (
	"fmt"
	"io"
)

type Record map[string]any

type Parser interface {
	Parse(r io.Reader) ([]Record, error)
}

type Input struct {
	Body   []byte
	Format string
}

func For(format string) (Parser, error) {
	switch format {
	case "csv":
		return &CSVParser{}, nil
	case "ndjson":
		return &NDJSONParser{}, nil
	case "parquet":
		return &ParquetParser{}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}
