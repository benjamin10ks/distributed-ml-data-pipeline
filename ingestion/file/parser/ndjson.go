package parser

import (
	"bytes"
	"encoding/json"
	"io"
)

type NDJSONParser struct{}

func (p *NDJSONParser) Parse(r io.Reader) ([]Record, error) {
	decoder := json.NewDecoder(r)

	var records []Record
	lineNum := 0

	for {
		var rec Record
		err := decoder.Decode(&rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
		lineNum++
	}
	return records, nil
}

func (p *NDJSONParser) ParseByBytes(b []byte) ([]Record, error) {
	return p.Parse(bytes.NewReader(b))
}
