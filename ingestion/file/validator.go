package file

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

const (
	MaxFileSize = 500 * 1024 * 1024 // 500 MB
)

// TODO: Add more data type support for images, videos, audio, etc. based on file signature and extension
func validate(event RawEvent) error {
	format := detectFormat(event.Path, event.Payload)
	if format == "unknown" {
		return fmt.Errorf("unsupported file format")
	}

	if len(event.Payload) == 0 {
		return fmt.Errorf("file is empty")
	}

	if len(event.Payload) > MaxFileSize {
		return fmt.Errorf("file size exceeds maximum limit of %d bytes", MaxFileSize)
	}

	h := sha256.New()
	_, err := io.ReadAll(io.TeeReader(bytes.NewReader(event.Payload), h))
	if err != nil {
		return fmt.Errorf("failed to read payload for hashing: %w", err)
	}
	hash := hex.EncodeToString(h.Sum(nil))
	if hash != event.ContentHash {
		return fmt.Errorf("content hash mismatch: expected %s, got %s", event.ContentHash, hash)
	}

	return nil
}

// sniffs for file type -- parquet, csv, ndjson. based on file extension or content
func detectFormat(key string, body []byte) string {
	if len(body) >= 4 && string(body[:4]) == "PAR1" {
		return "parquet"
	}

	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		return "gzip"
	}

	// Fallback to file extension
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '.' {
			ext := key[i+1:]
			switch ext {
			case "csv":
				return "csv"
			case "json", "ndjson", "jsonl":
				return "ndjson"
			case "parquet":
				return "parquet"
			}
			break
		}
	}

	return "unknown"
}
