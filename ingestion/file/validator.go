package file

// TODO: implement validation logic for the event. check if the file is not empty, if the format is supported, etc.
func validate(event RawEvent) error {
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
