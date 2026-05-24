package file

import "time"

type ManifestEntry struct {
	Path        string
	ContentHash string
	Source      string
	Status      string
	CreatedAt   time.Time
	ProcessedAt *time.Time
}
