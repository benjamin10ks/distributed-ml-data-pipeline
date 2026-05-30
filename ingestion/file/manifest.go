package file

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ManifestStatus string

const (
	StatusPending     ManifestStatus = "pending"
	StatusProcessing  ManifestStatus = "processing"
	StatusDone        ManifestStatus = "done"
	StatusFailed      ManifestStatus = "failed"
	StatusQuarantined ManifestStatus = "quarantined"
)

type ManifestEntry struct {
	Path        string
	ContentHash string
	Source      string
	Status      ManifestStatus
	CreatedAt   time.Time
	ProcessedAt *time.Time
}

type Manifest struct {
	db *pgxpool.Pool
}

func NewManifest(db *pgxpool.Pool) *Manifest {
	return &Manifest{db: db}
}

func (m *Manifest) Insert(ctx context.Context, entry ManifestEntry) error {
	_, err := m.db.Exec(ctx, `
		INSERT INTO file_manifest (path, content_hash, source, status, created_at)
		VALUES ( $1, $2, $3, $4, $5 )
		ON CONFLICT (content_hash) DO NOTHING
		`, entry.Path, entry.ContentHash, entry.Source, StatusPending)
	return err
}

func (m *Manifest) GetByHash(ctx context.Context, contentHash string) (*ManifestEntry, error) {
	row := m.db.QueryRow(ctx, `
		SELECT id, path, content_hash, source, status, created_at, processed_at
		FROM file_manifest WHERE content_hash = $1
		`, contentHash)

	var e ManifestEntry
	err := row.Scan(&e.Path, &e.ContentHash, &e.Source, &e.Status, &e.CreatedAt, &e.ProcessedAt)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (m *Manifest) UpdateStatus(ctx context.Context, contentHash string, status ManifestStatus) error {
	var processedAt *time.Time
	if status == StatusDone || status == StatusFailed || status == StatusQuarantined {
		t := time.Now().UTC()
		processedAt = &t
	}
	_, err := m.db.Exec(ctx, `
		UPDATE file_manifest
		SET status = $1, processed_at = $2
		WHERE content_hash = $3
		`, status, processedAt, contentHash)
	return err
}

func (m *Manifest) GetStuck(ctx context.Context, olderThan time.Duration) ([]ManifestEntry, error) {
	rows, err := m.db.Query(ctx, `
		SELECT id, path, content_hash, source, status, created_at, processed_at
		FROM file_manifest
		WHERE status = 'processing' AND created_at < NOW() - $1::interval
		`, fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []ManifestEntry
	for rows.Next() {
		var entry ManifestEntry
		if err := rows.Scan(&entry.Path, &entry.ContentHash, &entry.Source, &entry.Status, &entry.CreatedAt, &entry.ProcessedAt); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
