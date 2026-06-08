CREATE TABLE IF NOT EXISTS file_manifest (
    id           BIGSERIAL PRIMARY KEY,
    path         TEXT NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,  -- the idempotency key
    source       TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_manifest_status 
    ON file_manifest (status) 
    WHERE status IN ('pending', 'processing');  -- partial index, only rows you query
