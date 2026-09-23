CREATE TABLE IF NOT EXISTS vm_observations (
    cluster TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    host TEXT NOT NULL,
    address TEXT NOT NULL,
    state INTEGER,
    reason INTEGER,
    vcpus INTEGER,
    memory_bytes BIGINT,
    storage_bytes BIGINT,
    observed_at TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (cluster, id)
)
