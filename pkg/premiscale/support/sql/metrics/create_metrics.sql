CREATE TABLE IF NOT EXISTS premiscale_metrics (
    message_id UUID NOT NULL,
    sample_index INTEGER NOT NULL,
    measurement TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    tags JSONB NOT NULL,
    fields JSONB NOT NULL,
    PRIMARY KEY (message_id, sample_index)
)
