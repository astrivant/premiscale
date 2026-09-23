CREATE TABLE IF NOT EXISTS volumes (
    cluster TEXT NOT NULL,
    instance TEXT NOT NULL,
    host TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (host, path)
);
