CREATE TABLE IF NOT EXISTS instances (
    id TEXT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    "group" TEXT NOT NULL,
    host TEXT NOT NULL,
    address TEXT NOT NULL,
    phase TEXT NOT NULL,
    error TEXT NOT NULL
);
