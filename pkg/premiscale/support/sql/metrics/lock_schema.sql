-- Serialize first-connection DDL across independently scaled publisher processes.
SELECT pg_advisory_xact_lock(1886545253, 1835365490)
