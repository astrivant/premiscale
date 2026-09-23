INSERT INTO instances
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name, "group" = EXCLUDED."group", host = EXCLUDED.host,
    address = EXCLUDED.address, phase = EXCLUDED.phase, error = EXCLUDED.error;
