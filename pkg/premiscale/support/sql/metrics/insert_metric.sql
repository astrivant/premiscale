INSERT INTO premiscale_metrics (message_id, sample_index, measurement, observed_at, tags, fields)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (message_id, sample_index) DO NOTHING
