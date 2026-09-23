SELECT measurement, observed_at, tags, fields
FROM premiscale_metrics
WHERE message_id = %s
ORDER BY sample_index
