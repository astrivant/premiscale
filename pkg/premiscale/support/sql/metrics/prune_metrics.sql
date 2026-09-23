DELETE FROM premiscale_metrics
WHERE observed_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 second')
