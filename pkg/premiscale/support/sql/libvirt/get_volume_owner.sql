SELECT cluster, instance
FROM volumes
WHERE host = ? AND path = ?;
