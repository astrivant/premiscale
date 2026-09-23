SELECT cluster, instance
FROM volumes
WHERE host = %s AND path = %s;
