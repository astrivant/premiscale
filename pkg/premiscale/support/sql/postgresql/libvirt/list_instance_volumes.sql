SELECT path
FROM volumes
WHERE cluster = %s AND instance = %s AND host = %s;
