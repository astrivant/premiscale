local now = tonumber(redis.call('TIME')[1])
local count = tonumber(ARGV[2])
if count == 0 then
    redis.call('ZREM', KEYS[1], ARGV[1])
    redis.call('HDEL', KEYS[2], ARGV[1])
else
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
    redis.call('HSET', KEYS[2], ARGV[1], count)
end
return 1
