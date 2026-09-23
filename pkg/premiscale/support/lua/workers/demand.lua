local now = tonumber(redis.call('TIME')[1])
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now)
for _, identity in ipairs(expired) do
    redis.call('ZREM', KEYS[2], identity)
    redis.call('HDEL', KEYS[3], identity)
end
local active = 0
for _, count in ipairs(redis.call('HVALS', KEYS[3])) do
    active = active + tonumber(count)
end
-- Entries are deleted only after successful completion or dead-letter rejection.
-- XLEN therefore includes both unread and leased work, even with zero consumers.
return {active, redis.call('XLEN', KEYS[1])}
