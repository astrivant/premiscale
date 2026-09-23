-- Renew a delivery lease only while it belongs to this consumer.
-- KEYS[1]: source stream.
-- ARGV[1]: consumer group; ARGV[2]: consumer; ARGV[3]: delivery ID.
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending == 0 or pending[1][2] ~= ARGV[2] then
    return 0
end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
return 1
