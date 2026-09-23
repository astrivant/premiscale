-- Atomically acknowledge an owned delivery and remove its stream entry.
-- KEYS[1]: source stream.
-- ARGV[1]: consumer group; ARGV[2]: consumer; ARGV[3]: delivery ID.
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending == 0 then
    return 0
end
if pending[1][2] ~= ARGV[2] then
    return -1
end
redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
redis.call('XDEL', KEYS[1], ARGV[3])
return 1
