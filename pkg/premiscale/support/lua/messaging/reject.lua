-- Atomically quarantine an invalid delivery owned by this consumer.
-- KEYS[1]: source stream; KEYS[2]: dead-letter stream.
-- ARGV[1]: consumer group; ARGV[2]: consumer; ARGV[3]: delivery ID;
-- ARGV[4]: serialized message; ARGV[5]: rejection reason.
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #pending == 0 or pending[1][2] ~= ARGV[2] then
    return 0
end
redis.call('XADD', KEYS[2], '*', 'source', ARGV[3], 'message', ARGV[4], 'reason', ARGV[5])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[3])
redis.call('XDEL', KEYS[1], ARGV[3])
return 1
