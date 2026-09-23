local now = tonumber(redis.call('TIME')[1])
local previous = redis.call('HGET', KEYS[2], ARGV[1])
if previous then
    local record = cjson.decode(previous)
    if record[2] > now then return 0 end
    if #redis.call('XRANGE', KEYS[1], record[1], record[1], 'COUNT', 1) > 0 then
        return 0
    end
end
local identity = redis.call('XADD', KEYS[1], '*', 'message', ARGV[3])
redis.call('HSET', KEYS[2], ARGV[1], cjson.encode({identity, now + tonumber(ARGV[2])}))
return 1
