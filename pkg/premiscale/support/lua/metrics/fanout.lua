-- Check every destination before mutating any stream.
-- KEYS: subscriber streams; ARGV[1]: serialized metrics envelope.
for _, key in ipairs(KEYS) do
    local kind = redis.call('TYPE', key).ok
    if kind ~= 'none' and kind ~= 'stream' then
        return redis.error_reply('Metrics destination is not a stream')
    end
end
local ids = {}
for _, key in ipairs(KEYS) do
    table.insert(ids, redis.call('XADD', key, '*', 'message', ARGV[1]))
end
return ids
