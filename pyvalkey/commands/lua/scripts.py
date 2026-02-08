LUA_RUN_WITH_TIMEOUT = b"""
function(f, timeout_seconds, should_stop, KEYS, ARGV)
    local start_time = os.time()
    
    _G.KEYS = KEYS
    _G.ARGV = ARGV

    debug.sethook(function()
        if os.difftime(os.time(), start_time) > timeout_seconds or should_stop() then
            error("Timeout reached!")
        end
    end, "", 500)
    
    local status, result = pcall(f)

    debug.sethook()
    
    _G.KEYS = nil
    _G.ARGV = nil

    return status, result
end
""".strip()
