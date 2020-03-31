local _M = {}

-- check all the configure from
-- https://github.com/upyun/lua-resty-checkups

_M.global = {
    --  Interval of sending heartbeats to backend servers.
    checkup_timer_interval = 15,
    
    -- If set to true, checkups will sync upstram statuses
    -- from checkups to Nginx upstream blocks.
    checkup_shd_sync_enable = true,

    -- Interval of syncing upstream status from checkups
    -- to Nginx upstream blocks.
    shd_config_timer_interval = 1,
}


_M.snapshot = {
    -- Enable or disable heartbeats to servers
    enable = false,

    -- Cluster type, must be one of general, redis, mysql, http. 
    typ = "http",

    -- Limits the time during which a request can be responsed,
    -- likewise nginx proxy_next_upstream_timeout.
    try_timeout = 30,

    --  Connect timeout to upstream servers.
    timeout = 15,

    -- Retry count. Default is the number of servers.
    try = 100,
    -- If set to true and all the servers in the cluster are failing
    -- checkups will not mark the last failing server as unavailable(err)
    -- instead, it will be marked as unstable(still available in next try)
    protected = true,

    -- cluster configure info
   cluster = {
        {   -- level 1
            servers = {
                { host = "10.182.26.25", port = 5555 },
                { host = "10.182.26.17", port = 5555 },
                { host = "10.182.26.16", port = 5555 },
            }
        }
    }
}

return _M
