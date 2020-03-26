-- Copyright(C) 2016 Jingli Chen (Wine93), UPYUN Inc.

local math  = require "math"
local utils = require "modules.utils"

local ngx            = ngx
local tonumber       = tonumber
local floor          = math.floor
local min            = math.min
local ngx_now        = ngx.now
local req_start_time = ngx.req.start_time
local get_method     = ngx.get_method
local is_tab         = utils.is_tab

local _M = {
    _VERSION = "0.01",

    STATE_REQUEST = 0, -- connect, send, read header
    STATE_READ    = 1, -- read body

    TIME_RUN_OUT = 10,
}
local TIME_WEIGHT = { 0.1, 0.2, 0.7 }
local TIME_LIMIT = 0.001


function _M.register_callback(config)
    if not is_tab(config) then
        config = {}
    end

    local conn_timeout = config.cdn_timeout or 30
    local send_timeout = config.cdn_send_timeout or 60
    local read_timeout = config.cdn_read_timeout or 60
    local start_time = req_start_time()
    local bytes_sent = 0

    local function calc_timeout(phase, slice)
        local elapsed = ngx_now() - start_time
        local downstream_timeout

        if slice then
            downstream_timeout = config.cdn_downstream_timeout_slice or 60
        else
            downstream_timeout = config.cdn_downstream_timeout or 60
        end

        local useable_time = downstream_timeout - elapsed

        if phase == _M.STATE_REQUEST then
            if useable_time <= 0 then
                return
            end

            local timeout = { conn_timeout, send_timeout, read_timeout }
            local time_alloc = {}
            for i = 1, 3 do
                time_alloc[i] = useable_time * TIME_WEIGHT[i]
                time_alloc[i] = min(time_alloc[i], timeout[i])

                if time_alloc[i] < TIME_LIMIT then
                    time_alloc[i] = nil
                end
            end

            return time_alloc[1], time_alloc[2], time_alloc[3]
        end

        -- phase == STATE_READ
        local time4read = read_timeout
        if bytes_sent == 0 then
            bytes_sent = tonumber(ngx.var.bytes_sent) or 0
            time4read = min(useable_time, read_timeout)
        end

        return time4read
    end

    return calc_timeout
end


return _M
