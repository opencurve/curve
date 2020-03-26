-- Copyright (C) 2014 Jing Ye (yejingx), UPYUN Inc.

local checkups = require "resty.checkups"
local httpipe = require "modules.httpipe"
local reqlimit = require "modules.reqlimit"
local utils = require "modules.utils"

local type = type
local tostring = tostring
local str_format = string.format
local insert = table.insert
local concat = table.concat
local cjson = require "cjson.safe"


local req_start_time = ngx.req.start_time
local ngx_now = ngx.now
local send_headers = ngx.send_headers


local _M = { _VERSION = "0.10" }

local function is_func(f)
    return type(f) == "function"
end

local function set_uptime()
    if ngx.var.uptime then
        ngx.var.uptime = str_format("%.3f", ngx_now() - req_start_time())
    end
end


local function set_xstate(errmsg)
    if ngx.var.xstate then
        ngx.var.xstate = tostring(errmsg)
    end
end


local function set_upinfo(node, code, time, ctx)
    local append_var = function(k, v)
        if not v then
            return
        end

        local value  = ngx.var[k]
        if not value then
            return
        end

        ngx.var[k] = value == '-' and v or value .. ", " .. v
    end

    if code and time and ctx.upstats == true and ctx.host and ctx.port then
        local upstat = str_format("%s:%s/%s/%s", ctx.host, ctx.port, code, time)
        append_var("upstats", upstat)
    end

    local srv = ctx.srv
    if node and srv and srv.isp then
        node = ("%s/%s"):format(srv.isp, node)
    end

    if node and srv and srv.domain then
        node = ("%s-%s"):format(srv.domain, node)
    end

    append_var("upnode", node)
    append_var("uptimes", time)
    append_var("upcode", code)

    if node and code and time and ngx.var.http_x_debug then
        local upstat = str_format("%s/%s/%s", node, code, time)
        append_var("uptrace", upstat)
    end
end


function _M.close(hp)
    if hp and not hp:eof() then
        local ok, err = hp:close()
        if not ok then -- warn: unread data in buffer
            ngx.log(ngx.WARN, "failed to close: ", err)
        end
    end
end


local function prepare_opts(method, uri, source, opts)
    local client_hp, err = httpipe:new()
    if not client_hp then
        return nil, err
    end

    local tm, send_tm, read_tm = checkups.get_ups_timeout(source)
    local timeout = opts.timeout or tm or 5
    local send_timeout = opts.send_timeout or send_tm or timeout
    local read_timeout = opts.read_timeout or read_tm or timeout

    local calc_timeout = opts.calc_timeout
    if is_func(calc_timeout) then
        timeout, send_timeout, read_timeout = calc_timeout(reqlimit.STATE_REQUEST, opts.slice)
        if not timeout or not send_timeout or not read_timeout then
            return nil, reqlimit.TIME_RUN_OUT
        end
    end

    local method_with_request_body = {
        --__FORWARD_GET_PAYLOAD__
        GET = true, PUT = true, POST = true, DELETE = true, PATCH = true,
        --__FORWARD_GET_PAYLOAD__
    }
    if not opts.body and method_with_request_body[method] then
        opts.body = client_hp:get_client_body_reader(opts.chunk_size or 8192)
    end

    local req_opts = {
        method = method, path = uri,
        allow_redirects = opts.allow_redirects,
        redirect_follow_num = opts.redirect_follow_num,
        is_valid_addr = opts.is_valid_addr,
        ssl_enable = opts.ssl_enable,
        ssl_server_name = opts.ssl_server_name,
        ssl_verify = opts.ssl_verify or false,
        headers = opts.headers or {}, query = opts.query,
        unnormalize_header = opts.unnormalize_header,
        timeout = timeout * 1000,
        send_timeout = send_timeout * 1000,
        read_timeout = read_timeout * 1000,
        body = opts.body,
        maxsize = opts.max_body_size,
        stream = true,
        req_host = opts.req_host
    }

    return req_opts, nil
end


local function do_request(host, port, ctx, req_opts)
    local res, err, hp
    hp, err = httpipe:new()
    if not hp then
        return nil, err
    end

    ctx.hp = hp

    local start = ngx.now()
    hp:set_timeout(req_opts.timeout)

    res, err = hp:request(host, port, req_opts)

    ngx.log(ngx.INFO, "request ", host, ":", tostring(port), " ", cjson.encode(req_opts))

    if not res then
        ngx.log(ngx.WARN, "failed to request: ", host, ":", tostring(port), " ", err)
    end

    local uptime = ("%.3f"):format(ngx_now() - start)
    local upcode = res and res.status or (err == "timeout" and err or "err")
    local upnode = ("%s:%s"):format(host, port)

    set_upinfo(upnode, upcode, uptime, ctx)

    return res, err
end


local function capture(method, uri, source, opts)
    opts = opts or {}
    local req_opts, err = prepare_opts(method, uri, source, opts)
    if not req_opts then
        return nil, nil, err
    end

    local res
    local calc_timeout = opts.calc_timeout

    -- set_upinfo
    local ctx = {
        host = opts.host, port = opts.port,
        srv = opts.srv, upstats = opts.upstats
    }

    if source then
        res, err = checkups.ready_ok(source, do_request, {
            args = { ctx, req_opts }, cluster_key = opts.cluster_key
        })
    else
        res, err = do_request(opts.host, opts.port, ctx, req_opts)
    end

    local hp = ctx.hp
    local failed_filter = opts.failed_capture_filter
    local post_filter = opts.post_capture_filter

    if not res then
        if failed_filter then
            failed_filter(source, err)
        end
        _M.close(hp)
        return nil, nil, err
    end

    if post_filter then
        post_filter(source, res)
    end

    res.headers["Transfer-Encoding"] = nil
    res.headers["Connection"] = nil

    local discard_abnormal_body = opts.discard_abnormal_body

    local should_receive_body = true
    if discard_abnormal_body and (method ~= "GET"
        or (res.status ~= 200 and res.status ~= 206))
    then
        should_receive_body = false
    end

    if is_func(calc_timeout) then
        local read_timeout = calc_timeout(reqlimit.STATE_READ)
        if not read_timeout then
            _M.close(hp)
            return nil, nil, reqlimit.TIME_RUN_OUT
        end

        hp.read_timeout = read_timeout * 1000
    end

    if not should_receive_body or type(res.status) ~= "number" then
        hp:read_response{ body_filter = function (chunk) end }
        _M.close(hp)
    end


    return res, hp, nil
end


function _M.capture(method, uri, source, opts)
    local res, hp, err = capture(method, uri, source, opts)

    -- when POST failed(upstream 502/504/503/error)
    -- discard req body explicitly
    if (not res or (res and tonumber(res.status or 0) >= 500))
       and method == "POST" and opts.body
    then
        local _chunk, _err
        local reader = opts.body
        repeat
            _chunk, _err = reader()
        until not _chunk
    end

    return res, hp, err
end


function _M.flush_response(hp, res, buffer_num, buffer_size, calc_timeout)
    local buffer_num = buffer_num or 1
    local buffer_size = buffer_size or 8192
    local buffer = {}

    local chunk, err
    local reader = res.body_reader
    repeat
        if is_func(calc_timeout) then
            hp.read_timeout = (calc_timeout(reqlimit.STATE_READ) or 0) * 1000
        end

        chunk, err = reader(buffer_size)

        if err then
            err = tostring(err) or "error"
            if err == "timeout" then
                err = "read timeout"
            end
            set_xstate(err)

            if err ~= "closed" then
                ngx.log(ngx.WARN, "failed to read response: ", err)
            else
                ngx.log(ngx.INFO, "failed to read response: ", err)
            end

            if err == "exceeds maxsize" then
                _M.close(hp)
                if not ngx.headers_sent then
                    set_uptime()
                    return ngx.exit(ngx.HTTP_FORBIDDEN)
                end
            end

            if not ngx.headers_sent then
                if ngx.ctx.slice then
                    -- just return 423 for slice request when upstream
                    -- prematurely closed connection
                    ngx.status = 423
                    set_uptime()
                    return ngx.exit(423)
                end

                local ok, err = send_headers()
                if not ok then
                    ngx.log(ngx.WARN, "failed to send headers: ", err)
                end
            end

            ngx.flush(true) -- flush response headers to client forcibly
            set_uptime()
            return ngx.exit(444)
        end

        if chunk then
            if buffer_num == 1 then
                -- The buffer_num in Shanks is always 1 so far. So never
                -- insert it into table for avoiding the overhead table.concat.
                ngx.print(chunk)
                ngx.flush(true)
            else
                insert(buffer, chunk)
                if #buffer >= buffer_num then
                    ngx.print(concat(buffer))
                    ngx.flush(true)
                    buffer = {}
                end
            end
        end
    until not chunk

    if #buffer > 0 then
        ngx.print(concat(buffer))
        ngx.flush(true)
    end

    --get stream size from httpipe.total_size
    if ngx.var.upsize == "-" and ngx.var.from_upyun ~= "1" then
        ngx.var.upsize = hp and hp.total_size or 0
    end

    set_uptime()
end


function _M.raise_capture_error(err, raise_error_filter)
    set_uptime()

    err = tostring(err) or "error"
    ngx.log(ngx.WARN, "failed to capture: ", err)

    set_xstate(err)

    if type(raise_error_filter) == "function" then
        return raise_error_filter(err)
    end

    if err == "timeout" then
        return ngx.exit(ngx.HTTP_GATEWAY_TIMEOUT)
    end

    return ngx.exit(ngx.HTTP_SERVICE_UNAVAILABLE)
end


function _M.exec(method, uri, source, opts)
    local res, hp, err = _M.capture(method, uri, source, opts)

    if not res then
        return _M.raise_capture_error(err)
    end

    if type(res.status) == "number" then
        for k, v in pairs(res.headers) do
            ngx.header[k] = v
        end
    end

    ngx.status = res.status

    if not hp:eof() then
        return _M.flush_response(hp, res)
    else
        set_uptime()
        return ngx.exit(res.status)
    end
end


return _M
