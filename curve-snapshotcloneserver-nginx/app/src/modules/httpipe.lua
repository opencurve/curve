-- Copyright (C) Monkey Zhang (timebug), UPYUN Inc.


local type = type
local error = error
local pairs = pairs
local ipairs = ipairs
local rawset = rawset
local rawget = rawget
local sub = string.sub
local gsub = string.gsub
local find = string.find
local tostring = tostring
local tonumber = tonumber
local tcp = ngx.socket.tcp
local match = string.match
local upper = string.upper
local lower = string.lower
local concat = table.concat
local insert = table.insert
local format = string.format
local setmetatable = setmetatable
local ngx_re_match = ngx.re.match
local encode_args = ngx.encode_args
local ngx_req_socket = ngx.req.socket
local ngx_req_get_headers = ngx.req.get_headers


local _M = { _VERSION = "0.08" }

--------------------------------------
-- LOCAL CONSTANTS                  --
--------------------------------------

local mt = { __index = _M }

local HTTP = {
    [11] = " HTTP/1.1\r\n",
    [10] = " HTTP/1.0\r\n"
}

local PORT = {
    http = 80,
    https = 443
}

local USER_AGENT = "Resty/HTTPipe-" .. _M._VERSION

local STATE_NOT_READY = 0
local STATE_BEGIN = 1
local STATE_READING_HEADER = 2
local STATE_READING_BODY = 3
local STATE_EOF = 4

local common_headers = {
    "Cache-Control",
    "Content-Length",
    "Content-Type",
    "Date",
    "ETag",
    "Expires",
    "Host",
    "Location",
    "User-Agent"
}

for _, key in ipairs(common_headers) do
    rawset(common_headers, key, key)
    rawset(common_headers, lower(key), key)
end

local state_handlers

--------------------------------------
-- HTTP BASE FUNCTIONS              --
--------------------------------------

local function normalize_header(key)
    local val = common_headers[key]
    if val then
        return val
    end
    key = lower(key)
    val = common_headers[lower(key)]
    if val then
        return val
    end

    key = gsub(key, "^%l", upper)
    key = gsub(key, "-%l", upper)
    return key
end


local function req_header(self, opts)
    self.method = upper(opts.method or "GET")

    local req = {
        self.method,
        " "
    }

    local path = opts.path
    if type(path) ~= "string" then
        path = "/"
    elseif sub(path, 1, 1) ~= "/" then
        path = "/" .. path
    end
    insert(req, path)

    if type(opts.query) == "table" then
        opts.query = encode_args(opts.query)
    end

    if type(opts.query) == "string" then
        insert(req, "?" .. opts.query)
    end

    insert(req, HTTP[opts.version])

    opts.headers = opts.headers or {}

    local headers = {}

    for k, v in pairs(opts.headers) do
        if opts.unnormalize_header then
            headers[k] = v
        else
            headers[normalize_header(k)] = v
        end
    end

    if type(opts.body) == "string" then
        headers["Content-Length"] = #opts.body
    elseif self.previous.content_length and
    self.previous.content_length >= 0 then
        headers["Content-Length"] = self.previous.content_length
    end

    if type(opts.body) == "function" and
    not headers["Content-Length"] and not headers["Transfer-Encoding"] then
        headers["Transfer-Encoding"] = "chunked"
    end

    if not headers["Host"] then
        headers["Host"] = self.host
    end

    if not headers["User-Agent"] then
        headers["User-Agent"] = USER_AGENT
    end

    if not headers["Accept"] then
        headers["Accept"] = "*/*"
    end

    if opts.version == 10 and not headers["Connection"] then
        headers["Connection"] = "keep-alive"
    end

    for key, values in pairs(headers) do
        if type(values) ~= "table" then
            values = { values }
        end

        key = tostring(key)
        for _, value in pairs(values) do
            insert(req, key .. ": " .. tostring(value) .. "\r\n")
        end
    end

    insert(req, "\r\n")

    return concat(req), headers
end


-- local scheme, host, port, path, args = unpack(_M:parse_uri(uri))
function _M.parse_uri(self, uri)
    local r = [[^(https?)://([^:/]+)(?::(\d+))?(.*)]]
    local m, err = ngx_re_match(uri, r, "jo")
    if not m then
        return nil, err or "bad uri"
    end

    if not m[3] then m[3] = PORT[m[1]] end
    if not m[4] then
        m[4] = "/"
    else
        local raw = m[4]
        local from = find(raw, "?")
        if from then
            m[4] = raw:sub(1, from - 1) -- path
            m[5] = raw:sub(from + 1)    -- args
        end
    end

    return m
end


local function init(self)
    self.total_size = 0
    self.state = STATE_NOT_READY
    self.chunked = false
    self.keepalive = true
    self._eof = false
    self.previous = {}
    self.remaining = nil

    return self
end


--------------------------------------
-- HTTP PIPE FUNCTIONS              --
--------------------------------------

-- local hp, err = _M:new(chunk_size?, sock?)
function _M.new(self, chunk_size, sock)
    if not sock then
        local s, err = tcp()
        if not s then
            return nil, err
        end
        sock = s
    end

    return setmetatable(init({
        sock = sock,
        chunk_size = chunk_size or 8192,
    }), mt)
end


-- local ok, err = _M:set_timeout(time)
function _M.set_timeout(self, time)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(time)
end


-- local session, err = _M:ssl_handshake(self, ...)
function _M.ssl_handshake(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if not ngx.config
        or not ngx.config.ngx_lua_version
        or ngx.config.ngx_lua_version < 9011
    then
        error("ngx_lua 0.9.11+ required")
    end

    return sock:sslhandshake(...)
end


-- local ok, err = _M:connect(self, host, port, opts?)
-- local ok, err = _M:connect("unix:/path/to/unix-domain.socket", opts?)
function _M.connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    self.host = select(1, ...)
    if sub(self.host, 1, 5) == "unix:" then
        -- https://tools.ietf.org/html/rfc2616#section-14.23
        -- A client MUST include a Host header field in all HTTP/1.1 request
        -- messages . If the requested URI does not include an Internet host
        -- name for the service being requested, then the Host header field MUST
        -- be given with an empty value.
        self.host = ""
    end

    self.port = select(2, ...)
    if type(self.port) == "string" then
        self.port = tonumber(self.port)
    elseif type(self.port) ~= "number" then
        self.port = nil
    end

    return sock:connect(...)
end


local function discard_line(self)
    local read_line = self.read_line

    local line, err = read_line()
    if not line then
        return nil, err
    end

    return 1
end


local function should_receive_body(method, code)
    if method == "HEAD" then return nil end
    if code == 204 or code == 304 then return nil end
    if code >= 100 and code < 200 then return nil end
    return true
end


local function read_body_part(self)
    if not self.is_req_socket and
    not should_receive_body(self.method, self.status_code) then
        self.state = STATE_EOF
        return 'body_end', nil
    end

    local sock = self.sock
    local remaining = self.remaining
    local chunk_size = self.chunk_size

    if self.maxsize and remaining and remaining > self.maxsize then
        return nil, nil, "exceeds maxsize"
    end

    if self.chunked == true and
    (remaining == nil or remaining == 0) then
        local read_line = self.read_line
        local data, err = read_line()

        if err then
            return nil, nil, err
        end

        if data == "" then
            data, err = read_line()
            if err then
                return nil, nil, err
            end
        end

        if data then
            if data == "0" then
                local ok, err = discard_line(self)
                if not ok then
                    return nil, nil, err
                end

                self.state = STATE_EOF
                return 'body_end', nil
            else
                local length = tonumber(data, 16)
                remaining = length
            end
        end
    end

    if remaining == 0 then
        self.state = STATE_EOF
        return 'body_end', nil
    end

    if remaining ~= nil and remaining < chunk_size then
        chunk_size = remaining
    end

    local chunk, err, partial = sock:receive(chunk_size)

    local data = ""
    if not err then
        if chunk then
            data = chunk
        end
    elseif err == "closed" then
        self.state = STATE_EOF
        if partial then
            chunk_size = #partial
            if remaining and remaining - chunk_size ~= 0 then
                return nil, partial, err
            end

            data = partial
        else
            return 'body_end', nil
        end
    else
        return nil, nil, err
    end

    if remaining ~= nil then
        self.remaining = remaining - chunk_size
        self.total_size = self.total_size + chunk_size

        if self.maxsize and self.total_size > self.maxsize then
            return nil, nil, "exceeds maxsize"
        end
    end

    return 'body', data
end


local function read_header_part(self)
    local read_line = self.read_line

    local line, err = read_line()
    if not line then
        return nil, nil, err
    end

    if line == "" then
        if self.chunked then
            self.remaining = nil
        end

        self.state = STATE_READING_BODY
        return 'header_end', nil
    end

    local m, err = ngx_re_match(line, [[^(.+?):\s*(.+)]], "jo")
    if not m then
        return 'header', line
    end

    local name, value = m[1], m[2]

    local vname = lower(name)
    if vname == "content-length" then
        self.remaining = tonumber(value)
    end

    if vname == "transfer-encoding" and value ~= "identity" then
        self.chunked = true
    end

    if vname == "connection" and value == "close" then
        self.keepalive = value ~= "close"
    end

    return 'header', { normalize_header(name), value, line }
end


local function read_statusline(self)
    local sock = self.sock
    if self.read_line == nil then
        local rl, err = sock:receiveuntil("\n")
        if not rl then
            return nil, nil, err
        end
        self.read_line = function()
            --[[
            The line terminator for message-header fields is the sequence CRLF.
            However, we recommend that applications, when parsing such headers,
            recognize a single LF as a line terminator and ignore the leading
            CR.
            REF: http://stackoverflow.com/questions/5757290/http-header-line-break-style
            --]]
            local data, err = rl()
            if data and data:sub(-1) == "\r" then
                data = data:sub(1, -2)
            end

            return data, err
        end
    end

    local read_line = self.read_line

    local line, err = read_line()
    if not line then
        return nil, nil, err
    end

    local version, status = match(line, "HTTP/(%d*%.%d*) (%d%d%d)")
    if not version or not status then
        -- return nil, nil, "not match statusline"
        return nil, nil, line
    end

    version = tonumber(version) * 10
    if version < 11 then
        self.keepalive = false
    end

    self.status_code = tonumber(status)

    if self.status_code == 100 then
        local ok, err = discard_line(self)
        if not ok then
            return nil, nil, err
        end

        self.state = STATE_BEGIN
    else
        self.state = STATE_READING_HEADER
    end

    return 'statusline', status
end


-- local ok, err = _M:set_keepalive(...)
function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    self._eof = true

    if self.keepalive then
        return sock:setkeepalive(...)
    end

    return sock:close()
end


-- local times, err = _M:get_reused_times()
function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


-- local ok, err = _M:close()
function _M.close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    self._eof = true

    return sock:close()
end


local function read_eof(self)
    if not self.is_req_socket then
        self:set_keepalive()
    end
    return 'eof', nil
end


-- local typ, res, err = _M:read(chunk_size?)
function _M.read(self, chunk_size)
    local sock = self.sock
    if not sock then
        return nil, nil, "not initialized"
    end

    if chunk_size then
        self.chunk_size = chunk_size
    end

    if self.state == STATE_NOT_READY then
        return nil, nil, "not ready"
    end

    if self.read_timeout then
        sock:settimeout(self.read_timeout)
    end

    local handler = state_handlers[self.state]
    if handler then
        return handler(self)
    end

    return nil, nil, "bad state: " .. self.state
end


-- local eof = _M:eof()
function _M.eof(self)
    return self._eof
end


state_handlers = {
    read_statusline,
    read_header_part,
    read_body_part,
    read_eof
}


-- local res, err = _M:read_response(callback?)
function _M.read_response(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local callback = ...
    if type(callback) ~= "table" then
        callback = {}
    end

    local status
    local headers = {}
    local chunks = {}

    while not self._eof do
        local typ, res, err = self:read()
        if not typ then
            return nil, err
        end

        if typ == 'statusline' then
            status = tonumber(res)
        end

        if typ == 'header' then
            if type(res) == "table" then
                local key = res[1]
                if headers[key] then
                    if type(headers[key]) ~= "table" then
                        headers[key] = { headers[key] }
                    end
                    insert(headers[key], tostring(res[2]))
                else
                    headers[key] = tostring(res[2])
                end
            end
        end

        if typ == 'header_end' then
            if callback.header_filter then
                local rc = callback.header_filter(status, headers)
                if rc then break end
            end
        end

        if typ == 'body' then
            if callback.body_filter then
                local rc = callback.body_filter(res)
                if rc then break end
            else
                insert(chunks, res)
            end
        end

        if typ == 'eof' then
            break
        end
    end

    return { status = status, headers = headers, body = concat(chunks) }
end


-- local ok, err = _M:send_request(opts?)
function _M.send_request(self, opts)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if opts.send_timeout then
        sock:settimeout(opts.send_timeout)
    end

    if opts.read_timeout then
        self.read_timeout = opts.read_timeout
    end

    if opts.version and not HTTP[opts.version] then
        return nil, "unknown HTTP version"
    else
        opts.version = opts.version or 11
    end

    local req, headers = req_header(self, opts)
    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    if type(opts.body) == "string" then
        local bytes, err = sock:send(opts.body)
        if not bytes then
            return nil, err
        end
    elseif type(opts.body) == "function" then
        local chunked = headers["Transfer-Encoding"] == "chunked"
        repeat
            local chunk, err = opts.body()
            if chunk then
                local size = #chunk
                if chunked and size > 0 then
                    chunk = concat({
                            format("%X", size), "\r\n",
                            chunk, "\r\n",
                    })
                end
                local bytes, err = sock:send(chunk)
                if not bytes then
                    return nil, err
                end
            elseif err then
                return nil, err
            end
        until not chunk
        if chunked then
            local bytes, err = sock:send("0\r\n\r\n")
            if not bytes then
                return nil ,err
            end
        end
    end

    self.maxsize = opts.maxsize
    self.state = STATE_BEGIN

    return 1
end


local function get_body_reader(self)
    return function (chunk_size)
        local typ, res, err = self:read(chunk_size)
        if not typ then
            return nil, err
        end

        if typ == 'body' then
            return res
        end

        if typ == 'body_end' then
            read_eof(self)
        end

        return
    end
end


local function redirect(self, url, opts)
    if sub(url, 1, 1) == "/" then
        url = ("http://%s:%s%s"):format(gsub(self.host, [[:.+$]], ""), self.port, url)
    end

    local parsed_uri, _ = self:parse_uri(url)
    if not parsed_uri then
        return nil, "invalid redirect url"
    end

    local scheme, host, port, path, args = unpack(parsed_uri)
    opts.path = path
    opts.query = args

    if scheme == "https" and host ~= opts.req_host then
        opts.ssl_enable = true
    else
        opts.ssl_enable = false
    end

    if type(opts.is_valid_addr) == "function" then
        local valid, format = opts.is_valid_addr(host)
        if not valid then
            return nil, "invalid redirect host"
        end

        -- 0 IP_FORMAT
        -- 1 DOMAIN_FORMAT
        if format == 1 then
            opts.headers["Host"] = host
        end
    end

    if opts.stream then
        local res, err = self:read_response()
        if not res then
            return nil, err
        end
    end

    init(self)

    opts.redirect_follow_num = opts.redirect_follow_num - 1
    if opts.redirect_follow_num <= 0 then
        opts.allow_redirects = false
    end

    if host == opts.req_host then
        host = gsub(self.host, [[:.+$]], "")
        port = self.port
    end

    return self:request(host, port, opts)
end


-- local res, err = _M:request(opts?)
-- local res, err = _M:request(host, port, opts?)
-- local res, err = _M:request("unix:/path/to/unix-domain.socket", opts?)
function _M.request(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local arguments = {...}
    local n = #arguments
    if n > 3 then
        return nil, "expecting 0, 1, 2, or 3 arguments, but seen " ..
            tostring(n)
    end

    local opts = {}
    if type(arguments[n]) == "table" then
        opts = arguments[n]
        arguments[n] = nil
    end

    if not opts.headers then
        opts.headers = {}
    end

    if n > 0 and arguments[1] then
        local rc, err = self:connect(unpack(arguments))
        if not rc then
            return nil, err
        end

        if self.port then
            if opts.ssl_enable and self.port ~= 443 then
                self.host = self.host .. ":" .. tostring(self.port)
            elseif not opts.ssl_enable and self.port ~= 80 then
                self.host = self.host .. ":" .. tostring(self.port)
            end
        end

        if opts.ssl_enable then
            local server_name = opts.headers["Host"] or self.host
            if opts.ssl_server_name then
                server_name = opts.ssl_server_name
            end

            local ssl_verify = true
            if opts.ssl_verify == false then
                ssl_verify = false
            end

            local ok, err = self:ssl_handshake(nil, server_name, ssl_verify)
            if not ok then
                return nil, err
            end
         end
    end

    local prev = self.previous.pipe
    if self.previous.body_reader then
        opts.body = self.previous.body_reader
    end

    local ok, err = self:send_request(opts)
    if not ok then
        if prev then prev:close() end
        return nil, err
    end

    local res, err = self:read_response{
        header_filter = function (status, headers)
            return opts.stream
        end
    }

    if res and opts.stream then
        res.body_reader = get_body_reader(self)

        local size = tonumber(res.headers["Content-Length"]) or -1 -- -1:chunked
        local pipe, err = self:new(self.chunk_size)
        if not pipe then
            return nil, err
        end

        pipe.previous.pipe = self
        pipe.previous.content_length = size
        pipe.previous.body_reader = res.body_reader

        res.pipe = pipe
    end

    if res and opts.allow_redirects
        and (self.method == "GET" or self.method == "HEAD")
        and (res.status == 301 or res.status == 302)
    then
        local url = res.headers["Location"]
        if type(url) == "string" then
            local _res, _err = redirect(self, url, opts)
            if _res then
                return _res, _err
            end
        end
    end

    return res, err
end


-- local res, err = _M:request_uri(uri, opts?)
function _M.request_uri(self, uri, opts)
    if not opts then
        opts = {}
    end

    local parsed_uri, err = self:parse_uri(uri)
    if not parsed_uri then
        return nil, err
    end

    local scheme, host, port, path, args = unpack(parsed_uri)
    if not opts.path then
        opts.path = path
    end

    if not opts.query then
        opts.query = args
    end

    if scheme == "https" then
        opts.ssl_enable = true
    end

    return self:request(host, port, opts)
end


-- local reader, err = _M:get_client_body_reader(chunk_size?)
function _M.get_client_body_reader(self, chunk_size)
    local sock, err = ngx_req_socket()

    if not sock then
        return nil, err
    end

    local hp = self:new(chunk_size, sock)
    local headers = ngx_req_get_headers()

    hp.chunked = headers["Transfer-Encoding"] == "chunked"
    if not hp.chunked then
        hp.remaining = tonumber(headers["Content-Length"])
    end

    hp.state = STATE_READING_BODY
    hp.is_req_socket = 1

    return get_body_reader(hp)
end


return _M
