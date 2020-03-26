-- Copyright (c) Netease, xiaojing tang

local base     = require "resty.core.base"
local debug    = require "debug"
local argutils = require "resty.argutils"

local _M = {}

local next  = next
local sub   = string.sub
local gsub  = string.gsub
local len   = string.len
local find  = string.find
local lower = string.lower

local registry     = debug.getregistry()
local ngx_match    = ngx.re.match
local table_insert = table.insert
local set_uri_args = ngx.req.set_uri_args
local delete_args  = argutils.delete_args
local escape_uri   = ngx.escape_uri
local tab_insert   = table.insert
local tab_concat   = table.concat
local set_header   = ngx.req.set_header

_M.is_str = function(s) return type(s) == "string" end
_M.is_num = function(n) return type(n) == "number" end
_M.is_tab = function(t) return type(t) == "table" end
_M.is_func = function(f) return type(f) == "function" end
_M.null = function(e) return e == nil or e == ngx.null end


local inner_addr = {
    ["10"]  = {0, 255},    -- 10.0.0.0-10.255.255.255
    ["172"] = {16, 31},    -- 172.16.0.0-172.31.255.255
    ["192"] = {168, 168},  -- 192.168.0.0-192.168.255.255
}


function _M.unormalize_header(s)
    return gsub(lower(s), "-", "_")
end


function _M.is_empty_table(t)
    return next( t ) == nil
end


function _M.escape_path(path)
    return gsub(path, "([^/]+)", function (s) return escape_uri(s) end)
end


function _M.table_empty(t)
    return next(t) == nil
end


function _M.table_in(t, key)
    for k, v in ipairs(t) do
        if key == v then
            return true
        end
    end

    return false
end


function _M.get_ip_port(host)
    if not host then
        return nil, nil
    end

    local pos = find(host, ":", 1)
    if not pos then
        return host, 80
    end

    local ip = sub(host, 1, pos - 1)
    local port = tonumber(sub(host, pos + 1))

    return ip, port
end


function _M.is_ip_format(host)
    if not host == nil then
        return false
    end

    local pos = find(host, ":", 1)
    if pos then
        host = sub(host, 1, pos - 1)
    end

    local m, err = ngx_match(host, [[^\d+\.\d+\.\d+\.\d+$]], "ijso")
    if m and m[0] then
        return true
    else
        return false
    end
end


function _M.log_request_headers(msg)
    local h = ngx.req.get_headers()
    local t = {}
    for k, v in pairs(h) do
        tab_insert(t, ("%s=%s"):format(k, v))
    end
    ngx.log(ngx.ERR, msg, ", request headers:", tab_concat(t, " "))
end


function _M.gen_resource(bucket, object)
    local resource = ""
    if bucket and bucket ~= "" then
        resource = resource .. "/" .. bucket
    end

    if object and object ~= "" then
        resource = resource .. "/" .. object
    end

    return resource
end


function _M.split_string(s, p)
    local res = {}
    gsub(s, '[^'..p..']+', function(w) table_insert(res, w) end)
    return res
end


function _M.is_inner_ip(ip)
    local sub = _M.split_string(ip, ".")

    if sub and #sub == 4 and sub[1] and sub[2] then
        local range = inner_addr[sub[1]]
        local int_sub2 = tonumber(sub[2]) or 0
        if range and int_sub2 >= range[1] and int_sub2 <= range[2] then
            return true
        end
    end

    return false
end


function _M.set_isp(ip, special_ip)
    -- from multi line, do not set isp
    local isp = ngx.var.http_x_nos_isp
    if isp then
        set_header("x-nos-isp", nil)
        set_header("x-inner-isp", isp)
        return
    end

    if _M.is_inner_ip(ip) then
        set_header("x-inner-isp", "inner")
    elseif special_ip and special_ip[ip] then
        set_header("x-inner-isp", "bp")
    else
        set_header("x-inner-isp", "outer")
    end
end


function _M.delete_uri_args(del_args)
    local args = ngx.var.args
    if not args or not del_args then
        return
    end

    local opts = { delete_bool_value = true }
    for _, k in ipairs(del_args) do
        args = delete_args(args, "^" .. k .. "$", ".*", opts)
    end

    set_uri_args(args)
end


function _M.table_dup(ori_tab)
    if type(ori_tab) ~= "table" then
        return ori_tab
    end
    local new_tab = {}
    for k, v in pairs(ori_tab) do
        if type(v) == "table" then
            new_tab[k] = _M.table_dup(v)
        else
            new_tab[k] = v
        end
    end
    return new_tab
end


-- stash the old ngx.ctx, create a new anchor.
-- Note: stash_ngx_ctx and apply_ngx_ctx need to be called in pairs, otherwise
-- the overhead memory leak will happens!
function _M.stash_ngx_ctx()
    local ctxs = registry.ngx_lua_ctx_tables
    local ctx_ref = base.ref_in_table(ctxs, ngx.ctx)
    ngx.var.ctx_ref = tostring(ctx_ref)
end


-- restore the old ngx.ctx with the anchor fetched from stash_ngx_ctx, replace
-- current the ngx.ctx, you need to call it early after Nginx internal redirect
-- happens.
-- Note: stash_ngx_ctx and apply_ngx_ctx need to be called in pairs, otherwise
-- the overhead memory leak will happens!
function _M.apply_ngx_ctx()
    local ctx_ref = tonumber(ngx.var.ctx_ref)
    if not ctx_ref then
        return
    end

    local ctxs = registry.ngx_lua_ctx_tables
    local origin_ngx_ctx = ctxs[ctx_ref]
    ngx.ctx = origin_ngx_ctx

    --- FIXME unref the ctx_ref for avoiding memory leak
    local FREE_LIST_REF = 0
    ctxs[ctx_ref] = ctxs[FREE_LIST_REF]
    ctxs[FREE_LIST_REF] = ctx_ref
    ngx.var.ctx_ref = ""
end


return _M
