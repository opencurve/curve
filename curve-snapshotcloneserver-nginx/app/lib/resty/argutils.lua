-- Copyright (C) 2015 Jingli Chen (Wine93), UPYUN Inc.

local ngx_find     = ngx.re.find
local escape_uri   = ngx.escape_uri

local sort   = table.sort
local insert = table.insert
local concat = table.concat
local gsub   = string.gsub

local _M = {
    _VERSION = "0.04"
}


local function split(s, p)
    local res = {}
    gsub(s, "([^" .. p .. "]+)", function(w)
            if #w > 0 then insert(res, w) end
    end)
    return res
end


local function match(s, p, ci)
    local opts = "jo"
    if ci == true then opts = "ijo" end
    return ngx_find(s, p, opts)
end


local function parse_arg(arg)
    if type(arg) ~= "string" or #arg < 1 then
        return nil, nil, "invalid argument"
    end

    local from = arg:find("=")
    if from then
        local key = arg:sub(1, from - 1)
        local val = arg:sub(from + 1)
        if #key > 0 then
            return key, val
        end
    else  -- only key, like ?a
        return arg, true
    end
end


local function insert_arg(t, k, v)
    if not t[k] then
        t[k] = v
        return
    end

    if type(t[k]) == "table" then
        insert(t[k], v)
    else
        t[k] = { t[k], v }
    end
end


function _M.LESS(a, b)
    if a.key == b.key then
        if tostring(a.val) ~= tostring(b.val) then
            if type(a.val) == "boolean" then
                return true
            elseif type(b.val) ~= "boolean"
                and tostring(a.val) < tostring(b.val) then
                return true
            end
        end
    end

    return a.key < b.key
end


function _M.sort_args(args, opts)
    if type(args) ~= "string" then
        return nil, "non-string arguments"
    end

    if type(opts) ~= "table" then opts = {} end
    local order = opts.order or _M.LESS

    if type(order) ~= "function" then
        return nil, "non-function order"
    end

    local sorted_args = {}
    local args_a = split(args, '&')
    for _, arg in ipairs(args_a) do
        local key, val = parse_arg(arg)
        if key then
            insert(sorted_args, {
                key = key, val = val
            })
        end
    end

    -- sort the aguments by order function
    sort(sorted_args, order)

    local res = {}
    for _, arg in ipairs(sorted_args) do
        local key = arg.key
        local val = arg.val
        if type(val) == "boolean" then
            insert(res, key)
        else
            insert(res, key .. "=" .. val)
        end
    end

    return concat(res, '&')
end


function _M.match_args(args, key_pattern, val_pattern, opts)
    if type(args) ~= "string" then
        return nil, "non-string argument"
    end
    if type(key_pattern) ~= "string" then
        return nil, "non-string key regex expression"
    end
    if type(val_pattern) ~= "string" then
        return nil, "non-string val regex expression"
    end

    -- ci == case insensitive
    if type(opts) ~= "table" then opts = {} end
    local key_ci = opts.key_ci
    local val_ci = opts.val_ci
    local match_bool_value = opts.match_bool_value

    local res = {}
    local args_a = split(args, '&')
    for _, arg in ipairs(args_a) do
        local key, val = parse_arg(arg)
        if key then
            if type(val) == "boolean" then
                if match_bool_value
                and match(key, key_pattern, key_ci) then
                    insert_arg(res, key, val)
                end
            elseif type(val) == "string" then
                if match(key, key_pattern, key_ci)
                and match(val, val_pattern, val_ci) then
                    insert_arg(res, key, val)
                end
            end
        end
    end

    return res
end


function _M.delete_args(args, key_pattern, val_pattern, opts)
    if type(args) ~= "string" then
        return nil, "non-string argument"
    end
    if type(key_pattern) ~= "string" then
        return nil, "non-string key regex expression"
    end
    if type(val_pattern) ~= "string" then
        return nil, "non-string val regex expression"
    end

    -- ci == case insensitive
    if type(opts) ~= "table" then opts = {} end
    local key_ci = opts.key_ci
    local val_ci = opts.val_ci
    local delete_bool_value = opts.delete_bool_value

    local res = {}
    local args_a = split(args, '&')
    for _, arg in ipairs(args_a) do
        local key, val = parse_arg(arg)
        if key then
            if type(val) == "boolean" then
                if not delete_bool_value
                or not match(key, key_pattern, key_ci) then
                    insert(res, key)
                end
            elseif type(val) == "string" then
                if not match(key, key_pattern, key_ci)
                or not match(val, val_pattern, val_ci) then
                    insert(res, key .. "=" .. val)
                end
            end
        end
    end

    return concat(res, '&')
end


function _M.escape_args(args)
    local res = {}
    local args_a = split(args, '&')
    for _, arg in ipairs(args_a) do
        local key, val = parse_arg(arg)
        if key then
            key = escape_uri(key)
            if type(val) == "boolean" then
                insert(res, key)
            elseif type(val) == "string" then
                val = escape_uri(val)
                insert(res, key .. "=" .. val)
            end
        end
    end

    return concat(res, '&')
end


function _M.split_args(args)
    local res = {}
    local args_a = split(args, '&')
    for _, arg in ipairs(args_a) do
        local key, val = parse_arg(arg)
        insert(res, {key, val})
    end

    return res
end


return _M
