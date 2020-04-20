-- Copyright (c) Netease

local uuid     = require 'resty.uuid'
local checkups = require "resty.checkups.api"

local config = config

checkups.prepare_checker(config)
checkups.create_checker()

uuid.seed()
