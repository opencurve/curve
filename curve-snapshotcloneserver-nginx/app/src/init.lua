-- Copyright (c) Netease

require "resty.core"
local checkups = require "resty.checkups.api"
config = require "config"

config.global.version = "v0.0.1"
checkups.init(config)
