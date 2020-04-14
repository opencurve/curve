local httproxy = require "modules.httproxy"

return httproxy.exec(ngx.var.request_method, ngx.var.request_uri, "snapshot");
