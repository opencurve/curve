/*
 *  Copyright (c) 2022 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2022-10-08
 * Author: YangFan (fansehep)
 */

#include "curvefs/src/client/kvclient/memcache_client.h"

#include <libmemcached-1.0/exist.h>
#include <libmemcached-1.0/types/return.h>

#include "src/client/client_metric.h"

namespace curvefs {
namespace client {

thread_local memcached_st* tcli = nullptr;

bool MemCachedClient::Exist(const std::string& key) {
    // https://awesomized.github.io/libmemcached/libmemcached/memcached_exist.html?highlight=exist#_CPPv415memcached_existP12memcached_stPcP6size_t
    memcached_return_t ue;
    size_t value_length = 0;
    uint64_t start = butil::cpuwide_time_us();
    if (nullptr == tcli) {
        LOG(ERROR) << "create tcli";
        tcli = memcached_clone(nullptr, client_);
    }
    ue = memcached_exist(tcli, key.c_str(), key.length());
    if (ue == MEMCACHED_SUCCESS) {
        curve::client::CollectMetrics(&metric_->exist, 0,
                                      butil::cpuwide_time_us() - start);
        return true;
    }

    if (ue == MEMCACHED_NOTFOUND) {
        curve::client::CollectMetrics(&metric_->exist, 0,
                                      butil::cpuwide_time_us() - start);
    } else {
        std::string errorlog = ResError(ue);
        LOG(ERROR) << "Exist key = " << key << " error = " << errorlog;
        metric_->exist.eps.count << 1;
    }
    memcached_free(tcli);
    tcli = nullptr;
    return false;
}

}  // namespace client
}  // namespace curvefs
