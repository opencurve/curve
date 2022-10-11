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
 * Created Date: 2022-09-25
 * Author: YangFan (fansehep)
 */

#include "curvefs/src/client/kvclient/memcache_client.h"

namespace curvefs {
namespace client {

/**
 * only the threadpool will operate the kvclient,
 * for threadsafe and fast, we can make every thread has a client.
 */
thread_local memcached_st *tcli = nullptr;

bool MemCachedClient::SetImp(const std::string &key, const char *value,
                             const int value_len, std::string *errorlog) {
    if (nullptr == tcli) {
        tcli = memcached_clone(nullptr, client_);
    }
    auto res =
        memcached_set(tcli, key.c_str(), key.length(), value, value_len, 0, 0);
    if (MEMCACHED_SUCCESS == res) {
        return true;
    }
    *errorlog = ResError(res);
    return false;
}

bool MemCachedClient::GetImp(const std::string &key, std::string *value,
                             std::string *errorlog) {
    if (nullptr == tcli) {
        // multi thread use a memcached_st* client is unsafe.
        // should clone it or use memcached_st_pool.
        tcli = memcached_clone(nullptr, client_);
    }
    uint32_t flags = 0;
    size_t value_length = 0;
    memcached_return_t ue;
    char *res = memcached_get(tcli, key.c_str(), key.length(), &value_length,
                              &flags, &ue);
    if (res != nullptr && value->empty()) {
        value->reserve(value_length + 1);
        value->assign(res, res + value_length + 1);
        value->resize(value_length);
        free(res);
        return true;
    }
    *errorlog = ResError(ue);
    return false;
}
}  //  namespace client
}  //  namespace curvefs
