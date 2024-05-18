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
 * Created Date: 2022-09-22
 * Author: YangFan (fansehep)
 */
#ifndef CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_H_
#define CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_H_

#include <libmemcached-1.0/types/return.h>

#include <cstdint>
#include <string>

namespace curvefs {

namespace client {

/**
 * Single client to kv interface.
 */

class KVClient {
 public:
    KVClient() = default;
    ~KVClient() = default;

    virtual void Init() {}

    virtual void UnInit() {}

    /**
     * @param: errorlog: if error occurred, the errorlog will take
     *         the error info and log.
     * @return: success return true, else return false;
     */
    virtual bool Set(const std::string &key, const char *value,
                     const uint64_t value_len, std::string *errorlog) = 0;

    virtual bool Get(const std::string& key, char* value, uint64_t offset,
                     uint64_t length, std::string* errorlog,
                     uint64_t* actLength, memcached_return_t* retCod) = 0;

    virtual bool Exist(const std::string& key) = 0;
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_H_
