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

#include <string>

namespace curvefs {

namespace client {

/**
 * Single client to kv interface.
 * Normally, get it from the pool.
 */
template <typename T>
class KvClient {
 private:
    T* Imp() { return static_cast<T*>(this); }

 public:
    KvClient() = default;
    bool Init() { return Imp()->InitImp(); }
    bool Init(const std::string& config) { return Imp()->InitImp(config); }

    void UnInit() { return Imp()->UnInitImp(); }

    /**
     * @param: errorlog: if error occurred, the errorlog will take
     *         the error info and log.
     * @return: success return true, else return false;
     */
    bool Set(const std::string& key,
             const char* value,
             const int value_len,
             std::string* errorlog) {
        return Imp()->SetImp(key, value, value_len, errorlog);
    }

    /**
     * @param: errorlog: if return is false the errorlog
     *         will take the info.
     * @return: success return the value and true.
     */
    bool Get(const std::string& key,
             std::string* value,
             std::string* errorlog) {
        return Imp()->GetImp(key, value, errorlog);
    }
};

}  // namespace client
}  // namespace curvefs
#endif  // CURVEFS_SRC_CLIENT_KVCLIENT_KVCLIENT_H_
