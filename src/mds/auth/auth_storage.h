/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-05-24
 * Author: wanghai (SeanHai)
 */

#ifndef SRC_MDS_AUTH_AUTH_STORAGE_H_
#define SRC_MDS_AUTH_AUTH_STORAGE_H_

#include <string>
#include <unordered_map>
#include "proto/auth.pb.h"

namespace curve {
namespace mds {
namespace auth {

class AuthStorage {
 public:
    AuthStorage() = default;
    virtual ~AuthStorage() = default;

    virtual bool LoadKeys(std::unordered_map<std::string, Key> *keyMap) = 0;
    virtual bool StorageKey(const Key &key) = 0;
    virtual bool DeleteKey(const std::string &keyId) = 0;
};

}  // namespace auth
}  // namespace mds
}  // namespace curve



#endif  // SRC_MDS_AUTH_AUTH_STORAGE_H_
