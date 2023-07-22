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

#ifndef SRC_MDS_AUTH_AUTH_STORAGE_ETCD_H_
#define SRC_MDS_AUTH_AUTH_STORAGE_ETCD_H_

#include <string>
#include <unordered_map>
#include <memory>
#include "src/mds/auth/auth_storage.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/mds/auth/auth_storage_codec.h"

namespace curve {
namespace mds {
namespace auth {

using ::curve::kvstorage::KVStorageClient;

class AuthStorageEtcd : public AuthStorage {
 public:
    explicit AuthStorageEtcd(std::shared_ptr<KVStorageClient> client)
        : client_(client) {}

    bool LoadKeys(std::unordered_map<std::string, Key> *keyMap) override;

    bool StorageKey(const Key &key) override;

    bool DeleteKey(const std::string &keyId) override;

 private:
    // underlying storage media
    std::shared_ptr<KVStorageClient> client_;
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_AUTH_AUTH_STORAGE_ETCD_H_