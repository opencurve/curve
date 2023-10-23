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

#include <glog/logging.h>
#include <utility>
#include <string>
#include <vector>
#include "src/mds/auth/auth_storage_etcd.h"

namespace curve {
namespace mds {
namespace auth {

bool AuthStorageEtcd::LoadKeys(std::unordered_map<std::string, Key> *keyMap) {
    std::vector<std::string> out;
    keyMap->clear();
    int errCode = client_->List(AUTH_KEY_PREFIX, AUTH_KEY_END, &out);
    if (errCode == EtcdErrCode::EtcdKeyNotExist) {
        return true;
    }
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "etcd list err:" << errCode;
        return false;
    }
    for (const auto &item : out) {
        Key key;
        bool ecode = AuthStorageCodec::DecodeAuthData(item, &key);
        if (!ecode) {
            LOG(ERROR) << "Decode auth key err";
            return false;
        }
        auto id = key.id();
        auto ret = keyMap->emplace(id, std::move(key));
        if (!ret.second) {
            LOG(ERROR) << "Load auth keys failed, key id duplicated, id = "
                       << id;
            return false;
        }
    }
    return true;
}

bool AuthStorageEtcd::StorageKey(const Key &key) {
    std::string encodeKey = AuthStorageCodec::EncodeAuthKey(key.id());
    std::string value;
    bool ret = AuthStorageCodec::EncodeAuthData(key, &value);
    if (!ret) {
        LOG(ERROR) << "Encode auth key failed"
                   << ", key id = " << key.id();
        return false;
    }
    int errCode = client_->Put(encodeKey, value);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Put auth key into etcd failed"
                   << ", errcode = " << errCode
                   << ", auth key id = " << key.id();
        return false;
    }
    return true;
}

bool AuthStorageEtcd::DeleteKey(const std::string &keyId) {
    std::string encodeKey = AuthStorageCodec::EncodeAuthKey(keyId);
    int errCode = client_->Delete(encodeKey);
    if (errCode != EtcdErrCode::EtcdOK) {
        LOG(ERROR) << "Delete auth key failed"
                   << ", errcode = " << errCode
                   << ", auth key id = " << keyId;
        return false;
    }
    return true;
}

}  // namespace auth
}  // namespace mds
}  // namespace curve
