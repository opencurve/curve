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

#ifndef SRC_MDS_AUTH_AUTH_STORAGE_CODEC_H_
#define SRC_MDS_AUTH_AUTH_STORAGE_CODEC_H_

#include <string>

#include "src/common/namespace_define.h"
#include "proto/auth.pb.h"

namespace curve {
namespace mds {
namespace auth {

using curve::common::AUTH_KEY_PREFIX;
using curve::common::AUTH_KEY_END;
using curve::common::AUTH_PTRFIX_LENGTH;

class AuthStorageCodec {
 public:
    // there are three types of function here:
    // Encode__Key: attach item id to item prefix
    // Encode__Data: convert data structure to a string
    // Decode__Data: convert a string to data structure
    static std::string EncodeAuthKey(const std::string &keyId) {
        return AUTH_KEY_PREFIX + keyId;
    }

    static bool EncodeAuthData(const Key &data, std::string *value) {
        return data.SerializeToString(value);
    }

    static bool DecodeAuthData(const std::string &value, Key *data) {
        return data->ParseFromString(value);
    }
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_AUTH_AUTH_STORAGE_CODEC_H_