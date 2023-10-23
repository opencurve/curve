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
 * Created Date: 2023-06-25
 * Author: wanghai (SeanHai)
 */

#ifndef TEST_MDS_AUTH_MOCK_AUTH_STORAGE_H_
#define TEST_MDS_AUTH_MOCK_AUTH_STORAGE_H_

#include <gmock/gmock.h>
#include <string>
#include "src/mds/auth/auth_storage.h"

namespace curve {
namespace mds {
namespace auth {

class MockAuthStorage : public AuthStorage {
 public:
    MOCK_METHOD(bool, LoadKeys,
        ((std::unordered_map<std::string, Key> *keyMap)), (override));
    MOCK_METHOD(bool, StorageKey, (const Key &key), (override));
    MOCK_METHOD(bool, DeleteKey, (const std::string &keyId), (override));
};

}  // namespace auth
}  // namespace mds
}  // namespace curve

#endif  // TEST_MDS_AUTH_MOCK_AUTH_STORAGE_H_
