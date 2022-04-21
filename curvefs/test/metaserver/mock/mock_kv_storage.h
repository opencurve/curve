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
 * Date: Tuesday Apr 26 15:01:46 CST 2022
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_METASERVER_MOCK_MOCK_KV_STORAGE_H_
#define CURVEFS_TEST_METASERVER_MOCK_MOCK_KV_STORAGE_H_

#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/gmock.h>

#include <memory>
#include <string>

#include "curvefs/src/metaserver/storage/status.h"
#include "curvefs/src/metaserver/storage/storage.h"

namespace curvefs {
namespace metaserver {
namespace storage {

class MockKVStorage : public KVStorage {
 public:
    MOCK_METHOD3(HGet,
                 Status(const std::string&, const std::string&, ValueType*));

    MOCK_METHOD3(HSet,
                 Status(const std::string&,
                        const std::string&,
                        const ValueType&));

    MOCK_METHOD2(HDel, Status(const std::string&, const std::string&));

    MOCK_METHOD1(HGetAll, std::shared_ptr<Iterator>(const std::string&));

    MOCK_METHOD1(HSize, size_t(const std::string&));

    MOCK_METHOD1(HClear, Status(const std::string&));

    MOCK_METHOD3(SGet,
                 Status(const std::string&, const std::string&, ValueType*));

    MOCK_METHOD3(SSet,
                 Status(const std::string&,
                        const std::string&,
                        const ValueType&));

    MOCK_METHOD2(SDel, Status(const std::string&, const std::string&));

    MOCK_METHOD2(SSeek,
                 std::shared_ptr<Iterator>(const std::string&,
                                           const std::string&));

    MOCK_METHOD1(SGetAll, std::shared_ptr<Iterator>(const std::string&));

    MOCK_METHOD1(SSize, size_t(const std::string&));

    MOCK_METHOD1(SClear, Status(const std::string&));

    MOCK_METHOD0(Type, STORAGE_TYPE());

    MOCK_METHOD0(Open, bool());

    MOCK_METHOD0(Close, bool());

    MOCK_METHOD1(GetStatistics, bool(StorageStatistics*));

    MOCK_CONST_METHOD0(GetStorageOptions, StorageOptions());

    MOCK_METHOD0(BeginTransaction, std::shared_ptr<StorageTransaction>());
};

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_TEST_METASERVER_MOCK_MOCK_KV_STORAGE_H_
