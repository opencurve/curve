
/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Mon Aug  2 19:48:11 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_TEST_MDS_MOCK_MOCK_FS_STROAGE_H_
#define CURVEFS_TEST_MDS_MOCK_MOCK_FS_STROAGE_H_

#include <gmock/gmock.h>

#include <string>
#include <vector>

#include "curvefs/src/mds/fs_storage.h"

namespace curvefs {
namespace mds {

class MockMemoryFsStorage : public MemoryFsStorage {
 public:
    MOCK_METHOD2(Get, FSStatusCode(const std::string&, FsInfoWrapper*));
};

class MockFsStorage : public FsStorage {
 public:
    MockFsStorage() = default;
    ~MockFsStorage() = default;

    MOCK_METHOD0(Init, bool());
    MOCK_METHOD0(Uninit, void());
    MOCK_METHOD2(Get, FSStatusCode(uint64_t, FsInfoWrapper*));
    MOCK_METHOD2(Get, FSStatusCode(const std::string&, FsInfoWrapper*));
    MOCK_METHOD1(Insert, FSStatusCode(const FsInfoWrapper&));
    MOCK_METHOD1(Update, FSStatusCode(const FsInfoWrapper&));
    MOCK_METHOD1(Delete, FSStatusCode(const std::string&));
    MOCK_METHOD2(Rename, FSStatusCode(const FsInfoWrapper&,
                                      const FsInfoWrapper&));
    MOCK_METHOD1(Exist, bool(uint64_t));
    MOCK_METHOD1(Exist, bool(const std::string&));
    MOCK_METHOD0(NextFsId, uint64_t());
    MOCK_METHOD1(GetAll, void(std::vector<FsInfoWrapper>* fsInfoVec));
    MOCK_METHOD2(SetFsUsage, FSStatusCode(const std::string&, const FsUsage&));
    MOCK_METHOD3(GetFsUsage,
                 FSStatusCode(const std::string&, FsUsage*, bool fromCache));
    MOCK_METHOD1(DeleteFsUsage, FSStatusCode(const std::string&));
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_TEST_MDS_MOCK_MOCK_FS_STROAGE_H_
