/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 18-10-7
 * Author: wudemiao
 */

#ifndef TEST_CLIENT_MOCK_META_CACHE_H_
#define TEST_CLIENT_MOCK_META_CACHE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/client/client_common.h"
#include "src/client/metacache.h"
#include "src/client/file_instance.h"
#include "src/client/client_metric.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::Invoke;

class FakeMetaCache : public MetaCache {
 public:
    FakeMetaCache() : MetaCache() {}

    int GetLeader(LogicPoolID logicPoolId,
                  CopysetID copysetId,
                  ChunkServerID *serverId,
                  butil::EndPoint *serverAddr,
                  bool refresh = false,
                  FileMetric* fm = nullptr) {
        *serverId = 10000;
        butil::str2endpoint("127.0.0.1:9109", serverAddr);
        return 0;
    }
    int UpdateLeader(LogicPoolID logicPoolId,
                     CopysetID copysetId,
                     const butil::EndPoint &leaderAddr) {
        return 0;
    }
};

class MockMetaCache : public MetaCache {
 public:
    MockMetaCache() : MetaCache() {}

    MOCK_METHOD6(GetLeader, int(LogicPoolID, CopysetID, ChunkServerID*,
                                butil::EndPoint *, bool, FileMetric*));
    MOCK_METHOD3(UpdateLeader, int(LogicPoolID, CopysetID,
                                   const butil::EndPoint &));

    MOCK_METHOD2(GetChunkInfoByIndex,
                 MetaCacheErrorType(ChunkIndex, ChunkIDInfo *));

    void DelegateToFake() {
        ON_CALL(*this, GetLeader(_, _, _, _, _, _))
            .WillByDefault(Invoke(&fakeMetaCache_, &FakeMetaCache::GetLeader));
        ON_CALL(*this, UpdateLeader(_, _, _))
            .WillByDefault(Invoke(&fakeMetaCache_,
                                  &FakeMetaCache::UpdateLeader));
    }

 private:
    FakeMetaCache fakeMetaCache_;
};

}   // namespace client
}   // namespace curve

#endif  // TEST_CLIENT_MOCK_META_CACHE_H_
