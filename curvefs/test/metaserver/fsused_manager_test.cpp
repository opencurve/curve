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

#include "curvefs/src/metaserver/fsused_manager.h"

#include <gtest/gtest.h>

#include "curvefs/test/client/mock_metaserver_client.h"

namespace curvefs {
namespace metaserver {

using curvefs::client::rpcclient::MockMetaServerClient;
using testing::_;

class FsUsedManagerTest : public testing::Test {
 protected:
    virtual void SetUp() {
        mockMetaServerClient_ = std::make_shared<MockMetaServerClient>();
        fsUsedManager_ = std::make_shared<FsUsedManager>(mockMetaServerClient_);
    }
    virtual void TearDown() {}

    std::shared_ptr<MockMetaServerClient> mockMetaServerClient_;
    std::shared_ptr<FsUsedManager> fsUsedManager_;
};

TEST_F(FsUsedManagerTest, ApplyFsUsedDeltas) {
    std::unordered_map<uint32_t, FsUsedDelta> applyResult;
    auto add2result = [&applyResult](uint32_t fsid, const FsUsedDelta& delta,
                                     bool fromClient) {
        if (fromClient) {
            return MetaStatusCode::OK;
        }
        if (applyResult.find(fsid) == applyResult.end()) {
            applyResult[fsid] = delta;
        } else {
            applyResult[fsid].set_bytes(applyResult[fsid].bytes() +
                                        delta.bytes());
        }
        return MetaStatusCode::OK;
    };
    EXPECT_CALL(*mockMetaServerClient_, UpdateFsUsed(_, _, _))
        .WillRepeatedly(testing::Invoke(add2result));

    {
        // case 0: no delta
        applyResult.clear();
        fsUsedManager_->ApplyFsUsedDeltas();
        ASSERT_EQ(applyResult.empty(), true);
    }

    {
        // case 1: one delta
        applyResult.clear();
        FsUsedDelta delta1;
        delta1.set_fsid(1);
        delta1.set_bytes(100);
        fsUsedManager_->AddFsUsedDelta(std::move(delta1));
        fsUsedManager_->ApplyFsUsedDeltas();
        ASSERT_EQ(applyResult.size(), 1);
        ASSERT_EQ(applyResult[1].bytes(), 100);
    }

    {
        // case 2: two deltas
        applyResult.clear();
        FsUsedDelta delta1;
        delta1.set_fsid(1);
        delta1.set_bytes(100);
        FsUsedDelta delta2;
        delta2.set_fsid(1);
        delta2.set_bytes(100);
        fsUsedManager_->AddFsUsedDelta(std::move(delta1));
        fsUsedManager_->AddFsUsedDelta(std::move(delta2));
        fsUsedManager_->ApplyFsUsedDeltas();
        ASSERT_EQ(applyResult.size(), 1);
        ASSERT_EQ(applyResult[1].bytes(), 200);
    }

    {
        // case 3: two deltas, different fsid
        applyResult.clear();
        FsUsedDelta delta1;
        delta1.set_fsid(1);
        delta1.set_bytes(100);
        FsUsedDelta delta2;
        delta2.set_fsid(2);
        delta2.set_bytes(100);
        fsUsedManager_->AddFsUsedDelta(std::move(delta1));
        fsUsedManager_->AddFsUsedDelta(std::move(delta2));
        fsUsedManager_->ApplyFsUsedDeltas();
        ASSERT_EQ(applyResult.size(), 2);
        ASSERT_EQ(applyResult[1].bytes(), 100);
        ASSERT_EQ(applyResult[2].bytes(), 100);
    }
}

}  // namespace metaserver
}  // namespace curvefs
