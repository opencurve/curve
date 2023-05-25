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
 * Project: Curve
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/defer_sync.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class DeferSyncTest : public ::testing::Test {
 protected:
    void SetUp() override {
        metaClient_ = std::make_shared<MockMetaServerClient>();
    }

    void TearDown() override {
        metaClient_ = nullptr;
    }

 protected:
    std::shared_ptr<MockMetaServerClient> metaClient_;
};

TEST_F(DeferSyncTest, Basic) {
    auto builder = DeferSyncBuilder();
    auto deferSync = builder.SetOption([&](DeferSyncOption* option){
        option->delay = 3;
    }).Build();
    deferSync->Start();

    auto inode = MkInode(100, InodeOption().metaClient(metaClient_));
    // EXPECT_CALL_INDOE_SYNC_TIMES(*metaClient_, 100 /* ino */, 1 /* times */);
    inode->SetLength(100);  // make inode ditry to trigger sync
    deferSync->Push(inode);
    deferSync->Stop();
}

TEST_F(DeferSyncTest, Dirty) {
    auto builder = DeferSyncBuilder();
    auto deferSync = builder.SetOption([&](DeferSyncOption* option){
        option->delay = 3;
    }).Build();
    deferSync->Start();

    auto inode = MkInode(100, InodeOption().metaClient(metaClient_));
    EXPECT_CALL_INDOE_SYNC_TIMES(*metaClient_, 100 /* ino */, 0 /* times */);
    deferSync->Push(inode);
    deferSync->Stop();
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
