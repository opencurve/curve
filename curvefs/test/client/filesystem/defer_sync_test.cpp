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

class DeferInodesTest : public ::testing::Test {};

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

TEST_F(DeferInodesTest, cto) {
    // CASE 1: cto
    bool cto = true;
    auto deferInodes = std::make_shared<DeferInodes>(cto);
    bool yes = deferInodes->Add(MkInode(100));
    ASSERT_FALSE(yes);
    ASSERT_EQ(deferInodes->Size(), 0);

    // CASE 2: nocto
    cto = false;
    deferInodes = std::make_shared<DeferInodes>(cto);
    yes = deferInodes->Add(MkInode(100));
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 1);
}

TEST_F(DeferInodesTest, Add) {
    auto deferInodes = std::make_shared<DeferInodes>(false);

    // CASE 1: add inode(100) success
    bool yes = deferInodes->Add(MkInode(100, InodeOption().length(1024)));
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 1);

    std::shared_ptr<InodeWrapper> inode;
    yes = deferInodes->Get(100, &inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(inode->GetLength(), 1024);

    // CASE 2: add inode(200) success
    yes = deferInodes->Add(MkInode(200, InodeOption().length(2048)));
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 2);

    yes = deferInodes->Get(200, &inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(inode->GetLength(), 2048);

    // CASE 3: add inode(200) which alreay exists
    yes = deferInodes->Add(MkInode(200, InodeOption().length(2049)));
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 2);

    yes = deferInodes->Get(200, &inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(inode->GetLength(), 2049);
}

TEST_F(DeferInodesTest, Get) {
    auto deferInodes = std::make_shared<DeferInodes>(false);
    bool yes = deferInodes->Add(MkInode(100, InodeOption().length(1024)));
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 1);

    // CASE 1: get exist inode
    std::shared_ptr<InodeWrapper> inode;
    yes = deferInodes->Get(100, &inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(inode->GetLength(), 1024);

    // CASE 2: get non-exist inode
    yes = deferInodes->Get(200, &inode);
    ASSERT_FALSE(yes);
}

TEST_F(DeferInodesTest, Remove_Basic) {
    auto deferInodes = std::make_shared<DeferInodes>(false);
    auto inode = MkInode(100, InodeOption().length(1024));
    bool yes = deferInodes->Add(inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 1);

    // CASE 1: remove exist inode
    yes = deferInodes->Remove(inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 0);

    yes = deferInodes->Get(100, &inode);
    ASSERT_FALSE(yes);

    // CASE 2: remove non-exist inode
    yes = deferInodes->Remove(inode);
    ASSERT_FALSE(yes);
}

TEST_F(DeferInodesTest, Remove_CompareCtime) {
    auto deferInodes = std::make_shared<DeferInodes>(false);
    auto deferInode = MkInode(100, InodeOption().length(1024).ctime(123, 456));
    bool yes = deferInodes->Add(deferInode);
    ASSERT_EQ(deferInodes->Size(), 1);

    // CASE 1: attr ctime < defered ctime => remove fail
    auto inode = MkInode(100, InodeOption().length(1024).ctime(123, 455));
    yes = deferInodes->Remove(inode);
    ASSERT_FALSE(yes);
    ASSERT_EQ(deferInodes->Size(), 1);

    // CASE 2:  attr ctime > defered ctime => remove success
    inode = MkInode(100, InodeOption().length(1024).ctime(123, 457));
    yes = deferInodes->Remove(inode);
    ASSERT_TRUE(yes);
    ASSERT_EQ(deferInodes->Size(), 0);
}

TEST_F(DeferSyncTest, Basic) {
    auto builder = DeferSyncBuilder();
    auto deferSync = builder.SetOption([&](bool* cto, DeferSyncOption* option) {
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
    auto deferSync = builder.SetOption([&](bool* cto, DeferSyncOption* option) {
        option->delay = 3;
    }).Build();
    deferSync->Start();

    auto inode = MkInode(100, InodeOption().metaClient(metaClient_));
    EXPECT_CALL_INDOE_SYNC_TIMES(*metaClient_, 100 /* ino */, 0 /* times */);
    deferSync->Push(inode);
    deferSync->Stop();
}

TEST_F(DeferSyncTest, IsDefered_cto) {
    auto builder = DeferSyncBuilder();
    auto deferSync = builder.SetOption([&](bool* cto, DeferSyncOption* option) {
        *cto = true;
        option->delay = 3;
    }).Build();
    deferSync->Start();

    std::shared_ptr<InodeWrapper> inode;
    deferSync->Push(MkInode(100, InodeOption()));
    bool yes = deferSync->IsDefered(100, &inode);
    ASSERT_FALSE(yes);
    deferSync->Stop();
}

TEST_F(DeferSyncTest, IsDefered_nocto) {
    auto builder = DeferSyncBuilder();
    auto deferSync = builder.SetOption([&](bool* cto, DeferSyncOption* option) {
        *cto = false;
        option->delay = 3;
    }).Build();
    deferSync->Start();

    std::shared_ptr<InodeWrapper> inode;
    deferSync->Push(MkInode(100, InodeOption()));
    bool yes = deferSync->IsDefered(100, &inode);
    ASSERT_TRUE(yes);

    // wait inode synced and defer inode removed
    std::this_thread::sleep_for(std::chrono::seconds(4));
    yes = deferSync->IsDefered(100, &inode);
    ASSERT_FALSE(yes);
    deferSync->Stop();
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
