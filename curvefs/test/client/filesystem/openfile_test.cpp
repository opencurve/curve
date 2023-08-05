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
 * Created Date: 2023-04-03
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/filesystem/openfile.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class OpenFileTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(OpenFileTest, Open) {
    auto builder = OpenFilesBuilder();
    auto openfiles = builder.Build();

    Ino ino(100);
    auto inode = MkInode(ino);
    openfiles->Open(ino, inode);

    // CASE 1: ino(100) opened
    auto out = MkInode(0);
    bool yes = openfiles->IsOpened(ino, &out);
    ASSERT_TRUE(yes);
    ASSERT_EQ(out->GetInodeId(), ino);

    // CASE 2: ino(200) closed
    yes = openfiles->IsOpened(200, &out);
    ASSERT_FALSE(yes);
}

TEST_F(OpenFileTest, Close) {
    auto builder = OpenFilesBuilder();
    auto openfiles = builder.Build();

    Ino ino(100);
    auto inode = MkInode(ino);
    openfiles->Open(ino, inode);

    // CASE 1: check open status
    auto out = MkInode(0);
    bool yes = openfiles->IsOpened(ino, &out);
    ASSERT_TRUE(yes);
    ASSERT_EQ(out->GetInodeId(), ino);

    // CASE 2: check open status after close
    openfiles->Close(ino);
    yes = openfiles->IsOpened(ino, &out);
    ASSERT_FALSE(yes);
}

TEST_F(OpenFileTest, References) {
    auto builder = OpenFilesBuilder();
    auto openfiles = builder.Build();

    // open ino(100) twices
    Ino ino(100);
    auto inode = MkInode(ino);
    openfiles->Open(ino, inode);
    openfiles->Open(ino, inode);

    // CASE 1: close once
    openfiles->Close(ino);
    auto out = MkInode(0);
    bool yes = openfiles->IsOpened(ino, &out);
    ASSERT_TRUE(yes);
    ASSERT_EQ(out->GetInodeId(), ino);

    // CASE 2: close again and trigger delete
    openfiles->Close(ino);
    yes = openfiles->IsOpened(ino, &out);
    ASSERT_FALSE(yes);
}

TEST_F(OpenFileTest, CloseAll) {
    auto builder = OpenFilesBuilder();
    auto openfiles = builder.Build();

    // 1) open ino{1..10}
    for (auto ino = 1; ino <= 10; ino++) {
        auto inode = MkInode(ino);
        openfiles->Open(ino, inode);
    }

    // 2) check open status for ino{1..10}
    auto out = MkInode(0);
    for (auto ino = 1; ino <= 10; ino++) {
        bool yes = openfiles->IsOpened(ino, &out);
        ASSERT_TRUE(yes);
        ASSERT_EQ(out->GetInodeId(), ino);
    }

    // 3) CloseAll() and check open status for ino{1..10}
    openfiles->CloseAll();
    for (auto ino = 1; ino <= 10; ino++) {
        bool yes = openfiles->IsOpened(ino, &out);
        ASSERT_FALSE(yes);
    }
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
