
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

#include "curvefs/src/client/filesystem/utils.h"
#include "curvefs/test/client/filesystem/helper/helper.h"

namespace curvefs {
namespace client {
namespace filesystem {

class RPCClientTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RPCClientTest, GetAttr_Basic) {
    auto builder = RPCClientBuilder();
    auto rpc = builder.Build();

    // CASE 1: ok
    {
        EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                        CURVEFS_ERROR::OK);

        InodeAttr attr;
        auto rc = rpc->GetAttr(100, &attr);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    }

    // CASE 2: inode not exist
    {
        EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                        CURVEFS_ERROR::NOTEXIST);

        InodeAttr attr;
        auto rc = rpc->GetAttr(100, &attr);
        ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
    }
}

TEST_F(RPCClientTest, Lookup_Basic) {
    auto builder = RPCClientBuilder();
    auto rpc = builder.Build();

    // CASE 1: ok
    {
        EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                                     CURVEFS_ERROR::OK);
        EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                        CURVEFS_ERROR::OK);

        EntryOut entryOut;
        auto rc = rpc->Lookup(1, "f1", &entryOut);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    }

    // CASE 2: dentry not exist
    {
        EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                                     CURVEFS_ERROR::NOTEXIST);

        EntryOut entryOut;
        auto rc = rpc->Lookup(1, "f1", &entryOut);
        ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
    }

    // CASE 3: inode not exist
    {
        EXPECT_CALL_RETURN_GetDentry(*builder.GetDentryManager(),
                                     CURVEFS_ERROR::OK);
        EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                        CURVEFS_ERROR::NOTEXIST);

        EntryOut entryOut;
        auto rc = rpc->Lookup(1, "f1", &entryOut);
        ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
    }
}

TEST_F(RPCClientTest, ReadDir_Basic) {
    auto builder = RPCClientBuilder();
    auto rpc = builder.Build();

    // CASE 1: ok
    {
        EXPECT_CALL_INVOKE_ListDentry(*builder.GetDentryManager(),
            [&](uint64_t parent,
                std::list<Dentry>* dentries,
                uint32_t limit,
                bool only,
                uint32_t nlink) -> CURVEFS_ERROR {
                dentries->push_back(MkDentry(1, "test"));
                return CURVEFS_ERROR::OK;
            });
        EXPECT_CALL_INVOKE_BatchGetInodeAttrAsync(*builder.GetInodeManager(),
            [&](uint64_t parentId,
                std::set<uint64_t>* inos,
                std::map<uint64_t, InodeAttr>* attrs) -> CURVEFS_ERROR {
                for (const auto& ino : *inos) {
                    auto attr = MkAttr(ino, AttrOption().mtime(123, ino));
                    attrs->emplace(ino, attr);
                }
                return CURVEFS_ERROR::OK;
            });

        DirEntry dirEntry;
        auto entries = std::make_shared<DirEntryList>();
        auto rc = rpc->ReadDir(100, &entries);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
        ASSERT_EQ(entries->Size(), 1);
        ASSERT_TRUE(entries->Get(1, &dirEntry));
        ASSERT_EQ(dirEntry.ino, 1);
        ASSERT_EQ(dirEntry.name, "test");
    }

    // CASE 2: inode not exist
    {
        EXPECT_CALL_RETURN_GetInodeAttr(*builder.GetInodeManager(),
                                        CURVEFS_ERROR::NOTEXIST);

        InodeAttr attr;
        auto rc = rpc->GetAttr(100, &attr);
        ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
    }
}

TEST_F(RPCClientTest, Open_Basic) {
    auto builder = RPCClientBuilder();
    auto rpc = builder.Build();

    // CASE 1: ok
    {
        EXPECT_CALL_RETURN_GetInode(*builder.GetInodeManager(),
                                    CURVEFS_ERROR::OK);

        auto inode = MkInode(100);
        auto rc = rpc->Open(100, &inode);
        ASSERT_EQ(rc, CURVEFS_ERROR::OK);
    }

    // CASE 2: inode not exist
    {
        EXPECT_CALL_RETURN_GetInode(*builder.GetInodeManager(),
                                    CURVEFS_ERROR::NOTEXIST);

        auto inode = MkInode(100);
        auto rc = rpc->Open(100, &inode);
        ASSERT_EQ(rc, CURVEFS_ERROR::NOTEXIST);
    }
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
