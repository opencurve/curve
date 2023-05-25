
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
        EXPECT_CALL_RETURN_ListDentry(*builder.GetDentryManager(),
                                      CURVEFS_ERROR::OK);
        EXPECT_CALL_RETURN_BatchGetInodeAttrAsync(*builder.GetInodeManager(),
                                                  CURVEFS_ERROR::OK);

        auto entries = std::make_shared<DirEntryList>();
        auto rc = rpc->ReadDir(100, &entries);
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
