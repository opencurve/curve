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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "curvefs/test/client/mock_metaserver_client.h"
#include "curvefs/src/client/inode_cache_manager.h"

namespace curvefs {
namespace client {

using ::testing::Return;
using ::testing::_;
using ::testing::Contains;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::DoAll;

using rpcclient::MockMetaServerClient;

class TestInodeCacheManager : public ::testing::Test {
 protected:
    TestInodeCacheManager() {}
    ~TestInodeCacheManager() {}

    virtual void SetUp() {
        metaClient_ = std::make_shared<MockMetaServerClient>();
        iCacheManager_ = std::make_shared<InodeCacheManagerImpl>(metaClient_);
        iCacheManager_->SetFsId(fsId_);
        iCacheManager_->Init(10, true);
    }

    virtual void TearDown() {
        metaClient_ = nullptr;
        iCacheManager_ = nullptr;
    }

 protected:
    std::shared_ptr<InodeCacheManagerImpl> iCacheManager_;
    std::shared_ptr<MockMetaServerClient> metaClient_;
    uint32_t fsId_ = 888;
};

TEST_F(TestInodeCacheManager, GetInode) {
    uint64_t inodeId = 100;
    uint64_t fileLength = 100;

    Inode inode;
    inode.set_inodeid(inodeId);
    inode.set_fsid(fsId_);
    inode.set_length(fileLength);

    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(DoAll(SetArgPointee<2>(inode),
                Return(MetaStatusCode::OK)));

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    Inode out = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(fileLength, out.length());

    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    out = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(fileLength, out.length());
}

TEST_F(TestInodeCacheManager, CreateAndGetInode) {
    uint64_t inodeId = 100;

    InodeParam param;
    param.fsId = fsId_;
    param.type = FsFileType::TYPE_FILE;

    Inode inode;
    inode.set_inodeid(inodeId);
    inode.set_fsid(fsId_);
    inode.set_type(FsFileType::TYPE_FILE);
    EXPECT_CALL(*metaClient_, CreateInode(_, _))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))
        .WillOnce(DoAll(SetArgPointee<1>(inode),
            Return(MetaStatusCode::OK)));

    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = iCacheManager_->CreateInode(param, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::UNKNOWN, ret);

    ret = iCacheManager_->CreateInode(param, inodeWrapper);
    Inode out = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(FsFileType::TYPE_FILE, out.type());

    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    out = inodeWrapper->GetInodeUnlocked();
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(FsFileType::TYPE_FILE, out.type());
}

TEST_F(TestInodeCacheManager, DeleteInode) {
    uint64_t inodeId = 100;

    EXPECT_CALL(*metaClient_, DeleteInode(fsId_, inodeId))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(Return(MetaStatusCode::OK));

    CURVEFS_ERROR ret = iCacheManager_->DeleteInode(inodeId);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = iCacheManager_->DeleteInode(inodeId);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
}

TEST_F(TestInodeCacheManager, ClearInodeCache) {
    uint64_t inodeId = 100;
    iCacheManager_->ClearInodeCache(inodeId);
}

TEST_F(TestInodeCacheManager, ShipToFlushAndFlushAll) {
    uint64_t inodeId = 100;
    Inode inode;
    inode.set_inodeid(inodeId);
    inode.set_fsid(fsId_);
    inode.set_type(FsFileType::TYPE_FILE);

    std::shared_ptr<InodeWrapper> inodeWrapper = std::make_shared<InodeWrapper>(
            inode, metaClient_);
    inodeWrapper->MarkDirty();

    iCacheManager_->ShipToFlush(inodeWrapper);

    EXPECT_CALL(*metaClient_, UpdateInode(_))
        .WillOnce(Return(MetaStatusCode::UNKNOWN_ERROR))
        .WillOnce(Return(MetaStatusCode::OK));
    iCacheManager_->FlushAll();
}


}  // namespace client
}  // namespace curvefs
