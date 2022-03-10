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
#include "curvefs/src/common/define.h"

namespace curvefs {
namespace client {
namespace common {
DECLARE_bool(enableCto);
}  // namespace common
}  // namespace client
}  // namespace curvefs

namespace curvefs {
namespace client {

using ::testing::_;
using ::testing::Contains;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::AnyOf;

using rpcclient::MetaServerClientDone;
using rpcclient::MockMetaServerClient;

class TestInodeCacheManager : public ::testing::Test {
 protected:
    TestInodeCacheManager() {}
    ~TestInodeCacheManager() {}

    virtual void SetUp() {
        curvefs::client::common::FLAGS_enableCto = false;
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
        .WillOnce(DoAll(SetArgPointee<2>(inode), Return(MetaStatusCode::OK)));

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

    curvefs::client::common::FLAGS_enableCto = true;
    inodeWrapper->SetOpenCount(0);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(fileLength, out.length());

    inodeWrapper->SetOpenCount(1);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);
}

TEST_F(TestInodeCacheManager, CreateAndGetInode) {
    curvefs::client::common::FLAGS_enableCto = false;
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
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));

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
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

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

    std::shared_ptr<InodeWrapper> inodeWrapper =
        std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->MarkDirty();
    S3ChunkInfo info;
    inodeWrapper->AppendS3ChunkInfo(1, info);

    iCacheManager_->ShipToFlush(inodeWrapper);

    EXPECT_CALL(*metaClient_, UpdateInodeAsync(_, _, _))
        .WillOnce(Invoke([](const Inode &inode, MetaServerClientDone *done,
                            InodeOpenStatusChange statusChange) {
            done->SetMetaStatusCode(MetaStatusCode::OK);
            done->Run();
        }));

    EXPECT_CALL(*metaClient_, GetOrModifyS3ChunkInfoAsync(_, _, _, _))
        .WillOnce(
            Invoke([](uint32_t fsId, uint64_t inodeId,
                      const google::protobuf::Map<uint64_t, S3ChunkInfoList>
                          &s3ChunkInfos,
                      MetaServerClientDone *done) {
                done->SetMetaStatusCode(MetaStatusCode::OK);
                done->Run();
            }));

    iCacheManager_->FlushAll();
}

TEST_F(TestInodeCacheManager, BatchGetInodeAttr) {
    uint64_t inodeId1 = 100;
    uint64_t inodeId2 = 200;
    uint64_t fileLength = 100;

    // in
    std::set<uint64_t> inodeIds;
    inodeIds.emplace(inodeId1);
    inodeIds.emplace(inodeId2);

    // out
    std::list<InodeAttr> attrs;
    InodeAttr attr;
    attr.set_inodeid(inodeId1);
    attr.set_fsid(fsId_);
    attr.set_length(fileLength);
    attrs.emplace_back(attr);
    attr.set_inodeid(inodeId2);
    attrs.emplace_back(attr);

    EXPECT_CALL(*metaClient_, BatchGetInodeAttr(fsId_, &inodeIds, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(DoAll(SetArgPointee<2>(attrs),
                Return(MetaStatusCode::OK)));

    std::list<InodeAttr> getAttrs;
    CURVEFS_ERROR ret = iCacheManager_->BatchGetInodeAttr(&inodeIds, &getAttrs);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = iCacheManager_->BatchGetInodeAttr(&inodeIds, &getAttrs);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(getAttrs.size(), 2);
    ASSERT_THAT(getAttrs.begin()->inodeid(), AnyOf(inodeId1, inodeId2));
    ASSERT_EQ(getAttrs.begin()->fsid(), fsId_);
    ASSERT_EQ(getAttrs.begin()->length(), fileLength);
}

TEST_F(TestInodeCacheManager, BatchGetXAttr) {
    uint64_t inodeId1 = 100;
    uint64_t inodeId2 = 200;

    // in
    std::set<uint64_t> inodeIds;
    inodeIds.emplace(inodeId1);
    inodeIds.emplace(inodeId2);

    // out
    std::list<XAttr> xattrs;
    XAttr xattr;
    xattr.set_fsid(fsId_);
    xattr.set_inodeid(inodeId1);
    xattr.mutable_xattrinfos()->insert({XATTRFILES, "1"});
    xattr.mutable_xattrinfos()->insert({XATTRSUBDIRS, "1"});
    xattr.mutable_xattrinfos()->insert({XATTRENTRIES, "2"});
    xattr.mutable_xattrinfos()->insert({XATTRFBYTES, "100"});
    xattrs.emplace_back(xattr);
    xattr.set_inodeid(inodeId2);
    xattr.mutable_xattrinfos()->find(XATTRFBYTES)->second = "200";
    xattrs.emplace_back(xattr);

    EXPECT_CALL(*metaClient_, BatchGetXAttr(fsId_, &inodeIds, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(DoAll(SetArgPointee<2>(xattrs),
                Return(MetaStatusCode::OK)));

    std::list<XAttr> getXAttrs;
    CURVEFS_ERROR ret = iCacheManager_->BatchGetXAttr(&inodeIds, &getXAttrs);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = iCacheManager_->BatchGetXAttr(&inodeIds, &getXAttrs);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(getXAttrs.size(), 2);
    ASSERT_THAT(getXAttrs.begin()->inodeid(), AnyOf(inodeId1, inodeId2));
    ASSERT_EQ(getXAttrs.begin()->fsid(), fsId_);
    ASSERT_THAT(getXAttrs.begin()->xattrinfos().find(XATTRFBYTES)->second,
        AnyOf("100", "200"));
}

TEST_F(TestInodeCacheManager, ParentMap) {
    uint64_t inodeId1 = 1;
    uint64_t p1 = 100;
    uint64_t p2 = 200;
    uint64_t p3 = 300;

    uint64_t parent;
    ASSERT_FALSE(iCacheManager_->GetParent(inodeId1, &parent));

    iCacheManager_->AddParent(inodeId1, p1);
    ASSERT_TRUE(iCacheManager_->GetParent(inodeId1, &parent));
    ASSERT_EQ(parent, p1);

    iCacheManager_->AddParent(inodeId1, p2);
    ASSERT_TRUE(iCacheManager_->GetParent(inodeId1, &parent));
    ASSERT_EQ(parent, p2);

    ASSERT_TRUE(iCacheManager_->UpdateParent(inodeId1, p3));
    ASSERT_TRUE(iCacheManager_->GetParent(inodeId1, &parent));
    ASSERT_EQ(parent, p3);

    iCacheManager_->ClearParent(inodeId1);
    ASSERT_FALSE(iCacheManager_->GetParent(inodeId1, &parent));
}

}  // namespace client
}  // namespace curvefs
