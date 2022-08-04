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
#include <chrono>
#include <cstdint>
#include <thread>

#include "curvefs/src/client/rpcclient/metaserver_client.h"
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
using rpcclient::DataIndices;

class TestInodeCacheManager : public ::testing::Test {
 protected:
    TestInodeCacheManager() {}
    ~TestInodeCacheManager() {}

    virtual void SetUp() {
        curvefs::client::common::FLAGS_enableCto = false;
        metaClient_ = std::make_shared<MockMetaServerClient>();
        iCacheManager_ = std::make_shared<InodeCacheManagerImpl>(metaClient_);
        iCacheManager_->SetFsId(fsId_);
        RefreshDataOption option;
        option.maxDataSize = 1;
        option.refreshDataIntervalSec = 0;
        iCacheManager_->Init(3, true, 1, option);
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
    inode.set_type(FsFileType::TYPE_S3);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(1);
    s3ChunkInfo->set_compaction(1);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(1024);
    s3ChunkInfo->set_size(65536);
    s3ChunkInfo->set_zero(true);
    s3ChunkInfoMap->insert({1, *s3ChunkInfoList});

    // miss cache and get inode failed
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    std::shared_ptr<InodeWrapper> inodeWrapper;
    CURVEFS_ERROR ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    // miss cache and get inode ok, do not need streaming
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inode), SetArgPointee<3>(false),
                        Return(MetaStatusCode::OK)));
    ret = iCacheManager_->GetInode(inodeId, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    // miss cache and get inode ok, need streaming
    uint64_t inodeId2 = 200;
    Inode inode2 = inode;
    inode2.set_inodeid(inodeId2);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId2, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inode2), SetArgPointee<3>(true),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_,
                GetOrModifyS3ChunkInfo(fsId_, inodeId2, _, true, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK,
              iCacheManager_->GetInode(inodeId2, inodeWrapper));

    // hit cache and need refresh s3info
    EXPECT_CALL(*metaClient_,
        GetOrModifyS3ChunkInfo(fsId_, inodeId, _, true, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
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

TEST_F(TestInodeCacheManager, RefreshInode) {
    uint64_t inodeId = 100;
    uint64_t fileLength = 100;

    Inode inode;
    inode.set_inodeid(inodeId);
    inode.set_fsid(fsId_);
    inode.set_length(fileLength);
    inode.set_type(FsFileType::TYPE_S3);
    auto s3ChunkInfoMap = inode.mutable_s3chunkinfomap();
    S3ChunkInfoList *s3ChunkInfoList = new S3ChunkInfoList();
    S3ChunkInfo *s3ChunkInfo = s3ChunkInfoList->add_s3chunks();
    s3ChunkInfo->set_chunkid(1);
    s3ChunkInfo->set_compaction(1);
    s3ChunkInfo->set_offset(0);
    s3ChunkInfo->set_len(1024);
    s3ChunkInfo->set_size(65536);
    s3ChunkInfo->set_zero(true);
    s3ChunkInfoMap->insert({1, *s3ChunkInfoList});

    // cache miss, get s3-inode from metaserver failed
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND));
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, iCacheManager_->RefreshInode(inodeId));

    // cache miss, get s3-inode from metaserver ok, do not need streaming
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inode), SetArgPointee<3>(false),
                        Return(MetaStatusCode::OK)));
    ASSERT_EQ(CURVEFS_ERROR::OK, iCacheManager_->RefreshInode(inodeId));

    // cache miss, get s3-inode from metaserver, need streaming
    uint64_t inodeId2 = 200;
    Inode inode2 = inode;
    inode2.set_inodeid(inodeId2);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId2, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inode2), SetArgPointee<3>(true),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_,
                GetOrModifyS3ChunkInfo(fsId_, inodeId2, _, true, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, iCacheManager_->RefreshInode(inodeId2));

    // cache hit, refresh s3-inode from metaserver, do not need streaming
    Inode inodenew = inode;
    inodenew.set_length(fileLength * 3);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodeId, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inodenew), SetArgPointee<3>(false),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_,
                GetOrModifyS3ChunkInfo(fsId_, inodeId, _, true, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, iCacheManager_->RefreshInode(inodeId));
    std::shared_ptr<InodeWrapper> inodeWrapper;
    ASSERT_EQ(CURVEFS_ERROR::OK,
              iCacheManager_->GetInode(inodeId, inodeWrapper));
    ASSERT_EQ(inodenew.length(), inodeWrapper->GetLength());

    // cache miss, get file-inode from metaserver
    Inode inodefile;
    uint64_t inodefileid = 300;
    inodefile.set_inodeid(inodefileid);
    inodefile.set_fsid(fsId_);
    inodefile.set_type(FsFileType::TYPE_FILE);
    inodefile.set_length(1);
    EXPECT_CALL(*metaClient_, GetInode(fsId_, inodefileid, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(inodefile), SetArgPointee<3>(false),
                        Return(MetaStatusCode::OK)));
    EXPECT_CALL(*metaClient_, GetVolumeExtent(fsId_, inodefileid, _, _))
        .WillOnce(Return(MetaStatusCode::OK));
    ASSERT_EQ(CURVEFS_ERROR::OK, iCacheManager_->RefreshInode(inodefileid));
}

TEST_F(TestInodeCacheManager, GetInodeAttr) {
    uint64_t inodeId = 100;
    uint64_t parentId = 99;
    uint64_t fileLength = 100;

    std::list<InodeAttr> attrs;
    InodeAttr attr;
    attr.set_inodeid(inodeId);
    attr.set_fsid(fsId_);
    attr.set_length(fileLength);
    attr.add_parent(parentId);
    attr.set_type(FsFileType::TYPE_FILE);
    attrs.emplace_back(attr);

    // 1. get from metaserver
    InodeAttr out;
    EXPECT_CALL(*metaClient_, BatchGetInodeAttr(fsId_, _, _))
        .WillOnce(Return(MetaStatusCode::NOT_FOUND))
        .WillOnce(DoAll(SetArgPointee<2>(attrs), Return(MetaStatusCode::OK)));

    CURVEFS_ERROR ret = iCacheManager_->GetInodeAttr(inodeId, &out);
    ASSERT_EQ(CURVEFS_ERROR::NOTEXIST, ret);

    ret = iCacheManager_->GetInodeAttr(inodeId, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(inodeId, out.inodeid());
    ASSERT_EQ(fsId_, out.fsid());
    ASSERT_EQ(fileLength, out.length());

    // 2. create inode and get attr from icache
    InodeParam param;
    param.fsId = fsId_;
    param.type = FsFileType::TYPE_FILE;
    Inode inode;
    inode.set_inodeid(inodeId + 1);
    inode.set_fsid(fsId_ + 1);
    inode.set_type(FsFileType::TYPE_FILE);
    std::shared_ptr<InodeWrapper> inodeWrapper;
    EXPECT_CALL(*metaClient_, CreateInode(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));
    ret = iCacheManager_->CreateInode(param, inodeWrapper);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);

    ret = iCacheManager_->GetInodeAttr(inodeId + 1, &out);
    ASSERT_EQ(CURVEFS_ERROR::OK, ret);
    ASSERT_EQ(inodeId + 1, out.inodeid());
    ASSERT_EQ(fsId_ + 1, out.fsid());
    ASSERT_EQ(FsFileType::TYPE_FILE, out.type());
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
    inode.set_type(FsFileType::TYPE_S3);

    std::shared_ptr<InodeWrapper> inodeWrapper =
        std::make_shared<InodeWrapper>(inode, metaClient_);
    inodeWrapper->MarkDirty();
    S3ChunkInfo info;
    inodeWrapper->AppendS3ChunkInfo(1, info);

    iCacheManager_->ShipToFlush(inodeWrapper);

    EXPECT_CALL(*metaClient_, UpdateInodeWithOutNlinkAsync_rvr(_, _, _, _))
        .WillOnce(Invoke([](const Inode &inode, MetaServerClientDone *done,
                            InodeOpenStatusChange statusChange,
                            DataIndices /*indices*/) {
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

    EXPECT_CALL(*metaClient_, BatchGetInodeAttr(fsId_, inodeIds, _))
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

    EXPECT_CALL(*metaClient_, BatchGetXAttr(fsId_, inodeIds, _))
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

TEST_F(TestInodeCacheManager, TestFlushInodeBackground) {
    uint64_t inodeId = 100;
    Inode inode;
    inode.set_inodeid(inodeId);
    inode.set_fsid(fsId_);
    inode.set_type(FsFileType::TYPE_S3);
    InodeParam param;
    param.fsId = fsId_;
    param.type = FsFileType::TYPE_FILE;
    std::map<uint64_t, std::shared_ptr<InodeWrapper>> inodeMap;

    for (int i = 0; i < 4; i++) {
        inode.set_inodeid(inodeId + i);
        EXPECT_CALL(*metaClient_, CreateInode(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(inode), Return(MetaStatusCode::OK)));
        std::shared_ptr<InodeWrapper> inodeWrapper;
        iCacheManager_->CreateInode(param, inodeWrapper);
        inodeWrapper->MarkDirty();
        S3ChunkInfo info;
        inodeWrapper->AppendS3ChunkInfo(1, info);
        iCacheManager_->ShipToFlush(inodeWrapper);
        inodeMap.emplace(inodeId + i, inodeWrapper);
    }

    EXPECT_CALL(*metaClient_, UpdateInodeWithOutNlinkAsync_rvr(_, _, _, _))
        .WillRepeatedly(
            Invoke([](const Inode& inode, MetaServerClientDone* done,
                      InodeOpenStatusChange statusChange,
                      DataIndices /*dataIndices*/) {
                // run closure in a separate thread
                std::thread th{[done]() {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                    done->SetMetaStatusCode(MetaStatusCode::OK);
                    done->Run();
                }};

                th.detach();
            }));

    EXPECT_CALL(*metaClient_, GetOrModifyS3ChunkInfoAsync(_, _, _, _))
        .WillRepeatedly(
            Invoke([](uint32_t fsId, uint64_t inodeId,
                      const google::protobuf::Map<uint64_t, S3ChunkInfoList>
                          &s3ChunkInfos,
                      MetaServerClientDone *done) {
                done->SetMetaStatusCode(MetaStatusCode::OK);
                done->Run();
            }));
    iCacheManager_->Run();
    sleep(10);
    ASSERT_EQ(false,  iCacheManager_->IsDirtyMapExist(100));
    ASSERT_EQ(false,  iCacheManager_->IsDirtyMapExist(101));
    auto iter = inodeMap.find(100);
    ASSERT_EQ(false, iter->second->IsDirty());
    iter = inodeMap.find(102);
    ASSERT_EQ(false, iter->second->IsDirty());
    iCacheManager_->Stop();
}
}  // namespace client
}  // namespace curvefs
