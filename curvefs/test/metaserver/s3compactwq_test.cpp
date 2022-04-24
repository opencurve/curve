/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * @Project: curve
 * @Date: 2021-09-07
 * @Author: majie1
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/src/metaserver/s3compact_manager.h"
#include "curvefs/src/metaserver/s3compact_wq_impl.h"
#include "curvefs/src/metaserver/storage/rocksdb_storage.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/mock_s3_adapter.h"
#include "curvefs/test/metaserver/mock_s3compactwq_impl.h"
#include "curvefs/test/metaserver/mock_s3infocache.h"
#include "curvefs/test/metaserver/storage/utils.h"

using ::curvefs::metaserver::copyset::CopysetNode;
using ::curvefs::metaserver::copyset::CopysetNodeManager;
using ::curvefs::metaserver::copyset::MockCopysetNodeManager;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::ReturnRef;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

using ::curvefs::metaserver::storage::KVStorage;
using ::curvefs::metaserver::storage::RandomStoragePath;
using ::curvefs::metaserver::storage::RocksDBStorage;
using ::curvefs::metaserver::storage::StorageOptions;

namespace curvefs {
namespace metaserver {
class S3CompactWorkQueueImplTest : public ::testing::Test {
 public:
    void SetUp() override {
        opts_.enable = true;
        opts_.threadNum = 1;
        opts_.queueSize = 1;
        opts_.fragmentThreshold = 20;
        opts_.maxChunksPerCompact = 10;
        opts_.s3ReadMaxRetry = 2;
        opts_.s3ReadRetryInterval = 1;
        uint64_t s3adapterSize = 10;
        S3AdapterOption opts;
        s3adapterManager_ =
            std::make_shared<MockS3AdapterManager>(s3adapterSize, opts);
        s3adapter_ = std::make_shared<MockS3Adapter>();
        uint64_t cacheCapacity = 1;
        std::vector<std::string> mdsAddrs{"10.0.0.2:1000", "10.0.0.3:10001"};
        butil::EndPoint metaserverAddr;
        s3infoCache_ = std::make_shared<MockS3InfoCache>(
            cacheCapacity, mdsAddrs, metaserverAddr);

        dataDir_ = RandomStoragePath();
        StorageOptions options;
        options.dataDir = dataDir_;
        kvStorage_ = std::make_shared<RocksDBStorage>(options);
        ASSERT_TRUE(kvStorage_->Open());

        inodeStorage_ =
            std::make_shared<InodeStorage>(kvStorage_, "partition:1");
        trash_ = std::make_shared<TrashImpl>(inodeStorage_);
        inodeManager_ = std::make_shared<InodeManager>(inodeStorage_, trash_);
        impl_ = std::make_shared<S3CompactWorkQueueImpl>(
            s3adapterManager_, s3infoCache_, opts_,
            &copyset::CopysetNodeManager::GetInstance());
        mockCopystNodeManager_ = std::make_shared<MockCopysetNodeManager>();
        mockImpl_ = std::make_shared<MockS3CompactWorkQueueImpl>(
            s3adapterManager_, s3infoCache_, opts_,
            mockCopystNodeManager_.get());
        mockCopysetNodeWrapper_ = std::make_shared<MockCopysetNodeWrapper>();
    }

    void TearDown() override {
        ASSERT_TRUE(kvStorage_->Close());
        auto output = execShell("rm -rf " + dataDir_);
        ASSERT_EQ(output.size(), 0);
    }

    std::string execShell(const std::string& cmd) {
        std::array<char, 128> buffer;
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                      pclose);
        if (!pipe) {
            throw std::runtime_error("popen() failed!");
        }
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }

 protected:
    S3CompactWorkQueueOption opts_;
    std::shared_ptr<S3AdapterManager> testS3adapterManager_;
    std::shared_ptr<MockS3AdapterManager> s3adapterManager_;
    std::shared_ptr<MockS3Adapter> s3adapter_;
    std::shared_ptr<MockS3InfoCache> s3infoCache_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<TrashImpl> trash_;
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<S3CompactWorkQueueImpl> impl_;
    std::shared_ptr<MockS3CompactWorkQueueImpl> mockImpl_;
    std::shared_ptr<MockCopysetNodeWrapper> mockCopysetNodeWrapper_;
    std::shared_ptr<MockCopysetNodeManager> mockCopystNodeManager_;
    std::string dataDir_;
    std::shared_ptr<KVStorage> kvStorage_;
};

TEST_F(S3CompactWorkQueueImplTest, test_CopysetNodeWrapper) {
    braft::Configuration c;
    auto n = std::make_shared<CopysetNode>(0, 0, c, nullptr);
    CopysetNodeWrapper cw1(n);
    ASSERT_EQ(cw1.IsLeaderTerm(), false);
    ASSERT_EQ(cw1.IsValid(), true);
    CopysetNodeWrapper cw2(nullptr);
    ASSERT_EQ(cw2.IsLeaderTerm(), false);
    ASSERT_EQ(cw2.IsValid(), false);
}

TEST_F(S3CompactWorkQueueImplTest, test_S3InfoCache) {
    auto mock_requests3info = [](uint64_t fsid, S3Info* info) {
        if (fsid == 0) return S3InfoCache::RequestStatusCode::NOS3INFO;
        if (fsid == 1) return S3InfoCache::RequestStatusCode::RPCFAILURE;
        return S3InfoCache::RequestStatusCode::SUCCESS;
    };
    EXPECT_CALL(*s3infoCache_, GetS3Info(_, _))
        .WillRepeatedly(testing::Invoke([&](uint64_t fsid, S3Info* info) {
            return s3infoCache_->S3InfoCache::GetS3Info(fsid, info);
        }));
    EXPECT_CALL(*s3infoCache_, InvalidateS3Info(_))
        .WillRepeatedly(testing::Invoke([&](uint64_t fsid) {
            return s3infoCache_->S3InfoCache::InvalidateS3Info(fsid);
        }));
    EXPECT_CALL(*s3infoCache_, RequestS3Info(_, _))
        .WillRepeatedly(testing::Invoke(mock_requests3info));
    S3Info info;
    auto ret = s3infoCache_->GetS3Info(0, &info);
    // ASSERT_EQ(ret, -1);
    ret = s3infoCache_->GetS3Info(1, &info);
    ASSERT_EQ(ret, -1);
    ret = s3infoCache_->GetS3Info(2, &info);
    ASSERT_EQ(ret, 0);
    // cached
    ret = s3infoCache_->GetS3Info(2, &info);
    ASSERT_EQ(ret, 0);
    // over capacity
    ret = s3infoCache_->GetS3Info(3, &info);
    ASSERT_EQ(ret, 0);
    s3infoCache_->InvalidateS3Info(0);
    s3infoCache_->InvalidateS3Info(3);
}

TEST_F(S3CompactWorkQueueImplTest, test_S3AdapterManager) {
    S3AdapterOption opt;
    opt.loglevel = 0;
    opt.s3Address = "";
    opt.ak = "";
    opt.sk = "";
    opt.bucketName = "";
    opt.scheme = 0;
    opt.verifySsl = false;
    opt.maxConnections = 32;
    opt.connectTimeout = 60000;
    opt.requestTimeout = 10000;
    opt.asyncThreadNum = 1;
    opt.iopsTotalLimit = 0;
    opt.iopsReadLimit = 0;
    opt.iopsWriteLimit = 0;
    opt.bpsTotalMB = 0;
    opt.bpsReadMB = 0;
    opt.bpsWriteMB = 0;
    testS3adapterManager_ = std::make_shared<S3AdapterManager>(1, opt);

    // init/deinit and multi times
    testS3adapterManager_->Init();
    testS3adapterManager_->Init();
    testS3adapterManager_->Deinit();
    testS3adapterManager_->Deinit();

    // test get&release
    testS3adapterManager_->Init();
    std::pair<uint64_t, S3Adapter*> p = testS3adapterManager_->GetS3Adapter();
    ASSERT_EQ(p.first, 0);
    ASSERT_NE(p.second, nullptr);
    p = testS3adapterManager_->GetS3Adapter();
    ASSERT_EQ(p.first, 1);
    ASSERT_EQ(p.second, nullptr);
    testS3adapterManager_->ReleaseS3Adapter(0);
    testS3adapterManager_->Deinit();
}

TEST_F(S3CompactWorkQueueImplTest, test_GetNeedCompact) {
    // no need compact
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3chunkinfoMap;
    S3ChunkInfoList l1;
    for (int i = 0; i < 10; i++) {
        auto ref = l1.add_s3chunks();
        ref->set_chunkid(i);
        ref->set_offset(i);
        ref->set_len(1);
    }
    s3chunkinfoMap.insert({0, l1});
    ASSERT_TRUE(impl_->GetNeedCompact(s3chunkinfoMap, 10, 64).empty());
    // truncate smaller, part of chunk useless
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap, 9, 64).size(), 1);

    // need compact
    S3ChunkInfoList l2;
    for (int i = 0; i < 30; i++) {
        auto ref = l2.add_s3chunks();
        ref->set_chunkid(i);
        ref->set_offset(i + 64);
        ref->set_len(1);
    }
    s3chunkinfoMap.insert({1, l2});
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap, 64 + 30, 64).size(), 1);

    // truncate smaller, full chunk useless
    S3ChunkInfoList l3;
    auto ref = l2.add_s3chunks();
    ref->set_chunkid(0);
    ref->set_offset(64 * 2);
    ref->set_len(1);
    s3chunkinfoMap.insert({2, l3});
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap, 64 * 2 + 1, 64).size(), 1);
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap, 64 * 2, 64).size(), 2);

    // too much need compact, control size
    for (int j = 3; j < 20; j++) {
        S3ChunkInfoList l;
        for (int i = 0; i < 30; i++) {
            auto ref = l.add_s3chunks();
            ref->set_chunkid(i);
            ref->set_offset(i + 64 * j);
            ref->set_len(1);
        }
        s3chunkinfoMap.insert({j, l});
    }
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap, 64 * 19 + 30, 64).size(),
              opts_.maxChunksPerCompact);
}

TEST_F(S3CompactWorkQueueImplTest, test_DeleteObjs) {
    std::vector<std::string> objs;
    objs.emplace_back("obj1");
    objs.emplace_back("obj2");
    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    impl_->DeleteObjs(objs, s3adapter_.get());
}

TEST_F(S3CompactWorkQueueImplTest, test_BuildValidList) {
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    uint64_t inodeLen = 100;
    uint64_t chunkSize = 64 * 1024 * 1024;
    S3ChunkInfo tmpl;
    tmpl.set_compaction(0);
    tmpl.set_zero(false);

    S3ChunkInfoList l;

    // empty add one
    std::cerr << "empty add one" << std::endl;
    auto c1(tmpl);
    c1.set_chunkid(0);
    c1.set_offset(0);
    c1.set_len(1);
    *l.add_s3chunks() = c1;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 1);
    auto first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 0);
    l.Clear();
    c1.set_chunkid(0);
    c1.set_offset(0);
    c1.set_len(200);  // bigger than inodeLen
    *l.add_s3chunks() = c1;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 1);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 99);

    // B contains A
    std::cerr << "B conatins A" << std::endl;
    l.Clear();
    auto c2(tmpl);
    c2.set_chunkid(0);
    c2.set_offset(0);
    c2.set_len(1);
    *l.add_s3chunks() = c2;
    auto c3(tmpl);
    c3.set_chunkid(1);
    c3.set_offset(0);
    c3.set_len(2);
    *l.add_s3chunks() = c3;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 1);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);

    l.Clear();
    c2.set_chunkid(0);
    c2.set_offset(1);
    c2.set_len(1);
    *l.add_s3chunks() = c2;
    c3.set_chunkid(1);
    c3.set_offset(0);
    c3.set_len(2);
    *l.add_s3chunks() = c3;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 1);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);

    // A contains B
    std::cerr << "A conatins B" << std::endl;
    l.Clear();
    auto c4(tmpl);
    c4.set_chunkid(0);
    c4.set_offset(0);
    c4.set_len(3);
    *l.add_s3chunks() = c4;
    auto c5(tmpl);
    c5.set_chunkid(1);
    c5.set_offset(1);
    c5.set_len(1);
    *l.add_s3chunks() = c5;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 3);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 0);
    auto next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 1);
    next = std::next(next);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 2);
    ASSERT_EQ(next->end, 2);

    // no overlap, B behind A
    std::cerr << "B behind A" << std::endl;
    l.Clear();
    auto c6(tmpl);
    c6.set_chunkid(0);
    c6.set_offset(0);
    c6.set_len(1);
    *l.add_s3chunks() = c6;
    auto c7(tmpl);
    c7.set_chunkid(1);
    c7.set_offset(1);
    c7.set_len(1);
    *l.add_s3chunks() = c7;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 0);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 1);

    // no overlap, B front A
    std::cerr << "B front A" << std::endl;
    l.Clear();
    auto c8(tmpl);
    c8.set_chunkid(0);
    c8.set_offset(1);
    c8.set_len(1);
    *l.add_s3chunks() = c8;
    auto c9(tmpl);
    c9.set_chunkid(1);
    c9.set_offset(0);
    c9.set_len(1);
    *l.add_s3chunks() = c9;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 0);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 1);

    // overlap A, B
    std::cerr << "overlap AB" << std::endl;
    l.Clear();
    auto c10(tmpl);
    c10.set_chunkid(0);
    c10.set_offset(0);
    c10.set_len(2);
    *l.add_s3chunks() = c10;
    auto c11(tmpl);
    c11.set_chunkid(1);
    c11.set_offset(1);
    c11.set_len(2);
    *l.add_s3chunks() = c11;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 0);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 2);

    // overlap B, A
    std::cerr << "overlap BA" << std::endl;
    l.Clear();
    auto c12(tmpl);
    c12.set_chunkid(0);
    c12.set_offset(1);
    c12.set_len(2);
    *l.add_s3chunks() = c12;
    auto c13(tmpl);
    c13.set_chunkid(1);
    c13.set_offset(0);
    c13.set_len(2);
    *l.add_s3chunks() = c13;
    validList = impl_->BuildValidList(l, inodeLen, 0, chunkSize);
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 2);
    ASSERT_EQ(next->end, 2);

    std::cerr << "truncated smaller, chunk is not valid" << std::endl;
    l.Clear();
    auto c14(tmpl);
    c14.set_chunkid(0);
    c14.set_offset(chunkSize * 1);
    c14.set_len(1);
    *l.add_s3chunks() = c14;
    inodeLen = chunkSize;
    validList = impl_->BuildValidList(l, inodeLen, 1, chunkSize);
    ASSERT_TRUE(validList.empty());
}

TEST_F(S3CompactWorkQueueImplTest, test_ReadFullChunk) {
    int ret;
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    struct S3CompactWorkQueueImpl::S3CompactCtx ctx {
        1, 1, PartitionInfo(), 4, 64, 0, s3adapter_.get()
    };
    struct S3CompactWorkQueueImpl::S3NewChunkInfo newChunkInfo;
    std::string fullChunk;

    auto reset = [&]() {
        validList.clear();
        fullChunk.clear();
        newChunkInfo = {};
    };

    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    auto mock_getobj = [&](const Aws::String& key, std::string* data) {
        data->clear();
        data->append(ctx.blockSize, '\0');
        return 0;
    };
    EXPECT_CALL(*s3adapter_, GetObject(_, _))
        .WillRepeatedly(testing::Invoke(mock_getobj));

    validList.emplace_back(0, 1, 0, 0, 0, 0, true);
    ret = impl_->ReadFullChunk(ctx, validList, &fullChunk, &newChunkInfo);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(newChunkInfo.newChunkId, 0);
    ASSERT_EQ(newChunkInfo.newCompaction, 1);

    reset();
    validList.emplace_back(0, 0, 1, 1, 0, 1, false);
    validList.emplace_back(1, 10, 0, 0, 1, 11, false);
    validList.emplace_back(13, 13, 2, 0, 13, 14, false);
    ret = impl_->ReadFullChunk(ctx, validList, &fullChunk, &newChunkInfo);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(newChunkInfo.newChunkId, 2);
    ASSERT_EQ(newChunkInfo.newCompaction, 1);
    ASSERT_EQ(fullChunk.size(), 14);

    reset();
    EXPECT_CALL(*s3adapter_, GetObject(_, _)).WillRepeatedly(Return(-1));
    validList.emplace_back(0, 1, 1, 1, 0, 0, false);
    ret = impl_->ReadFullChunk(ctx, validList, &fullChunk, &newChunkInfo);
    ASSERT_EQ(ret, -1);
}

TEST_F(S3CompactWorkQueueImplTest, test_WriteFullChunk) {
    struct S3CompactWorkQueueImpl::S3CompactCtx ctx {
        100, 1, PartitionInfo(), 4, 16, 0, s3adapter_.get()
    };
    struct S3CompactWorkQueueImpl::S3NewChunkInfo newChunkInfo {
        2, 0, 3
    };
    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(0));
    std::string fullChunk(10, '0');
    std::vector<std::string> objsAdded;
    int ret = impl_->WriteFullChunk(ctx, newChunkInfo, fullChunk, &objsAdded);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(objsAdded[0], "1_100_2_0_3");
    ASSERT_EQ(objsAdded[1], "1_100_2_1_3");
    ASSERT_EQ(objsAdded[2], "1_100_2_2_3");

    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(-1));
    ret = impl_->WriteFullChunk(ctx, newChunkInfo, fullChunk, &objsAdded);
    ASSERT_EQ(ret, -1);
}

TEST_F(S3CompactWorkQueueImplTest, test_CompactChunks) {
    uint64_t blockSize = 4;
    uint64_t chunkSize = 64;
    Inode tmp;
    auto mock_updateinode =
        [&](CopysetNode* copysetNode, const PartitionInfo& pinfo,
            uint64_t inode,
            ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3ChunkInfoAdd,
            ::google::protobuf::Map<uint64_t, S3ChunkInfoList>
                s3ChunkInfoRemove) {
            *tmp.mutable_s3chunkinfomap() = s3ChunkInfoAdd;
            return MetaStatusCode::OK;
        };
    EXPECT_CALL(*mockImpl_, UpdateInode_rvr(_, _, _, _, _))
        .WillRepeatedly(testing::Invoke(mock_updateinode));
    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(0));
    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    auto mock_getobj = [&](const Aws::String& key, std::string* data) {
        data->clear();
        data->append(blockSize, '\0');
        return 0;
    };
    EXPECT_CALL(*s3adapter_, GetObject(_, _))
        .WillRepeatedly(testing::Invoke(mock_getobj));

    EXPECT_CALL(*mockCopysetNodeWrapper_, IsLeaderTerm())
        .WillRepeatedly(Return(true));

    EXPECT_CALL(*mockCopysetNodeWrapper_, IsValid())
        .WillRepeatedly(Return(true));

    auto pairResult = std::make_pair(0, s3adapter_.get());
    EXPECT_CALL(*s3adapterManager_, GetS3Adapter())
        .WillRepeatedly(Return(pairResult));

    EXPECT_CALL(*s3adapterManager_, ReleaseS3Adapter(_))
        .WillRepeatedly(Return());

    auto mock_gets3info_success = [&](uint64_t fsid, S3Info* s3info) {
        s3info->set_ak("1");
        s3info->set_sk("2");
        s3info->set_endpoint("3");
        s3info->set_bucketname("4");
        s3info->set_blocksize(blockSize);
        s3info->set_chunksize(chunkSize);
        return 0;
    };

    EXPECT_CALL(*s3infoCache_, GetS3Info(_, _))
        .WillRepeatedly(testing::Invoke(mock_gets3info_success));
    EXPECT_CALL(*s3infoCache_, InvalidateS3Info(_)).WillRepeatedly(Return());

    std::string v = "5";
    EXPECT_CALL(*s3adapter_, GetS3Ak()).WillRepeatedly(Return(v));
    EXPECT_CALL(*s3adapter_, GetS3Sk()).WillRepeatedly(Return(v));
    EXPECT_CALL(*s3adapter_, GetS3Endpoint()).WillRepeatedly(Return(v));
    EXPECT_CALL(*s3adapter_, Reinit(_, _, _, _)).WillRepeatedly(Return());
    EXPECT_CALL(*s3adapter_, GetBucketName()).WillRepeatedly(Return(v));

    struct S3CompactWorkQueueImpl::S3CompactTask t {
        inodeManager_, Key4Inode(0, 1), PartitionInfo(), mockCopysetNodeWrapper_
    };
    // inode not exist
    mockImpl_->CompactChunks(t);
    // s3chunkinfomap size 0
    std::cerr << "s3chunkinfomap size 0" << std::endl;
    Inode inode1;
    inode1.set_fsid(1);
    inode1.set_inodeid(1);
    inode1.set_length(60);
    inode1.set_nlink(1);
    inode1.set_ctime(0);
    inode1.set_ctime_ns(0);
    inode1.set_mtime(0);
    inode1.set_mtime_ns(0);
    inode1.set_atime(0);
    inode1.set_atime_ns(0);
    inode1.set_uid(0);
    inode1.set_gid(0);
    inode1.set_mode(0);
    inode1.set_type(FsFileType::TYPE_FILE);
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3chunkinfoMap;
    *inode1.mutable_s3chunkinfomap() = s3chunkinfoMap;
    ASSERT_EQ(inodeStorage_->Insert(inode1), MetaStatusCode::OK);
    t.inodeKey = Key4Inode(1, 1);
    mockImpl_->CompactChunks(t);
    // normal
    std::cerr << "normal" << std::endl;
    S3ChunkInfoList l0;
    S3ChunkInfoList l1;
    for (int i = 0; i < 16; i++) {
        auto ref = l0.add_s3chunks();
        ref->set_chunkid(i);
        ref->set_compaction(0);
        ref->set_offset(i * 4);
        ref->set_len(4);
        ref->set_size(4);
        ref->set_zero(false);
    }
    for (int i = 16; i < 22; i++) {
        auto ref = l0.add_s3chunks();
        ref->set_chunkid(i);
        ref->set_compaction(0);
        ref->set_offset(i);
        ref->set_len(5);
        ref->set_size(5);
        ref->set_zero(false);
    }
    auto rc = inodeStorage_->ModifyInodeS3ChunkInfoList(
        inode1.fsid(), inode1.inodeid(), 0, &l0, nullptr);
    ASSERT_EQ(rc, MetaStatusCode::OK);
    ASSERT_EQ(inodeStorage_->Update(inode1), MetaStatusCode::OK);
    mockImpl_->CompactChunks(t);
    ASSERT_EQ(tmp.s3chunkinfomap().size(), 1);
    const auto& l = tmp.s3chunkinfomap().at(0);
    ASSERT_EQ(l.s3chunks_size(), 1);
    const auto& s3chunkinfo = l.s3chunks(0);
    ASSERT_EQ(s3chunkinfo.chunkid(), 21);
    ASSERT_EQ(s3chunkinfo.compaction(), 1);
    ASSERT_EQ(s3chunkinfo.offset(), 0);
    ASSERT_EQ(s3chunkinfo.len(), 60);
    ASSERT_EQ(s3chunkinfo.size(), 60);
    ASSERT_EQ(s3chunkinfo.zero(), false);
    // inode nlink = 0, deleted
    inode1.set_nlink(0);
    ASSERT_EQ(inodeStorage_->Update(inode1), MetaStatusCode::OK);
    mockImpl_->CompactChunks(t);
    EXPECT_CALL(*mockImpl_, UpdateInode_rvr(_, _, _, _, _))
        .WillRepeatedly(Return(MetaStatusCode::UNKNOWN_ERROR));
    mockImpl_->CompactChunks(t);
    auto mock_gets3info_fail = [&](uint64_t fsid, S3Info* s3info) {
        return -1;
    };
    EXPECT_CALL(*s3infoCache_, GetS3Info(_, _))
        .WillRepeatedly(testing::Invoke(mock_gets3info_fail));
    // copysetnode is not leader
    EXPECT_CALL(*mockCopysetNodeWrapper_, IsLeaderTerm())
        .WillRepeatedly(Return(true));
    mockImpl_->CompactChunks(t);
}
}  // namespace metaserver
}  // namespace curvefs
