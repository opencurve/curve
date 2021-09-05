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
#include "curvefs/test/metaserver/mock_s3_adapter.h"
#include "curvefs/test/metaserver/mock_s3compactwq_impl.h"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::StrEq;

namespace curvefs {
namespace metaserver {
class S3CompactWorkQueueImplTest : public ::testing::Test {
 public:
    void SetUp() override {
        opts_.enable = true;
        opts_.threadNum = 1;
        opts_.queueSize = 1;
        opts_.blockSize = 4;
        opts_.chunkSize = 64;
        opts_.fragmentThreshold = 20;
        opts_.maxChunksPerCompact = 10;
        s3adapter_ = std::make_shared<MockS3Adapter>();
        inodeStorage_ = std::make_shared<MemoryInodeStorage>();
        impl_ = std::make_shared<S3CompactWorkQueueImpl>(s3adapter_, opts_);
        mockImpl_ =
            std::make_shared<MockS3CompactWorkQueueImpl>(s3adapter_, opts_);
    }

    void TearDown() override {
        return;
    }

 protected:
    S3CompactWorkQueueOption opts_;
    std::shared_ptr<MockS3Adapter> s3adapter_;
    std::shared_ptr<InodeStorage> inodeStorage_;
    std::shared_ptr<S3CompactWorkQueueImpl> impl_;
    std::shared_ptr<MockS3CompactWorkQueueImpl> mockImpl_;
    MockCopysetNodeWrapper mockCopysetNodeWrapper_;
};

TEST_F(S3CompactWorkQueueImplTest, test_GetNeedCWrapperompact) {
    // no need compact
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3chunkinfoMap;
    S3ChunkInfoList l1;
    for (int i = 0; i < 10; i++) {
        auto ref = l1.add_s3chunks();
        ref->set_chunkid(i);
    }
    s3chunkinfoMap.insert({1, l1});
    ASSERT_TRUE(impl_->GetNeedCompact(s3chunkinfoMap).empty());

    // need compact
    S3ChunkInfoList l2;
    for (int i = 0; i < 30; i++) {
        auto ref = l2.add_s3chunks();
        ref->set_chunkid(i);
    }
    s3chunkinfoMap.insert({2, l2});
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap).size(), 1);

    // too much need compact, control size
    for (int j = 3; j < 20; j++) {
        S3ChunkInfoList l;
        for (int i = 0; i < 30; i++) {
            auto ref = l.add_s3chunks();
            ref->set_chunkid(i);
        }
        s3chunkinfoMap.insert({j, l});
    }
    ASSERT_EQ(impl_->GetNeedCompact(s3chunkinfoMap).size(),
              opts_.maxChunksPerCompact);
}

TEST_F(S3CompactWorkQueueImplTest, test_DeleteObjs) {
    std::vector<std::string> objs;
    objs.emplace_back("obj1");
    objs.emplace_back("obj2");
    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    impl_->DeleteObjs(objs);
}

TEST_F(S3CompactWorkQueueImplTest, test_BuildValidList) {
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    uint64_t inodeLen = 100;
    S3ChunkInfo tmpl;
    tmpl.set_compaction(0);
    tmpl.set_zero(false);

    S3ChunkInfoList l;

    // empty add one
    auto c1(tmpl);
    c1.set_chunkid(0);
    c1.set_offset(0);
    c1.set_len(1);
    *l.add_s3chunks() = c1;
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 1);
    auto first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    l.Clear();
    c1.set_chunkid(0);
    c1.set_offset(0);
    c1.set_len(200);  // bigger than inodeLen
    *l.add_s3chunks() = c1;
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 1);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 100);

    // B contains A
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 1);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 2);

    // A contains B
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 3);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    auto next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 2);
    next = std::next(next);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 2);
    ASSERT_EQ(next->end, 3);

    // no overlap, B behind A
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 2);

    // no overlap, B front A
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 2);

    // overlap A, B
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 0);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 1);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 1);
    ASSERT_EQ(next->begin, 1);
    ASSERT_EQ(next->end, 3);

    // overlap B, A
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
    validList = std::move(impl_->BuildValidList(l, inodeLen));
    ASSERT_EQ(validList.size(), 2);
    first = validList.begin();
    ASSERT_EQ(first->chunkid, 1);
    ASSERT_EQ(first->begin, 0);
    ASSERT_EQ(first->end, 2);
    next = std::next(first);
    ASSERT_EQ(next->chunkid, 0);
    ASSERT_EQ(next->begin, 2);
    ASSERT_EQ(next->end, 3);
}

TEST_F(S3CompactWorkQueueImplTest, test_ReadFullChunk) {
    int ret;
    std::list<struct S3CompactWorkQueueImpl::Node> validList;
    std::uint64_t inodeId = 1;
    std::uint64_t blockSize = 4;
    std::string fullChunk;
    std::uint64_t newChunkid = 0;
    std::uint64_t newCompaction = 0;

    auto reset = [&]() {
        validList.clear();
        fullChunk.clear();
        newChunkid = 0;
        newCompaction = 0;
    };

    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    auto mock_getobj = [&](const Aws::String& key, std::string* data) {
        data->clear();
        data->append(blockSize, '\0');
        return 0;
    };
    EXPECT_CALL(*s3adapter_, GetObject(_, _))
        .WillRepeatedly(testing::Invoke(mock_getobj));

    validList.emplace_back(0, 1, 0, 0, 0, true);
    ret = impl_->ReadFullChunk(validList, inodeId, blockSize, &fullChunk,
                               &newChunkid, &newCompaction);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(newChunkid, 0);
    ASSERT_EQ(newCompaction, 1);

    reset();
    validList.emplace_back(0, 1, 1, 1, 0, false);
    validList.emplace_back(1, 11, 0, 0, 0, false);
    validList.emplace_back(13, 14, 2, 0, 0, false);
    ret = impl_->ReadFullChunk(validList, inodeId, blockSize, &fullChunk,
                               &newChunkid, &newCompaction);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(newChunkid, 2);
    ASSERT_EQ(newCompaction, 1);
    ASSERT_EQ(fullChunk.size(), 14);

    reset();
    EXPECT_CALL(*s3adapter_, GetObject(_, _)).WillRepeatedly(Return(-1));
    validList.emplace_back(0, 1, 1, 1, 0, false);
    ret = impl_->ReadFullChunk(validList, inodeId, blockSize, &fullChunk,
                               &newChunkid, &newCompaction);
    ASSERT_EQ(ret, -1);
}

TEST_F(S3CompactWorkQueueImplTest, test_WriteFullChunk) {
    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(0));
    std::string fullChunk(10, '0');
    std::vector<std::string> objsAdded;
    int ret = impl_->WriteFullChunk(fullChunk, 100, 4, 2, 3, &objsAdded);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(objsAdded[0], "2_0_3_100");
    ASSERT_EQ(objsAdded[1], "2_1_3_100");
    ASSERT_EQ(objsAdded[2], "2_2_3_100");

    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(-1));
    ret = impl_->WriteFullChunk(fullChunk, 100, 4, 2, 3, &objsAdded);
    ASSERT_EQ(ret, -1);
}

TEST_F(S3CompactWorkQueueImplTest, test_compactChunks) {
    Inode tmp;
    auto mock_updateinode = [&](CopysetNode* copysetNode,
                                const PartitionInfo& pinfo,
                                const Inode& inode) {
        tmp = inode;
        return MetaStatusCode::OK;
    };
    EXPECT_CALL(*mockImpl_, UpdateInode(_, _, _))
        .WillRepeatedly(testing::Invoke(mock_updateinode));
    EXPECT_CALL(*s3adapter_, PutObject(_, _)).WillRepeatedly(Return(0));
    EXPECT_CALL(*s3adapter_, DeleteObject(_)).WillRepeatedly(Return(0));
    auto mock_getobj = [&](const Aws::String& key, std::string* data) {
        data->clear();
        data->append(opts_.blockSize, '\0');
        return 0;
    };
    EXPECT_CALL(*s3adapter_, GetObject(_, _))
        .WillRepeatedly(testing::Invoke(mock_getobj));

    EXPECT_CALL(mockCopysetNodeWrapper_, IsLeaderTerm())
        .WillRepeatedly(Return(true));

    PartitionInfo partitioninfo_;
    // inode not exist
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(0, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
    // s3chunkinfomap size 0
    Inode inode1;
    inode1.set_fsid(1);
    inode1.set_inodeid(1);
    inode1.set_length(60);
    inode1.set_nlink(1);
    ::google::protobuf::Map<uint64_t, S3ChunkInfoList> s3chunkinfoMap;
    *inode1.mutable_s3chunkinfomap() = s3chunkinfoMap;
    ASSERT_EQ(inodeStorage_->Insert(inode1), MetaStatusCode::OK);
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(1, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
    // normal
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
    (*inode1.mutable_s3chunkinfomap())[0] = l0;
    ASSERT_EQ(inodeStorage_->Update(inode1), MetaStatusCode::OK);
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(1, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
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
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(1, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
    EXPECT_CALL(*mockImpl_, UpdateInode(_, _, _))
        .WillRepeatedly(Return(MetaStatusCode::UNKNOWN_ERROR));
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(1, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
    // copysetnode is not leader
    EXPECT_CALL(mockCopysetNodeWrapper_, IsLeaderTerm())
        .WillRepeatedly(Return(true));
    mockImpl_->CompactChunks(inodeStorage_, InodeKey(1, 1), partitioninfo_,
                             &mockCopysetNodeWrapper_);
}
}  // namespace metaserver
}  // namespace curvefs
