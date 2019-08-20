/*
 * Project: curve
 * Created Date: Monday October 15th 2018
 * Author: hzsunjianliang
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "test/mds/nameserver2/mock_chunk_id_generator.h"
#include "test/mds/nameserver2/mock_topology_admin.h"
#include "src/mds/nameserver2/chunk_allocator.h"
#include "src/mds/common/mds_define.h"

using ::testing::Return;
using ::testing::_;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::AtLeast;
using ::curve::mds::topology::CopysetIdInfo;
using ::curve::mds::topology::PoolIdType;

namespace curve {
namespace mds {

const uint64_t DefaultChunkSize = 16 * kMB;

class ChunkAllocatorTest: public ::testing::Test {
 protected:
    void SetUp() override {
        mockChunkIDGenerator_ = std::make_shared<MockChunkIDGenerator>();
        mockTopologyAdmin_ = std::make_shared<MOCKTopologyAdmin1>();
    }
    void TearDown() override {
        mockChunkIDGenerator_ = nullptr;
        mockTopologyAdmin_ = nullptr;
    }
    std::shared_ptr<MockChunkIDGenerator> mockChunkIDGenerator_;
    std::shared_ptr<MOCKTopologyAdmin1> mockTopologyAdmin_;
};

TEST_F(ChunkAllocatorTest, testcase1) {
    auto impl = std::make_shared<ChunkSegmentAllocatorImpl>(
                                mockTopologyAdmin_,
                                mockChunkIDGenerator_);
    // test segment pointer == nullptr
    ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
        DefaultSegmentSize, DefaultChunkSize, 0, nullptr), false);

    // test offset not align with segmentsize
    PageFileSegment segment;
    ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
        DefaultSegmentSize, DefaultChunkSize, 1, &segment), false);

    // test  chunkSize not align with segmentsize
    ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
        DefaultSegmentSize, DefaultChunkSize - 1, 0, &segment), false);

    // test  topologyAdmin_AllocateChunkRoundRobinInSingleLogicalPool
    // return false
    {
        PageFileSegment segment;

        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(Return(false));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            DefaultSegmentSize, DefaultChunkSize, 0, &segment), false);
    }

    // test topologyAdmin_ Allocate return size error
    {
        PageFileSegment segment;

        std::vector<CopysetIdInfo> copysetInfos;
        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(copysetInfos),
            Return(true)));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            DefaultSegmentSize, DefaultChunkSize, 0, &segment), false);
    }

    // test   GenChunkID error
    {
        PoolIdType logicalPoolID = 1;
        PageFileSegment segment;
        std::vector<CopysetIdInfo> copysetInfos;
        for (int i = 0; i != DefaultSegmentSize/DefaultChunkSize; i++) {
            CopysetIdInfo info = {logicalPoolID, i};
            copysetInfos.push_back(info);
        }

        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(copysetInfos),
            Return(true)));

        EXPECT_CALL(*mockChunkIDGenerator_, GenChunkID(_))
        .Times(1)
        .WillOnce(Return(false));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            DefaultSegmentSize, DefaultChunkSize, 0, &segment), false);
    }

    // test ok
    {
        PoolIdType logicalPoolID = 1;
        PageFileSegment segment;
        std::vector<CopysetIdInfo> copysetInfos;
        for (int i = 0; i != DefaultSegmentSize/DefaultChunkSize; i++) {
            CopysetIdInfo info = {logicalPoolID, i};
            copysetInfos.push_back(info);
        }

        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(copysetInfos),
            Return(true)));

        EXPECT_CALL(*mockChunkIDGenerator_, GenChunkID(_))
        .Times(1)
        .WillOnce(Return(false));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            DefaultSegmentSize, DefaultChunkSize, 0, &segment), false);
    }

    // test logicalid not same
    {
        PageFileSegment segment;
        PoolIdType logicalPoolID = 1;
        std::vector<CopysetIdInfo> copysetInfos;

        uint64_t segmentSize = DefaultChunkSize*2;

        for (int i = 0; i != segmentSize/DefaultChunkSize; i++) {
            CopysetIdInfo info = {i, i};
            copysetInfos.push_back(info);
        }

        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(copysetInfos),
            Return(true)));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            segmentSize, DefaultChunkSize, 0, &segment), false);
    }


    // test ok
    {
        PageFileSegment segment;
        PoolIdType logicalPoolID = 1;
        std::vector<CopysetIdInfo> copysetInfos;

        uint64_t segmentSize = DefaultChunkSize*2;

        for (int i = 0; i != segmentSize/DefaultChunkSize; i++) {
            CopysetIdInfo info = {logicalPoolID, i};
            copysetInfos.push_back(info);
        }

        EXPECT_CALL(*mockTopologyAdmin_,
            AllocateChunkRoundRobinInSingleLogicalPool(_, _,  _, _))
            .Times(1)
            .WillOnce(DoAll(SetArgPointee<3>(copysetInfos),
            Return(true)));

        EXPECT_CALL(*mockChunkIDGenerator_, GenChunkID(_))
        .Times(AtLeast(segmentSize/DefaultChunkSize))
        .WillRepeatedly(DoAll(SetArgPointee<0>(1), Return(true)));

        ASSERT_EQ(impl->AllocateChunkSegment(FileType::INODE_PAGEFILE,
            segmentSize, DefaultChunkSize, 0, &segment), true);

        PageFileSegment expectSegment;
        expectSegment.set_chunksize(DefaultChunkSize);
        expectSegment.set_segmentsize(segmentSize);
        expectSegment.set_startoffset(0);
        expectSegment.set_logicalpoolid(logicalPoolID);
        for (uint32_t i = 0; i < segmentSize/DefaultChunkSize ; i++) {
            PageFileChunkInfo* chunkinfo =  expectSegment.add_chunks();
            chunkinfo->set_chunkid(1);
            chunkinfo->set_copysetid(i);
            LOG(INFO) << "chunkid = " << 1 << ", copysetid = " << i;
        }
        ASSERT_EQ(segment.SerializeAsString(),
            expectSegment.SerializeAsString());
    }
}
}  // namespace mds
}  // namespace curve
