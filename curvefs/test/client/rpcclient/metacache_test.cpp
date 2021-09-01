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
 * Project: curve
 * Created Date: Mon Sept 5 2021
 * Author: lixiaocui
 */

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/src/client/rpcclient/metacache.h"
#include "curvefs/test/client/rpcclient/mock_mds_client.h"
#include "curvefs/test/client/rpcclient/mock_cli2_client.h"
#include "curvefs/proto/common.pb.h";

namespace curvefs {
namespace client {
namespace rpcclient {
using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

using ::curvefs::common::PartitionInfo;
class MetaCacheTest : public testing::Test {
 protected:
    void SetUp() override {
        // init
        mockMdsClient_ = std::make_shared<MockMdsClient>();
        mockCli2Client_ = std::make_shared<MockCli2Client>();
        metaCache_.Init(opt_, mockCli2Client_, mockMdsClient_);

        // add item to metaserver list
        curve::client::PeerAddr pd1;
        pd1.Parse("10.182.26.2:9120:0");
        curve::client::PeerAddr pd2;
        pd2.Parse("127.0.0.1:9120:0");

        curve::client::CopysetPeerInfo<MetaserverID> peerinfo_1(1, pd1, pd2);
        metaServerList_.AddCopysetPeerInfo(peerinfo_1);

        pd1.addr_.port = 9121;
        pd2.addr_.port = 9121;
        curve::client::CopysetPeerInfo<MetaserverID> peerinfo_2(2, pd1, pd2);
        metaServerList_.AddCopysetPeerInfo(peerinfo_2);

        pd1.addr_.port = 9122;
        pd2.addr_.port = 9122;
        curve::client::CopysetPeerInfo<MetaserverID> peerinfo_3(3, pd1, pd2);
        metaServerList_.AddCopysetPeerInfo(peerinfo_3);
        metaServerList_.UpdateLeaderIndex(0);

        // add item to partition list
        PartitionInfo pInfo;
        pInfo.set_fsid(1);
        pInfo.set_poolid(1);
        pInfo.set_copysetid(1);
        pInfo.set_partitionid(1);
        pInfo.set_start(1);
        pInfo.set_end(10);
        pInfoList_.emplace_back(pInfo);

        PartitionInfo pInfo2;
        pInfo2.CopyFrom(pInfo);
        pInfo2.set_start(10);
        pInfo2.set_end(20);
        pInfoList2_.emplace_back(pInfo2);

        expect.groupID = std::move(CopysetGroupID(1, 1));
        expect.partitionID = 1;
        expect.metaServerID = 1;
        str2endpoint("127.0.0.1", 9120, &expect.endPoint);
    }

    void TearDown() override {}

    bool CopysetTargetEQ(const CopysetTarget &t1, const CopysetTarget &t2) {
        Print(t1);
        Print(t2);
        return t1.groupID.poolID == t2.groupID.poolID &&
               t1.groupID.copysetID == t2.groupID.copysetID &&
               t1.partitionID == t2.partitionID &&
               t1.metaServerID == t2.metaServerID &&
               std::string(butil::endpoint2str(t1.endPoint).c_str()) ==
                   std::string(butil::endpoint2str(t2.endPoint).c_str());
    }

    void Print(CopysetTarget t1) {
        LOG(INFO) << t1.groupID.poolID << ", " << t1.groupID.copysetID << ","
                  << t1.partitionID << "," << t1.metaServerID << ","
                  << butil::endpoint2str(t1.endPoint).c_str();
    }

 protected:
    MetaCache metaCache_;
    MetaCacheOpt opt_;
    std::shared_ptr<MockMdsClient> mockMdsClient_;
    std::shared_ptr<MockCli2Client> mockCli2Client_;

    curve::client::CopysetInfo<MetaserverID> metaServerList_;
    MetaCache::PatitionInfoList pInfoList_;
    MetaCache::PatitionInfoList pInfoList2_;

    CopysetTarget expect;
};

TEST_F(MetaCacheTest, test_GetTarget) {
    // in
    uint32_t fsID = 1;
    uint64_t inodeID = 1;
    CopysetGroupID groupID(1, 1);

    // out
    CopysetTarget target;
    uint64_t applyIndex;

    // test1: get target fail, fsid->partitionlist not exist
    bool ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_FALSE(ret);

    // test2: get target fail, fsid->paritionlist do not find parition
    metaCache_.UpdatePartitionInfo(fsID, pInfoList2_);
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_FALSE(ret);

    // test3: get target fail, copysetid->copysetinfo not exist
    // set copset(1,1)->peerList
    metaCache_.UpdatePartitionInfo(fsID, pInfoList_);
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_FALSE(ret);

    // test4: get target ok
    // set fsID(1)->partitionInfoList
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(CopysetTargetEQ(target, expect));

    // test5: get target ok, need refresh from metaserver
    curve::client::PeerAddr pd;
    pd.Parse("127.0.0.1:9120:0");
    metaServerList_.UpdateLeaderIndex(-1);
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    EXPECT_CALL(*mockCli2Client_.get(), GetLeader(_, _, _, _, _, _))
        .WillOnce(DoAll(SetArgPointee<4>(pd), Return(true)));
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(CopysetTargetEQ(target, expect));

    // test6: get target ok, need refresh from mds
    metaServerList_.UpdateLeaderIndex(-1);
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    EXPECT_CALL(*mockCli2Client_.get(), GetLeader(_, _, _, _, _, _))
        .WillOnce(Return(false));
    auto tmpMetaServerList = metaServerList_;
    tmpMetaServerList.UpdateLeaderIndex(0);
    std::vector<CopysetInfo<MetaserverID>> metaServerInfos;
    metaServerInfos.push_back(tmpMetaServerList);
    EXPECT_CALL(*mockMdsClient_.get(), GetMetaServerListInCopysets(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(metaServerInfos), Return(true)));
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(CopysetTargetEQ(target, expect));

    // test7: get target fail
    metaServerList_.UpdateLeaderIndex(-1);
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    EXPECT_CALL(*mockCli2Client_.get(), GetLeader(_, _, _, _, _, _))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*mockMdsClient_.get(), GetMetaServerListInCopysets(_, _, _))
        .WillRepeatedly(Return(false));
    ret = metaCache_.GetTarget(fsID, inodeID, &target, &applyIndex);
    ASSERT_FALSE(ret);
}

TEST_F(MetaCacheTest, test_SelectTarget) {
    // in
    uint32_t fsID = 1;
    CopysetGroupID groupID(1, 1);

    // out
    CopysetTarget target;
    uint64_t applyIndex;

    // test1: fsid->partition info list empty, select fail
    bool ret = metaCache_.SelectTarget(fsID, &target, &applyIndex);
    ASSERT_FALSE(ret);

    // test2: copsetid->copyset info empty, select fail
    metaCache_.UpdatePartitionInfo(fsID, pInfoList2_);
    ret = metaCache_.SelectTarget(fsID, &target, &applyIndex);
    ASSERT_FALSE(ret);

    // test3: select ok
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    ret = metaCache_.SelectTarget(fsID, &target, &applyIndex);
    ASSERT_TRUE(ret);
}

TEST_F(MetaCacheTest, test_UpdateAndGetApplyIndex) {
    // in
    CopysetGroupID groupID(1, 1);
    uint64_t applyIndex = 100;

    // test1: no copyset
    metaCache_.UpdateApplyIndex(groupID, applyIndex);
    ASSERT_EQ(0, metaCache_.GetApplyIndex(groupID));

    // test2: update ok
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    metaCache_.UpdateApplyIndex(groupID, applyIndex);
    ASSERT_EQ(100, metaCache_.GetApplyIndex(groupID));
}
TEST_F(MetaCacheTest, test_IsLeaderMayChange) {
    // in
    CopysetGroupID groupID(1, 1);

    // test1: no copyset
    ASSERT_FALSE(metaCache_.IsLeaderMayChange(groupID));

    // test2: leader not change
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    ASSERT_FALSE(metaCache_.IsLeaderMayChange(groupID));

    // test3: leader change
    metaServerList_.SetLeaderUnstableFlag();
    metaCache_.UpdateCopysetInfo(groupID, metaServerList_);
    ASSERT_TRUE(metaCache_.IsLeaderMayChange(groupID));
}

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
