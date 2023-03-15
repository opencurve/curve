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
 * @Date: 2021-09-29 11:22:21
 * @Author: chenwei
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "curvefs/src/metaserver/heartbeat.h"
#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/test/metaserver/mock_heartbeat_service.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/resource_statistic.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node_manager.h"
#include "curvefs/test/metaserver/copyset/mock/mock_copyset_node.h"

using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SaveArg;
using ::testing::Invoke;
using ::curvefs::mds::heartbeat::MockHeartbeatService;
using ::curvefs::mds::heartbeat::MetaServerHeartbeatRequest;
using ::curvefs::mds::heartbeat::MetaServerHeartbeatResponse;
using ::curvefs::mds::heartbeat::HeartbeatStatusCode;
using ::curve::fs::FileSystemType;
using ::curve::fs::LocalFsFactory;
using ::curvefs::metaserver::storage::StorageOptions;
using ::curvefs::metaserver::copyset::MockCopysetNodeManager;
using ::curvefs::metaserver::copyset::MockCopysetNode;
using ::curvefs::mds::heartbeat::BlockGroupDeallcateStatusCode;

namespace curvefs {
namespace metaserver {
class HeartbeatTest : public ::testing::Test {
 public:
    void SetUp() override {
        options_.type = "rocksdb";
        options_.dataDir = "/tmp";
        options_.maxMemoryQuotaBytes = 1024;
        options_.maxDiskQuotaBytes = 10240;

        resourceCollector_ = absl::make_unique<ResourceCollector>(
            options_.maxDiskQuotaBytes, options_.maxMemoryQuotaBytes,
            options_.dataDir);

        GetHeartbeatOption(&hbopts_);
    }

    bool GetMetaserverSpaceStatus(MetaServerSpaceStatus* status,
                                  uint64_t ncopysets) {
        HeartbeatOptions options;
        options.resourceCollector = resourceCollector_.get();
        Heartbeat heartbeat;
        heartbeat.Init(options);
        return heartbeat.GetMetaserverSpaceStatus(status, ncopysets);
    }

    void GetHeartbeatOption(HeartbeatOptions *opt) {
        opt->metaserverId = 1;
        opt->metaserverToken = "token";
        opt->intervalSec = 2;
        opt->timeout = 1000;
        opt->ip = "127.0.0.1";
        opt->port = 6000;
        opt->mdsListenAddr = "127.0.0.1:6710";
        opt->copysetNodeManager = &mockCopysetManager_;
        opt->storeUri = "local://./metaserver_data/copysets";
        opt->fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
        opt->resourceCollector = resourceCollector_.get();
    }

 protected:
    StorageOptions options_;
    std::unique_ptr<ResourceCollector> resourceCollector_;
    HeartbeatOptions hbopts_;
    MockCopysetNodeManager mockCopysetManager_;
};

template <typename RpcRequestType, typename RpcResponseType,
          bool RpcFailed = false>
void RpcService(google::protobuf::RpcController *cntl_base,
                const RpcRequestType *request, RpcResponseType *response,
                google::protobuf::Closure *done) {
    if (RpcFailed) {
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
        cntl->SetFailed(112, "Not connected to");
    }
    done->Run();
}

TEST_F(HeartbeatTest, testInitFail) {
    HeartbeatOptions options;
    Heartbeat heartbeat;
    options.storeUri = "local://metaserver_data/copysets";

    // IP not ok
    ASSERT_EQ(heartbeat.Init(options), -1);

    // mds addr empty
    options.ip = "127.0.0.1";
    options.port = 6000;
    ASSERT_EQ(heartbeat.Init(options), -1);

    // mds addr not ok
    options.mdsListenAddr = "127.0.0.1000:6001";
    ASSERT_EQ(heartbeat.Init(options), -1);

    // init ok
    options.mdsListenAddr = "127.0.0.1:6001";
    ASSERT_EQ(heartbeat.Init(options), 0);
}

TEST_F(HeartbeatTest, test1) {
    Heartbeat heartbeat;

    // mds service not start
    ASSERT_EQ(heartbeat.Init(hbopts_), 0);
    ASSERT_EQ(heartbeat.Run(), 0);
    sleep(2);
    ASSERT_EQ(heartbeat.Fini(), 0);
}

TEST_F(HeartbeatTest, test_ok) {
    Heartbeat heartbeat;

    // send heartbeat ok
    brpc::Server server;
    MockHeartbeatService mockHeartbeatService;
    std::string mdsServiceAddr = "127.0.0.1:6710";
    ASSERT_EQ(0, server.AddService(&mockHeartbeatService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(mdsServiceAddr.c_str(), nullptr));

    MetaServerHeartbeatResponse response;
    response.set_statuscode(HeartbeatStatusCode::hbOK);
    EXPECT_CALL(mockHeartbeatService, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<MetaServerHeartbeatRequest,
                                          MetaServerHeartbeatResponse>)));

    ASSERT_EQ(heartbeat.Init(hbopts_), 0);
    ASSERT_EQ(heartbeat.Run(), 0);
    sleep(5);
    ASSERT_EQ(heartbeat.Fini(), 0);

    server.Stop(0);
    server.Join();
}

TEST_F(HeartbeatTest, test_fail) {
    Heartbeat heartbeat;

    // send heartbeat ok
    brpc::Server server;
    MockHeartbeatService mockHeartbeatService;
    std::string mdsServiceAddr = "127.0.0.1:6710";
    ASSERT_EQ(0, server.AddService(&mockHeartbeatService,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server.Start(mdsServiceAddr.c_str(), nullptr));

    MetaServerHeartbeatResponse response;
    response.set_statuscode(HeartbeatStatusCode::hbMetaServerUnknown);
    EXPECT_CALL(mockHeartbeatService, MetaServerHeartbeat(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response),
                        Invoke(RpcService<MetaServerHeartbeatRequest,
                                          MetaServerHeartbeatResponse>)));

    ASSERT_EQ(heartbeat.Init(hbopts_), 0);
    ASSERT_EQ(heartbeat.Run(), 0);
    sleep(5);
    ASSERT_EQ(heartbeat.Fini(), 0);

    server.Stop(0);
    server.Join();
}

TEST_F(HeartbeatTest, GetMetaServerSpaceStatusTest) {
    StorageStatistics statistics;
    bool succ = resourceCollector_->GetResourceStatistic(&statistics);
    ASSERT_TRUE(succ);
    ASSERT_EQ(statistics.maxMemoryQuotaBytes, options_.maxMemoryQuotaBytes);
    ASSERT_EQ(statistics.maxDiskQuotaBytes, options_.maxDiskQuotaBytes);
    ASSERT_GT(statistics.diskUsageBytes, 0);
    ASSERT_GT(statistics.memoryUsageBytes, 0);

    MetaServerSpaceStatus status;
    succ = GetMetaserverSpaceStatus(&status, 1);
    ASSERT_TRUE(succ);
    LOG(INFO) << "metaserver space status: " << status.ShortDebugString();
    ASSERT_EQ(status.memorythresholdbyte(), options_.maxMemoryQuotaBytes);
    ASSERT_EQ(status.diskthresholdbyte(), options_.maxDiskQuotaBytes);
    ASSERT_GT(status.memoryusedbyte(), 0);
    ASSERT_GT(status.diskusedbyte(), 0);
    ASSERT_EQ(status.diskcopysetminrequirebyte(), status.diskusedbyte());
    ASSERT_EQ(status.memorycopysetminrequirebyte(),
              status.memoryusedbyte());
}

TEST_F(HeartbeatTest, Test_BuildRequest) {
    Heartbeat heartbeat;
    ASSERT_EQ(heartbeat.Init(hbopts_), 0);

    MockCopysetNode mockCopysetNode;
    std::vector<CopysetNode *> copysetVec;
    copysetVec.emplace_back(&mockCopysetNode);

    BlockGroupStatInfoMap blockGroupStatInfoMap;
    uint32_t fsId = 1;
    auto &statInfo = blockGroupStatInfoMap[fsId];
    statInfo.set_fsid(1);
    auto de = statInfo.add_deallocatableblockgroups();
    de->set_blockgroupoffset(0);
    de->set_deallocatablesize(1024);

    EXPECT_CALL(mockCopysetManager_, GetAllCopysets(_))
        .WillOnce(SetArgPointee<0>(copysetVec));
    EXPECT_CALL(mockCopysetNode, GetPoolId()).WillOnce(Return(1));
    EXPECT_CALL(mockCopysetNode, GetCopysetId()).WillOnce(Return(1));
    EXPECT_CALL(mockCopysetNode, ListPeers(_)).Times(1);
    EXPECT_CALL(mockCopysetNode, GetLeaderId()).Times(1);
    EXPECT_CALL(mockCopysetNode, IsLoading()).WillRepeatedly(Return(false));
    EXPECT_CALL(mockCopysetNode, GetPartitionInfoList(_))
        .WillOnce(Return(true));
    EXPECT_CALL(mockCopysetNode, GetConfChange(_, _)).Times(1);
    EXPECT_CALL(mockCopysetNode, GetConfEpoch()).WillOnce(Return(1));
    EXPECT_CALL(mockCopysetNode, IsLeaderTerm()).WillOnce(Return(true));
    EXPECT_CALL(mockCopysetNode, GetBlockStatInfo(_))
        .WillOnce(DoAll(SetArgPointee<0>(blockGroupStatInfoMap), Return(true)));

    HeartbeatRequest req;
    heartbeat.taskExecutor_->SetDeallocFsId(1);
    heartbeat.BuildRequest(&req);

    // assert block group stat info
    {
        // assert deallocatableBlockGroups
        auto outStatInfos = req.blockgroupstatinfos();
        ASSERT_EQ(outStatInfos.size(), 1);

        auto outOne = outStatInfos[0];
        ASSERT_EQ(outOne.fsid(), fsId);

        auto outDes = outOne.deallocatableblockgroups();
        ASSERT_EQ(outDes.size(), 1);

        auto outDesOne = outDes[0];
        ASSERT_EQ(outDesOne.blockgroupoffset(), de->blockgroupoffset());
        ASSERT_EQ(outDesOne.deallocatablesize(), de->deallocatablesize());
        ASSERT_TRUE(outDesOne.inodeidlist().empty());
        ASSERT_TRUE(outDesOne.inodeidunderdeallocate().empty());

        // assert blockGroupDeallocateStatus
        auto outStatus = outOne.blockgroupdeallocatestatus();
        ASSERT_EQ(outStatus.size(), 1);

        auto outStatusOne = outStatus[fsId];
        ASSERT_EQ(outStatusOne, BlockGroupDeallcateStatusCode::BGDP_DONE);
    }
}
}  // namespace metaserver
}  // namespace curvefs
