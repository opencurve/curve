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
#include "curvefs/test/metaserver/mock_heartbeat_service.h"
#include "curvefs/src/metaserver/storage/storage.h"
#include "curvefs/src/metaserver/resource_statistic.h"

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
    }

    bool GetMetaserverSpaceStatus(MetaServerSpaceStatus* status,
                                  uint64_t ncopysets) {
        HeartbeatOptions options;
        options.resourceCollector = resourceCollector_.get();
        Heartbeat heartbeat;
        heartbeat.Init(options);
        return heartbeat.GetMetaserverSpaceStatus(status, ncopysets);
    }

 protected:
    StorageOptions options_;
    std::unique_ptr<ResourceCollector> resourceCollector_;
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
    HeartbeatOptions options;
    Heartbeat heartbeat;

    options.metaserverId = 1;
    options.metaserverToken = "token";
    options.intervalSec = 1;
    options.timeout = 1000;
    options.ip = "127.0.0.1";
    options.port = 6000;
    options.mdsListenAddr = "127.0.0.1:6710";
    options.copysetNodeManager = &CopysetNodeManager::GetInstance();
    options.storeUri = "local://./metaserver_data/copysets";
    options.fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

    // mds service not start
    ASSERT_EQ(heartbeat.Init(options), 0);
    ASSERT_EQ(heartbeat.Run(), 0);
    sleep(2);
    ASSERT_EQ(heartbeat.Fini(), 0);
}

TEST_F(HeartbeatTest, test_ok) {
    HeartbeatOptions options;
    Heartbeat heartbeat;

    options.metaserverId = 1;
    options.metaserverToken = "token";
    options.intervalSec = 2;
    options.timeout = 1000;
    options.ip = "127.0.0.1";
    options.port = 6000;
    options.mdsListenAddr = "127.0.0.1:6710";
    options.copysetNodeManager = &CopysetNodeManager::GetInstance();
    options.storeUri = "local://./metaserver_data/copysets";
    options.fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    options.resourceCollector = resourceCollector_.get();

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

    ASSERT_EQ(heartbeat.Init(options), 0);
    ASSERT_EQ(heartbeat.Run(), 0);
    sleep(5);
    ASSERT_EQ(heartbeat.Fini(), 0);

    server.Stop(0);
    server.Join();
}

TEST_F(HeartbeatTest, test_fail) {
    HeartbeatOptions options;
    Heartbeat heartbeat;

    options.metaserverId = 1;
    options.metaserverToken = "token";
    options.intervalSec = 2;
    options.timeout = 1000;
    options.ip = "127.0.0.1";
    options.port = 6000;
    options.mdsListenAddr = "127.0.0.1:6710";
    options.copysetNodeManager = &CopysetNodeManager::GetInstance();
    options.storeUri = "local://./metaserver_data/copysets";
    options.fs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");

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

    ASSERT_EQ(heartbeat.Init(options), 0);
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

}  // namespace metaserver
}  // namespace curvefs
