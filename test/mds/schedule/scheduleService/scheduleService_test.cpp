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
 * Created Date: 2020-01-06
 * Author: lixiaocui
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include "src/mds/schedule/scheduleService/scheduleService.h"
#include "test/mds/mock/mock_coordinator.h"
#include "proto/schedule.pb.h"

namespace curve {
namespace mds {
namespace schedule {

using ::testing::Return;
using ::testing::_;
using ::testing::SetArgPointee;
using ::testing::DoAll;

class TestScheduleService : public ::testing::Test {
 protected:
    virtual void SetUp() {
        server_ = new brpc::Server();

        coordinator_ = std::make_shared<MockCoordinator>();
        ScheduleServiceImpl *scheduleService =
            new ScheduleServiceImpl(coordinator_);
        ASSERT_EQ(0,
            server_->AddService(scheduleService, brpc::SERVER_OWNS_SERVICE));
        ASSERT_EQ(0, server_->Start("127.0.0.1", {5900, 5999}, nullptr));
        listenAddr_ = server_->listen_address();
    }

    virtual void TearDown() {
        server_->Stop(0);
        server_->Join();
        delete server_;
        server_ = nullptr;
    }

 protected:
    std::shared_ptr<MockCoordinator> coordinator_;
    butil::EndPoint listenAddr_;
    brpc::Server *server_;
};

TEST_F(TestScheduleService, test_RapidLeaderSchedule) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    ScheduleService_Stub stub(&channel);
    RapidLeaderScheduleRequst request;
    request.set_logicalpoolid(1);
    RapidLeaderScheduleResponse response;

    // 1. Fast leader balance returned successfully
    {
        EXPECT_CALL(*coordinator_, RapidLeaderSchedule(1))
            .WillOnce(Return(kScheduleErrCodeSuccess));
        brpc::Controller cntl;
        stub.RapidLeaderSchedule(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(kScheduleErrCodeSuccess, response.statuscode());
    }

    // 2. The logicaltool passed in does not exist
    {
        EXPECT_CALL(*coordinator_, RapidLeaderSchedule(1))
            .WillOnce(Return(kScheduleErrCodeInvalidLogicalPool));
        brpc::Controller cntl;
        stub.RapidLeaderSchedule(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(kScheduleErrCodeInvalidLogicalPool, response.statuscode());
    }
}

TEST_F(TestScheduleService, test_QueryChunkServerRecoverStatus) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    ScheduleService_Stub stub(&channel);
    QueryChunkServerRecoverStatusRequest request;
    request.add_chunkserverid(1);
    QueryChunkServerRecoverStatusResponse response;

    // 1. Querying the recovery status of chunkserver returned success
    {
        std::map<ChunkServerIdType, bool> expectRes{{1, 1}};
        EXPECT_CALL(*coordinator_, QueryChunkServerRecoverStatus(
            std::vector<ChunkServerIdType>{1}, _))
            .WillOnce(DoAll(SetArgPointee<1>(expectRes),
                Return(kScheduleErrCodeSuccess)));

        brpc::Controller cntl;
        stub.QueryChunkServerRecoverStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(kScheduleErrCodeSuccess, response.statuscode());
        ASSERT_EQ(1, response.recoverstatusmap_size());
        ASSERT_TRUE(response.recoverstatusmap().begin()->second);
    }

    // 2. The chunkserverid passed in is illegal
    {
        std::map<ChunkServerIdType, bool> expectRes{{1, 1}};
        EXPECT_CALL(*coordinator_, QueryChunkServerRecoverStatus(
            std::vector<ChunkServerIdType>{1}, _))
            .WillOnce(Return(kScheduleErrInvalidQueryChunkserverID));
        brpc::Controller cntl;
        stub.QueryChunkServerRecoverStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(kScheduleErrInvalidQueryChunkserverID, response.statuscode());
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve
