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
 * @Date: 2021-11-23 19:59:16
 * @Author: chenwei
 */

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "curvefs/proto/schedule.pb.h"
#include "curvefs/src/mds/schedule/scheduleService/scheduleService.h"
#include "curvefs/test/mds/mock/mock_coordinator.h"

namespace curvefs {
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
        ASSERT_EQ(
            0, server_->AddService(scheduleService, brpc::SERVER_OWNS_SERVICE));
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

TEST_F(TestScheduleService, test_QueryMetaServerRecoverStatus) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listenAddr_, NULL));

    ScheduleService_Stub stub(&channel);
    QueryMetaServerRecoverStatusRequest request;
    request.add_metaserverid(1);
    QueryMetaServerRecoverStatusResponse response;

    // 1. 查询metaserver恢复状态返回成功
    {
        std::map<MetaServerIdType, bool> expectRes{{1, 1}};
        EXPECT_CALL(*coordinator_, QueryMetaServerRecoverStatus(
                                       std::vector<MetaServerIdType>{1}, _))
            .WillOnce(DoAll(SetArgPointee<1>(expectRes),
                            Return(ScheduleStatusCode::Success)));

        brpc::Controller cntl;
        stub.QueryMetaServerRecoverStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(ScheduleStatusCode::Success, response.statuscode());
        ASSERT_EQ(1, response.recoverstatusmap_size());
        ASSERT_TRUE(response.recoverstatusmap().begin()->second);
    }

    // 2. 传入的metaserverid不合法
    {
        std::map<MetaServerIdType, bool> expectRes{{1, 1}};
        EXPECT_CALL(*coordinator_, QueryMetaServerRecoverStatus(
                                       std::vector<MetaServerIdType>{1}, _))
            .WillOnce(Return(ScheduleStatusCode::InvalidQueryMetaserverID));
        brpc::Controller cntl;
        stub.QueryMetaServerRecoverStatus(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(ScheduleStatusCode::InvalidQueryMetaserverID,
                  response.statuscode());
    }
}

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs
