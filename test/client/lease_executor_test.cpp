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
 * File Created: November 20, 2019
 * Author: wuhanqing
 */

#include "src/client/lease_executor.h"

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "src/client/iomanager4file.h"
#include "src/client/mds_client.h"
#include "test/client/mock/mock_namespace_service.h"

namespace curve {
namespace client {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SaveArgPointee;
using ::testing::SetArgPointee;

static void MockRefreshSession(::google::protobuf::RpcController* controller,
                               const curve::mds::ReFreshSessionRequest* request,
                               curve::mds::ReFreshSessionResponse* response,
                               ::google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);

    response->set_statuscode(curve::mds::StatusCode::kOK);
    response->set_sessionid("");
}

class LeaseExecutorTest : public ::testing::Test {
 protected:
    void SetUp() override {
        MetaServerOption mdsOpt;
        mdsOpt.rpcRetryOpt.addrs.push_back(kSvrAddr);

        userInfo_.owner = "test";

        ASSERT_EQ(0, mdsClient_.Initialize(mdsOpt));
        ASSERT_TRUE(io4File_.Initialize("/test", {}, &mdsClient_));
        ASSERT_EQ(0, server_.AddService(&curveFsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(kSvrAddr, nullptr));
    }

    void TearDown() override {
        ASSERT_NO_THROW(io4File_.UnInitialize());
        ASSERT_NO_THROW(mdsClient_.UnInitialize());

        server_.Stop(0);
        server_.Join();
    }

    void PrepareMockService() {
        curve::mds::FileInfo* fileInfo = new curve::mds::FileInfo();
        fileInfo->set_filestatus(curve::mds::FileStatus::kFileCreated);
        response_.set_allocated_fileinfo(fileInfo);

        EXPECT_CALL(curveFsService_, RefreshSession(_, _, _, _))
            .WillRepeatedly(
                DoAll(SetArgPointee<2>(response_), Invoke(MockRefreshSession)));
    }

 protected:
    const char* kSvrAddr = "127.0.0.1:21001";

    brpc::Server server_;
    curve::mds::MockNameService curveFsService_;
    curve::mds::ReFreshSessionResponse response_;

    MDSClient mdsClient_;
    IOManager4File io4File_;
    UserInfo userInfo_;
    FInfo fi_;
    LeaseOption leaseOpt_;
    LeaseSession lease_;
};

TEST_F(LeaseExecutorTest, TestStartFailed) {
    fi_.fullPathName = "/TestStartFailed";

    {
        leaseOpt_.mdsRefreshTimesPerLease = 100;
        lease_.leaseTime = 0;

        LeaseExecutor exec(leaseOpt_, userInfo_, &mdsClient_, &io4File_);
        ASSERT_FALSE(exec.Start(fi_, lease_));
    }

    {
        leaseOpt_.mdsRefreshTimesPerLease = 0;
        lease_.leaseTime = 1000;

        LeaseExecutor exec(leaseOpt_, userInfo_, &mdsClient_, &io4File_);
        ASSERT_FALSE(exec.Start(fi_, lease_));
    }
}

TEST_F(LeaseExecutorTest, TestStartStop) {
    PrepareMockService();

    fi_.fullPathName = "/TestStartStop";
    fi_.filestatus = FileStatus::Created;

    leaseOpt_.mdsRefreshTimesPerLease = 1;
    lease_.leaseTime = 5000000;

    LeaseExecutor exec(leaseOpt_, userInfo_, &mdsClient_, &io4File_);
    ASSERT_TRUE(exec.Start(fi_, lease_));

    std::this_thread::sleep_for(std::chrono::seconds(20));

    ASSERT_NO_FATAL_FAILURE(exec.Stop());
}

TEST_F(LeaseExecutorTest, TestMultiStop) {
    PrepareMockService();

    fi_.fullPathName = "/TestMultiStop";
    fi_.filestatus = FileStatus::Created;

    leaseOpt_.mdsRefreshTimesPerLease = 1;
    lease_.leaseTime = 5000000;

    LeaseExecutor exec(leaseOpt_, userInfo_, &mdsClient_, &io4File_);
    ASSERT_TRUE(exec.Start(fi_, lease_));

    std::this_thread::sleep_for(std::chrono::seconds(10));

    ASSERT_NO_FATAL_FAILURE(exec.Stop());
    ASSERT_NO_FATAL_FAILURE(exec.Stop());
}

TEST_F(LeaseExecutorTest, TestNoStop) {
    PrepareMockService();

    fi_.fullPathName = "/TestNoStop";
    fi_.filestatus = FileStatus::Created;

    leaseOpt_.mdsRefreshTimesPerLease = 1;
    lease_.leaseTime = 5000000;

    LeaseExecutor exec(leaseOpt_, userInfo_, &mdsClient_, &io4File_);
    ASSERT_TRUE(exec.Start(fi_, lease_));

    std::this_thread::sleep_for(std::chrono::seconds(10));

    // does not explicit call exec.Stop(),
    // because LeaseExecutor's destructor will implicit stop,
    // so this test case will exit success rather than stuck forever
    // if remove implicit stop in LeaseExecutor's destructor
    // and uncomment the following code, this test case will stuck here
    // ASSERT_NO_FATAL_FAILURE(exec.Stop());
}

}  // namespace client
}  // namespace curve
