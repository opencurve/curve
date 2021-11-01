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
 * @Date: 2021-09-27
 * @Author: chengyi01
 */

#include "curvefs/src/tools/umountfs/curvefs_umountfs_tool.h"

#include <brpc/controller.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <queue>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/common/mds_define.h"
#include "curvefs/test/tools/mock_mds_service.h"

DECLARE_string(mdsAddr);
DECLARE_string(mountpoint);

namespace curvefs {
namespace tools {
namespace umountfs {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;

class UmountfsToolTest : public testing::Test {
 protected:
    void SetUp() override {
        ASSERT_EQ(0, server_.AddService(&mockMdsService_,
                                        brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));
    }
    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    std::string addr_ = "127.0.0.1:6701";
    brpc::Server server_;
    MockMdsService mockMdsService_;
    UmountfsTool ut_;
};

void UF(::google::protobuf::RpcController* controller,
        const ::curvefs::mds::UmountFsRequest* request,
        ::curvefs::mds::UmountFsResponse* response,
        ::google::protobuf::Closure* done) {
    done->Run();
}

TEST_F(UmountfsToolTest, test_umount_success) {
    FLAGS_mdsAddr = addr_;
    ::curvefs::mds::UmountFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response), Invoke(UF)));
    int ret = ut_.Run();
    ASSERT_EQ(ret, 0);
}

// mountpoint not exist
TEST_F(UmountfsToolTest, test_umount_failed) {
    FLAGS_mdsAddr = addr_;
    ::curvefs::mds::UmountFsResponse response;
    response.set_statuscode(curvefs::mds::FSStatusCode::MOUNT_POINT_NOT_EXIST);
    EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(response), Invoke(UF)));
    int ret = ut_.Run();
    ASSERT_EQ(ret, -1);
}

// connect to mds failed
TEST_F(UmountfsToolTest, test_umount_connect_failed) {
    FLAGS_mdsAddr = "127.0.0.1:6700";
    int ret = ut_.Run();
    ASSERT_EQ(ret, -1);
}

// init failed
TEST_F(UmountfsToolTest, test_umount_init_name) {
    FLAGS_mdsAddr = "abcd";
    int ret = ut_.Run();
    ASSERT_EQ(ret, -1);
}

// invalid mountpoint
TEST_F(UmountfsToolTest, test_umount_invalid_mountpoint) {
    FLAGS_mdsAddr = addr_;
    FLAGS_mountpoint = "/1234/";
    int ret = ut_.Run();
    ASSERT_EQ(ret, -1);
    FLAGS_mountpoint = "127.0.0.1:/mnt/curvefs-umount-test";
}

// init
TEST_F(UmountfsToolTest, test_umount_tool_init) {
    FLAGS_mdsAddr = addr_;

    std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
    std::shared_ptr<brpc::Controller> controller =
        std::make_shared<brpc::Controller>();
    curvefs::mds::UmountFsRequest request;
    request.set_fsname("123");
    request.set_mountpoint(FLAGS_mountpoint);
    std::queue<curvefs::mds::UmountFsRequest> requestQueue;
    requestQueue.push(request);
    std::shared_ptr<curvefs::mds::UmountFsResponse> response =
        std::make_shared<curvefs::mds::UmountFsResponse>();

    ::curvefs::mds::UmountFsResponse re;
    re.set_statuscode(curvefs::mds::FSStatusCode::OK);
    EXPECT_CALL(mockMdsService_, UmountFs(_, _, _, _))
        .WillRepeatedly(DoAll(SetArgPointee<2>(re), Invoke(UF)));

    std::shared_ptr<curvefs::mds::MdsService_Stub> service_stub =
        std::make_shared<curvefs::mds::MdsService_Stub>(channel.get());

    ut_.CurvefsToolRpc::Init(
        channel, controller, requestQueue, response, service_stub,
        std::bind(&curvefs::mds::MdsService_Stub::UmountFs, service_stub.get(),
                  std::placeholders::_1, std::placeholders::_2,
                  std::placeholders::_3, nullptr));

    int ret = ut_.RunCommand();
    ASSERT_EQ(ret, 0);
}

}  // namespace umountfs
}  // namespace tools
}  // namespace curvefs

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
