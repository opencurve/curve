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

#include "curvefs/src/space_allocator/space_alloc_service.h"

#include <brpc/channel.h>
#include <brpc/server.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <type_traits>

#include "curvefs/proto/space.pb.h"
#include "curvefs/test/space_allocator/common.h"
#include "curvefs/test/space_allocator/mock/mock_space_manager.h"

namespace curvefs {
namespace space {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;

const char* kTestIpAddr = "127.0.0.1:56789";  // NOLINT

class SpaceAllocServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        service_.reset(new SpaceAllocServiceImpl(&space_));

        ASSERT_EQ(0, server_.AddService(service_.get(),
                                        brpc::SERVER_DOESNT_OWN_SERVICE));

        ASSERT_EQ(0, server_.Start(kTestIpAddr, nullptr));

        ASSERT_EQ(0, channel_.Init(kTestIpAddr, nullptr));
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    MockSpaceManager space_;
    std::unique_ptr<SpaceAllocServiceImpl> service_;
    brpc::Server server_;
    brpc::Channel channel_;
};

TEST_F(SpaceAllocServiceTest, TestInitSpace) {
    // test failed
    {
        InitSpaceRequest request;
        InitSpaceResponse response;
        auto* fsInfo = request.mutable_fsinfo();
        fsInfo->set_fsid(100);
        fsInfo->set_fsname("TestInitSpace");
        fsInfo->set_rootinodeid(0);
        fsInfo->set_capacity(100 * kGiB);
        fsInfo->set_blocksize(4 * kKiB);
        auto* volume = fsInfo->mutable_volume();
        volume->set_volumesize(100 * kGiB);
        volume->set_blocksize(4 * kKiB);
        volume->set_volumename("TestInitSpaceVolume");
        volume->set_user("test");
        fsInfo->set_mountnum(1);

        EXPECT_CALL(space_, InitSpace(_, _, _, _))
            .WillOnce(Return(SPACE_RELOAD_ERROR));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.InitSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_RELOAD_ERROR, response.status());
    }

    // test succeeded
    {
        InitSpaceRequest request;
        InitSpaceResponse response;
        auto* fsInfo = request.mutable_fsinfo();
        fsInfo->set_fsid(100);
        fsInfo->set_fsname("TestInitSpace");
        fsInfo->set_rootinodeid(0);
        fsInfo->set_capacity(100 * kGiB);
        fsInfo->set_blocksize(4 * kKiB);
        auto* volume = fsInfo->mutable_volume();
        volume->set_volumesize(100 * kGiB);
        volume->set_blocksize(4 * kKiB);
        volume->set_volumename("TestInitSpaceVolume");
        volume->set_user("test");
        fsInfo->set_mountnum(1);

        EXPECT_CALL(space_, InitSpace(_, _, _, _))
            .WillOnce(Return(SPACE_OK));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.InitSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_OK, response.status());
    }
}

TEST_F(SpaceAllocServiceTest, TestUnInitSpace) {
    // test failed
    {
        UnInitSpaceRequest request;
        UnInitSpaceResponse response;

        request.set_fsid(1);

        EXPECT_CALL(space_, UnInitSpace(_))
            .WillOnce(Return(SPACE_NOT_FOUND));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.UnInitSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_NOT_FOUND, response.status());
    }

    // test succeeded
    {
        UnInitSpaceRequest request;
        UnInitSpaceResponse response;

        request.set_fsid(1);

        EXPECT_CALL(space_, UnInitSpace(_))
            .WillOnce(Return(SPACE_OK));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.UnInitSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_OK, response.status());
    }
}

TEST_F(SpaceAllocServiceTest, TestStatSpace) {
    // test failure
    {
        StatSpaceRequest request;
        StatSpaceResponse response;

        request.set_fsid(1);

        EXPECT_CALL(space_, StatSpace(_, _, _, _))
            .WillOnce(Return(SPACE_NOT_FOUND));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.StatSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_NOT_FOUND, response.status());
    }

    // test success
    {
        StatSpaceRequest request;
        StatSpaceResponse response;

        request.set_fsid(1);

        EXPECT_CALL(space_, StatSpace(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<1>(100 * kGiB),
                            SetArgPointee<2>(50 * kGiB),
                            SetArgPointee<3>(4 * kKiB),
                            Return(SPACE_OK)));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.StatSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_OK, response.status());
        EXPECT_EQ(100 * kGiB / (4 * kKiB), response.totalblock());
        EXPECT_EQ(50 * kGiB / (4 * kKiB), response.availableblock());
        EXPECT_EQ(50 * kGiB / (4 * kKiB), response.usedblock());
    }
}

TEST_F(SpaceAllocServiceTest, TestAllocateSpace) {
    // test failure
    {
        AllocateSpaceRequest request;
        AllocateSpaceResponse response;

        request.set_fsid(1);
        request.set_size(8 * kKiB);

        EXPECT_CALL(space_, AllocateSpace(_, _, _, _))
            .WillOnce(Return(SPACE_NOSPACE));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.AllocateSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_NOSPACE, response.status());
    }

    // test success
    {
        AllocateSpaceRequest request;
        AllocateSpaceResponse response;

        request.set_fsid(1);
        request.set_size(8 * kKiB);

        std::vector<PExtent> exts{{12 * kKiB, 8 * kKiB}};

        EXPECT_CALL(space_, AllocateSpace(_, _, _, _))
            .WillOnce(DoAll(SetArgPointee<3>(exts),
                            Return(SPACE_OK)));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.AllocateSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_OK, response.status());
        EXPECT_EQ(1, response.extents_size());
        EXPECT_TRUE(IsEqual(response.extents(), exts));
    }
}

TEST_F(SpaceAllocServiceTest, TestDeallocateSpace) {
    // test failure
    {
        DeallocateSpaceRequest request;
        DeallocateSpaceResponse response;

        request.set_fsid(1);
        auto* e = request.add_extents();
        e->set_offset(12 * kKiB);
        e->set_length(8 * kKiB);

        EXPECT_CALL(space_, DeallocateSpace(_, _))
            .WillOnce(Return(SPACE_NOT_FOUND));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.DeallocateSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_NOT_FOUND, response.status());
    }

    // test success
    {
        DeallocateSpaceRequest request;
        DeallocateSpaceResponse response;

        request.set_fsid(1);
        auto* e = request.add_extents();
        e->set_offset(12 * kKiB);
        e->set_length(8 * kKiB);

        std::remove_cv<
            std::remove_reference<decltype(request.extents())>::type>::type
            protoExts;

        EXPECT_CALL(space_, DeallocateSpace(_, _))
            .WillOnce(DoAll(SaveArg<1>(&protoExts),
                            Return(SPACE_OK)));

        brpc::Controller cntl;
        SpaceAllocService_Stub stub(&channel_);
        stub.DeallocateSpace(&cntl, &request, &response, nullptr);

        EXPECT_FALSE(cntl.Failed());
        EXPECT_EQ(SPACE_OK, response.status());
        EXPECT_EQ(1, protoExts.size());
        EXPECT_EQ(12 * kKiB, protoExts[0].offset());
        EXPECT_EQ(8 * kKiB, protoExts[0].length());
    }
}

}  // namespace space
}  // namespace curvefs

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
