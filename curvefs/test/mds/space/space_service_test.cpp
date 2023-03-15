/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Date: Wednesday Mar 23 14:47:45 CST 2022
 * Author: wuhanqing
 */

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "absl/memory/memory.h"
#include "curvefs/src/mds/space/service.h"
#include "curvefs/test/mds/mock/mock_space_manager.h"
#include "curvefs/test/mds/mock/mock_volume_space.h"
#include "curvefs/test/utils/protobuf_message_utils.h"
#include "src/common/macros.h"

namespace curvefs {
namespace mds {
namespace space {

using ::curvefs::test::GenerateAnDefaultInitializedMessage;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::Matcher;

static constexpr uint32_t kRetryTimes = 10;
static unsigned int seed = time(nullptr);

class SpaceServiceTest : public ::testing::Test {
 protected:
    void SetUp() override {
        service_ = absl::make_unique<SpaceServiceImpl>(&spaceMgr_);
        server_.AddService(service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE);

        bool success = false;
        int retry = 0;

        do {
            listening_.port = rand_r(&seed) % 20000 + 20000;  // [20000 ~ 39999)

            int ret = server_.Start(listening_, nullptr);
            if (!ret) {
                LOG(INFO) << "Listening on " << listening_;
                success = true;
                break;
            }
        } while (retry++ < kRetryTimes);

        ASSERT_TRUE(success);
    }

    void TearDown() override {
        server_.Stop(0);
        server_.Join();
    }

 protected:
    int port_;
    brpc::Server server_;
    MockSpaceManager spaceMgr_;
    std::unique_ptr<SpaceServiceImpl> service_;
    butil::EndPoint listening_;
};

TEST_F(SpaceServiceTest, FsNotFound) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listening_, nullptr));

    EXPECT_CALL(spaceMgr_, GetVolumeSpace(_))
        .WillRepeatedly(Return(nullptr));

#define NotFoundTest(type)                                            \
    do {                                                              \
        auto request = GenerateAnDefaultInitializedMessage(           \
            "curvefs.mds.space." STRINGIFY(type) "Request");          \
        ASSERT_TRUE(request);                                         \
        type##Response response;                                      \
        brpc::Controller cntl;                                        \
        SpaceService_Stub stub(&channel);                             \
        stub.type(&cntl, static_cast<type##Request*>(request.get()),  \
                  &response, nullptr);                                \
        ASSERT_FALSE(cntl.Failed());                                  \
        ASSERT_EQ(SpaceErrCode::SpaceErrNotFound, response.status()); \
    } while (0)

    NotFoundTest(AllocateBlockGroup);
    NotFoundTest(AcquireBlockGroup);
    NotFoundTest(ReleaseBlockGroup);
    NotFoundTest(StatSpace);
    NotFoundTest(UpdateUsage);

#undef NotFoundTest
}

TEST_F(SpaceServiceTest, AllcoateBlockGroupTest) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listening_, nullptr));

    for (auto err : {SpaceErrNoSpace, SpaceOk}) {
        MockVolumeSpace volumeSpace;
        EXPECT_CALL(spaceMgr_, GetVolumeSpace(_))
            .WillOnce(Return(&volumeSpace));

        EXPECT_CALL(volumeSpace, AllocateBlockGroups(_, _, _))
            .WillOnce(Return(err));

        auto request = GenerateAnDefaultInitializedMessage(
            "curvefs.mds.space.AllocateBlockGroupRequest");
        ASSERT_TRUE(request);

        AllocateBlockGroupResponse response;
        brpc::Controller cntl;
        SpaceService_Stub stub(&channel);

        stub.AllocateBlockGroup(
            &cntl, static_cast<AllocateBlockGroupRequest*>(request.get()),
            &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(err, response.status());
    }
}

TEST_F(SpaceServiceTest, AcquireBlockGroupTest) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listening_, nullptr));

    for (auto err : {SpaceErrNoSpace, SpaceOk}) {
        MockVolumeSpace volumeSpace;
        EXPECT_CALL(spaceMgr_, GetVolumeSpace(_))
            .WillOnce(Return(&volumeSpace));

        EXPECT_CALL(volumeSpace, AcquireBlockGroup(_, _, _))
            .WillOnce(
                Invoke([err](uint64_t blockGroupOffset,
                             const std::string& owner, BlockGroup* group) {
                    if (err != SpaceOk) {
                        return err;
                    }

                    group->set_offset(blockGroupOffset);
                    group->set_size(0);
                    group->set_available(0);
                    group->set_bitmaplocation(BitmapLocation::AtEnd);

                    return err;
                }));

        auto request = GenerateAnDefaultInitializedMessage(
            "curvefs.mds.space.AcquireBlockGroupRequest");
        ASSERT_TRUE(request);

        AcquireBlockGroupResponse response;
        brpc::Controller cntl;
        SpaceService_Stub stub(&channel);

        stub.AcquireBlockGroup(
            &cntl, static_cast<AcquireBlockGroupRequest*>(request.get()),
            &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(err, response.status());
    }
}

TEST_F(SpaceServiceTest, ReleaseBlockGroupTest) {
    brpc::Channel channel;
    ASSERT_EQ(0, channel.Init(listening_, nullptr));

    for (auto err : {SpaceErrNoSpace, SpaceOk}) {
        MockVolumeSpace volumeSpace;
        EXPECT_CALL(spaceMgr_, GetVolumeSpace(_))
            .WillOnce(Return(&volumeSpace));

        EXPECT_CALL(
            volumeSpace,
            ReleaseBlockGroups(Matcher<const std::vector<BlockGroup> &>(_)))
            .WillOnce(Return(err));

        auto request = GenerateAnDefaultInitializedMessage(
            "curvefs.mds.space.ReleaseBlockGroupRequest");
        ASSERT_TRUE(request);

        ReleaseBlockGroupResponse response;
        brpc::Controller cntl;
        SpaceService_Stub stub(&channel);

        stub.ReleaseBlockGroup(
            &cntl, static_cast<ReleaseBlockGroupRequest*>(request.get()),
            &response, nullptr);
        ASSERT_FALSE(cntl.Failed());
        ASSERT_EQ(err, response.status());
    }
}

}  // namespace space
}  // namespace mds
}  // namespace curvefs
