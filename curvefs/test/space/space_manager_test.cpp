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

#include "curvefs/src/space/space_manager.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "curvefs/src/space/utils.h"
#include "curvefs/test/space/common.h"
#include "curvefs/test/space/mock/mock_metaserver.h"

namespace curvefs {
namespace space {

using ::testing::_;
using ::testing::Invoke;

void ListDentryService(::google::protobuf::RpcController* controller,
                       const ::curvefs::metaserver::ListDentryRequest* request,
                       ::curvefs::metaserver::ListDentryResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_statuscode(metaserver::MetaStatusCode::OK);
}

const char kMetaServerAddress[] = "127.0.0.1:12345";

class SpaceManagerTest : public ::testing::Test {
 protected:
    void SetUp() override {
        SpaceManagerOption option;
        option.allocatorType = "bitmap";
        option.allocatorOption.bitmapAllocatorOption.length = 10 * kGiB;
        option.allocatorOption.bitmapAllocatorOption.sizePerBit = 4 * kMiB;
        option.reloaderOption.metaServerOption.addr = kMetaServerAddress;

        space_.reset(new SpaceManagerImpl(option));

        mockMetaServer_.reset(new MockMetaServerService());
        server_.reset(new brpc::Server());

        ASSERT_EQ(0, server_->AddService(mockMetaServer_.get(),
                                         brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server_->Start(kMetaServerAddress, nullptr));
    }

    void TearDown() override {
        server_->Stop(0);
        server_->Join();
    }

    void PrepareEnv() {
        mds::FsInfo blockFsInfo;
        blockFsInfo.set_fsid(1);
        blockFsInfo.set_fsname("block");
        blockFsInfo.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);
        blockFsInfo.set_capacity(10 * kGiB);

        EXPECT_CALL(*mockMetaServer_, ListDentry(_, _, _, _))
            .WillRepeatedly(Invoke(ListDentryService));

        EXPECT_EQ(SPACE_OK, space_->InitSpace(blockFsInfo));
    }

 protected:
    std::unique_ptr<SpaceManager> space_;
    std::unique_ptr<brpc::Server> server_;
    std::unique_ptr<MockMetaServerService> mockMetaServer_;
};

TEST_F(SpaceManagerTest, TestInitSpace) {
    mds::FsInfo fsInfo;
    fsInfo.set_fsid(1);
    fsInfo.set_fsname("block");
    fsInfo.set_fstype(::curvefs::common::FSType::TYPE_VOLUME);

    EXPECT_CALL(*mockMetaServer_, ListDentry(_, _, _, _))
        .WillRepeatedly(Invoke(ListDentryService));

    EXPECT_EQ(SPACE_OK, space_->InitSpace(fsInfo));

    // if fsid is already exists, init return exists
    EXPECT_EQ(SPACE_EXISTS, space_->InitSpace(fsInfo));
}

TEST_F(SpaceManagerTest, TestUnInitSpace) {
    EXPECT_EQ(SPACE_NOT_FOUND, space_->UnInitSpace(1));

    PrepareEnv();

    EXPECT_EQ(SPACE_OK, space_->UnInitSpace(1));

    EXPECT_EQ(SPACE_NOT_FOUND, space_->UnInitSpace(1));
}

TEST_F(SpaceManagerTest, TestAllocateSpace) {
    PrepareEnv();

    Extents exts;
    EXPECT_EQ(SPACE_NOT_FOUND, space_->AllocateSpace(100, 1 * kGiB, {}, &exts));
    EXPECT_TRUE(exts.empty());

    EXPECT_EQ(SPACE_OK, space_->AllocateSpace(1, 1 * kGiB, {}, &exts));
    EXPECT_EQ(1 * kGiB, TotalLength(exts));

    Extents exts2;
    EXPECT_EQ(SPACE_NO_SPACE, space_->AllocateSpace(1, 10 * kGiB, {}, &exts2));
    EXPECT_TRUE(exts2.empty());

    SpaceStat stat;
    EXPECT_EQ(SPACE_OK, space_->StatSpace(1, &stat));
    EXPECT_EQ(9 * kGiB, stat.available);
}

TEST_F(SpaceManagerTest, TestDeallocateSpace) {
    PrepareEnv();

    EXPECT_EQ(SPACE_NOT_FOUND, space_->DeallocateSpace(100, {}));

    Extents exts;
    EXPECT_EQ(SPACE_OK, space_->AllocateSpace(1, 5 * kGiB, {}, &exts));
    EXPECT_EQ(5 * kGiB, TotalLength(exts));

    EXPECT_EQ(SPACE_OK,
              space_->DeallocateSpace(1, ConvertToProtoExtents(exts)));

    SpaceStat stat;
    EXPECT_EQ(SPACE_OK, space_->StatSpace(1, &stat));
    EXPECT_EQ(stat.total, stat.available);
    EXPECT_EQ(10 * kGiB, stat.available);

    Extents dealloc(exts);
    EXPECT_EQ(SPACE_DEALLOC_ERROR,
              space_->DeallocateSpace(1, ConvertToProtoExtents(exts)));
}

TEST_F(SpaceManagerTest, TestStatSpace) {
    PrepareEnv();

    SpaceStat stat;
    EXPECT_EQ(SPACE_NOT_FOUND, space_->StatSpace(100, &stat));

    EXPECT_EQ(SPACE_OK, space_->StatSpace(1, &stat));

    EXPECT_EQ(stat.total, stat.available);
    EXPECT_EQ(stat.total, 10 * kGiB);
}

}  // namespace space
}  // namespace curvefs

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
