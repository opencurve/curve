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
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui1
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include "src/mds/nameserver2/idgenerator/inode_id_generator.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "test/mds/mock/mock_etcdclient.h"
#include "src/mds/common/mds_define.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
class TestInodeIdGenerator : public ::testing::Test {
 protected:
    TestInodeIdGenerator() {}
    ~TestInodeIdGenerator() {}

    void SetUp() override {
        client_ = std::make_shared<MockEtcdClient>();
        inodeIdGen_ = std::make_shared<InodeIdGeneratorImp>(client_);
    }

    void TearDown() override {
        client_ = nullptr;
        inodeIdGen_ = nullptr;
    }

 protected:
    std::shared_ptr<MockEtcdClient> client_;
    std::shared_ptr<InodeIdGeneratorImp> inodeIdGen_;
};

TEST_F(TestInodeIdGenerator, test_all) {
    uint64_t alloc1 = USERSTARTINODEID + INODEBUNDLEALLOCATED;
    uint64_t alloc2 = alloc1 + INODEBUNDLEALLOCATED;
    std::string strAlloc1 = NameSpaceStorageCodec::EncodeID(alloc1);
    EXPECT_CALL(*client_, Get(INODESTOREKEY, _))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist))
        .WillOnce(
            DoAll(SetArgPointee<1>(strAlloc1), Return(EtcdErrCode::EtcdOK)));
    EXPECT_CALL(*client_, CompareAndSwap(
        INODESTOREKEY, "", NameSpaceStorageCodec::EncodeID(alloc1)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*client_, CompareAndSwap(
        INODESTOREKEY, strAlloc1, NameSpaceStorageCodec::EncodeID(alloc2)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    uint64_t end = 2 * INODEBUNDLEALLOCATED;
    InodeID res;
    for (int i = USERSTARTINODEID + 1; i <= end; i++) {
        ASSERT_TRUE(inodeIdGen_->GenInodeID(&res));
        ASSERT_EQ(i, res);
    }
}
}  // namespace mds
}  // namespace curve
