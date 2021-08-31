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
 * @Date: 2021-08-31
 * @Author: chengyi
 */

#include "curvefs/src/mds/chunkid_allocator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "curvefs/test/mds/mock_etcdclient.h"

namespace curvefs {
namespace mds {

using curve::kvstorage::KVStorageClient;
using std::make_shared;
using std::set;
using std::shared_ptr;
using std::string;
using std::vector;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::SetArgReferee;
using ::testing::StrEq;

uint64_t CHUNKID = 0;  // use to record chunkId

// replace MockEtcdClientImpl::Get
int GetRep(const std::string& key, std::string* out) {
    *out = ChunkIdAllocatorImpl::EncodeID(CHUNKID);
    LOG(INFO) << "check chunk id is: " << *out;
    return 0;
}

// replaceMockEtcdClientImpl::CompareAndSwap

int CompareAndSwapRep(const std::string& key, const std::string& preV,
                      const std::string& target) {
    uint64_t preValue = 0;
    ChunkIdAllocatorImpl::DecodeID(preV, &preValue);
    uint64_t targetValue = 0;
    ChunkIdAllocatorImpl::DecodeID(target, &targetValue);
    if (preValue != CHUNKID) {
        LOG(ERROR) << "Error："
                   << "prev:" << preValue << ", now: " << CHUNKID
                   << ", target: " << target;
        return EtcdErrCode::EtcdOutOfRange;
    }

    CHUNKID = targetValue;
    return EtcdErrCode::EtcdOK;
}

class ChunkIdAllocatorTest : public testing::Test {
 protected:
    void SetUp() override;
    void TearDown() override;

    std::shared_ptr<ChunkIdAllocator> chunkIdAllocator_;
    std::shared_ptr<MockEtcdClientImpl> mockEtcdClient_;
};

void ChunkIdAllocatorTest::SetUp() {
    mockEtcdClient_ = make_shared<MockEtcdClientImpl>();
    chunkIdAllocator_ = make_shared<ChunkIdAllocatorImpl>(mockEtcdClient_);
}

void ChunkIdAllocatorTest::TearDown() {}

TEST_F(ChunkIdAllocatorTest, test_get_chunkIds_1001) {
    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillRepeatedly(Invoke(GetRep));
    EXPECT_CALL(*mockEtcdClient_.get(), CompareAndSwap(_, _, _))
        .WillRepeatedly(Invoke(CompareAndSwapRep));
    const int times = 1001;
    set<uint64_t> chunkids;  // record get chunkids
    for (int i = 0; i < times; ++i) {
        uint64_t tmp = 0;
        chunkIdAllocator_->GenChunkId(&tmp);
        chunkids.insert(tmp);
        LOG(INFO) << "allocate id:" << tmp;
    }
    LOG(INFO) << "chunkids' szie: " << chunkids.size();

    ASSERT_EQ(times, chunkids.size());
}

TEST_F(ChunkIdAllocatorTest, test_reclient_get_chunkIds_1001) {
    // after test_get_chunkIds_1001，client close and restart,
    // try to get chunkids
    mockEtcdClient_ = make_shared<MockEtcdClientImpl>();
    chunkIdAllocator_ = make_shared<ChunkIdAllocatorImpl>(mockEtcdClient_);

    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillRepeatedly(Invoke(GetRep));
    EXPECT_CALL(*mockEtcdClient_.get(), CompareAndSwap(_, _, _))
        .WillRepeatedly(Invoke(CompareAndSwapRep));
    const int times = 1001;
    set<uint64_t> chunkids;  // record get chunkids
    for (int i = 0; i < times; ++i) {
        uint64_t tmp = 0;
        chunkIdAllocator_->GenChunkId(&tmp);
        chunkids.insert(tmp);
        LOG(INFO) << "allocate id:" << tmp;
    }
    LOG(INFO) << "chunkids' szie: " << chunkids.size();

    ASSERT_EQ(times, chunkids.size());
}

TEST_F(ChunkIdAllocatorTest, storeKeyExist_etcdNotOK) {
    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));
    uint64_t chunkId;
    int ret = chunkIdAllocator_->GenChunkId(&chunkId);
    LOG(INFO) << "ret: " << ret;
    ASSERT_LT(ret, 0);
}

TEST_F(ChunkIdAllocatorTest, storeKeyNotExist) {
    string out = "abs";
    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));
    EXPECT_CALL(*mockEtcdClient_.get(), CompareAndSwap(_, _, _))
        .WillOnce(Return(0));
    uint64_t chunkId;
    int ret = chunkIdAllocator_->GenChunkId(&chunkId);
    LOG(INFO) << "ret: " << ret;
    ASSERT_EQ(ret, 0);
}

TEST_F(ChunkIdAllocatorTest, DecodeID_ERROR) {
    string out = "abs";  // set out can't be decoded
    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(out), Return(EtcdErrCode::EtcdOK)));
    uint64_t chunkId;
    int ret = chunkIdAllocator_->GenChunkId(&chunkId);
    LOG(INFO) << "ret: " << ret;
    ASSERT_LT(ret, 0);
}

TEST_F(ChunkIdAllocatorTest, CAS_ERROR) {
    string out = "1000";  // set out can't be decoded
    EXPECT_CALL(*mockEtcdClient_.get(), Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(out), Return(EtcdErrCode::EtcdOK)));
    EXPECT_CALL(*mockEtcdClient_.get(), CompareAndSwap(_, _, _))
        .WillOnce(Return(-3));
    uint64_t chunkId;
    int ret = chunkIdAllocator_->GenChunkId(&chunkId);
    LOG(INFO) << "ret: " << ret;
    ASSERT_LT(ret, 0);
}

}  // namespace mds
}  // namespace curvefs

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);

    return RUN_ALL_TESTS();
}
