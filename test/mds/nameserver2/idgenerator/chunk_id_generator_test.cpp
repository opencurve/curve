/*
 * Project: curve
 * Created Date: Thur Apr 24th 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include "src/mds/nameserver2/idgenerator/chunk_id_generator.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "test/mds/mock/mock_etcdclient.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
class TestChunkIdGenerator : public ::testing::Test {
 protected:
    TestChunkIdGenerator() {}
    ~TestChunkIdGenerator() {}

    void SetUp() override {
        client_ = std::make_shared<MockEtcdClient>();
        chunkIdGen_ = std::make_shared<ChunkIDGeneratorImp>(client_);
    }

    void TearDown() override {
        client_ = nullptr;
        chunkIdGen_ = nullptr;
    }

 protected:
    std::shared_ptr<MockEtcdClient> client_;
    std::shared_ptr<ChunkIDGeneratorImp> chunkIdGen_;
};

TEST_F(TestChunkIdGenerator, test_all) {
    uint64_t alloc1 = CHUNKINITIALIZE + CHUNKBUNDLEALLOCATED;
    uint64_t alloc2 = alloc1 + CHUNKBUNDLEALLOCATED;
    std::string strAlloc1 = NameSpaceStorageCodec::EncodeID(alloc1);
    EXPECT_CALL(*client_, Get(CHUNKSTOREKEY, _))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist))
        .WillOnce(
            DoAll(SetArgPointee<1>(strAlloc1), Return(EtcdErrCode::EtcdOK)));
    EXPECT_CALL(*client_, CompareAndSwap(
        CHUNKSTOREKEY, "", NameSpaceStorageCodec::EncodeID(alloc1)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));
    EXPECT_CALL(*client_, CompareAndSwap(
        CHUNKSTOREKEY, strAlloc1, NameSpaceStorageCodec::EncodeID(alloc2)))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    uint64_t end = 2 * CHUNKBUNDLEALLOCATED;
    InodeID res;
    for (int i = CHUNKINITIALIZE + 1; i <= end; i++) {
        ASSERT_TRUE(chunkIdGen_->GenChunkID(&res));
        ASSERT_EQ(i, res);
    }
}
}  // namespace mds
}  // namespace curve
