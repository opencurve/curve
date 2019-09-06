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
#include "src/mds/nameserver2/idgenerator/etcd_id_generator.h"
#include "src/mds/nameserver2/helper/namespace_helper.h"
#include "test/mds/mock/mock_etcdclient.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace mds {
class TestEtcdIdGenerator : public ::testing::Test {
 protected:
    TestEtcdIdGenerator() {}
    ~TestEtcdIdGenerator() {}

    void SetUp() override {
        initial_ = 0;
        bundle_ = 1000;
        storeKey_ = "04";

        client_ = std::make_shared<MockEtcdClient>();
        etcdIdGen_ = std::make_shared<EtcdIdGenerator>(
            client_, storeKey_, initial_, bundle_);
    }

    void TearDown() override {
        client_ = nullptr;
        etcdIdGen_ = nullptr;
    }

 public:
    void GenID1000Times() {
        for (int i = 0; i < 1000; i++) {
            InodeID res;
            bool ok = etcdIdGen_->GenID(&res);
            while (!ok) {
                ok = etcdIdGen_->GenID(&res);
            }
        }
    }

    void GenID500Times() {
        for (int i = 0; i < 500; i++) {
            InodeID res;
            bool ok = etcdIdGen_->GenID(&res);
            while (!ok) {
                ok = etcdIdGen_->GenID(&res);
            }
        }
    }

 protected:
    std::shared_ptr<MockEtcdClient> client_;
    std::shared_ptr<EtcdIdGenerator> etcdIdGen_;

    uint64_t initial_;
    uint64_t bundle_;
    std::string storeKey_;
};

TEST_F(TestEtcdIdGenerator, test_all) {
    // 1. test inital state
    uint64_t alloc1 = initial_ + bundle_;
    uint64_t alloc2 = alloc1 + bundle_;
    std::string strAlloc1 = NameSpaceStorageCodec::EncodeID(alloc1);
    EXPECT_CALL(*client_, Get(storeKey_, _))
        .WillOnce(Return(EtcdErrCode::KeyNotExist))
        .WillOnce(DoAll(SetArgPointee<1>(strAlloc1), Return(EtcdErrCode::OK)));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, "", NameSpaceStorageCodec::EncodeID(alloc1)))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, strAlloc1, NameSpaceStorageCodec::EncodeID(alloc2)))
        .WillOnce(Return(EtcdErrCode::OK));

    uint64_t end = 2 * bundle_;
    uint64_t res;
    for (int i = initial_ + 1; i <= end; i++) {
        ASSERT_TRUE(etcdIdGen_->GenID(&res));
        ASSERT_EQ(i, res);
    }

    // 2. test restart state
    uint64_t alloc3 = alloc2 + bundle_;
    std::string strAlloc2 = NameSpaceStorageCodec::EncodeID(alloc2);
    EXPECT_CALL(*client_, Get(storeKey_, _))
        .WillOnce(DoAll(SetArgPointee<1>(strAlloc2), Return(EtcdErrCode::OK)));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, strAlloc2, NameSpaceStorageCodec::EncodeID(alloc3)))
        .WillOnce(Return(EtcdErrCode::OK));
    for (int i = end + 1; i <= 3 * bundle_; i++) {
        ASSERT_TRUE(etcdIdGen_->GenID(&res));
        ASSERT_EQ(i, res);
    }

    // 3. kill etcd, gen id ok
    EXPECT_CALL(*client_, Get(storeKey_, _))
        .WillOnce(Return(EtcdErrCode::PermissionDenied));
    ASSERT_FALSE(etcdIdGen_->GenID(&res));
}

TEST_F(TestEtcdIdGenerator, test_multiclient) {
    uint64_t alloc1 = initial_ + bundle_;
    uint64_t alloc2 = alloc1 + bundle_;
    uint64_t alloc3 = alloc2 + bundle_;
    std::string strAlloc1 = NameSpaceStorageCodec::EncodeID(alloc1);
    std::string strAlloc2 = NameSpaceStorageCodec::EncodeID(alloc2);
    EXPECT_CALL(*client_, Get(storeKey_, _))
        .WillOnce(Return(EtcdErrCode::KeyNotExist))
        .WillOnce(DoAll(SetArgPointee<1>(strAlloc1), Return(EtcdErrCode::OK)))
        .WillOnce(DoAll(SetArgPointee<1>(strAlloc2), Return(EtcdErrCode::OK)));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, "", NameSpaceStorageCodec::EncodeID(alloc1)))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, strAlloc1, NameSpaceStorageCodec::EncodeID(alloc2)))
        .WillOnce(Return(EtcdErrCode::OK));
    EXPECT_CALL(*client_, CompareAndSwap(
        storeKey_, strAlloc2, NameSpaceStorageCodec::EncodeID(alloc3)))
        .WillOnce(Return(EtcdErrCode::OK));

    common::Thread thread1 = common::Thread(
        &TestEtcdIdGenerator::GenID1000Times, this);
    common::Thread thread2 = common::Thread(
        &TestEtcdIdGenerator::GenID1000Times, this);
    common::Thread thread3 = common::Thread(
        &TestEtcdIdGenerator::GenID500Times, this);
    thread1.join();
    thread2.join();
    thread3.join();

    uint64_t res;
    ASSERT_TRUE(etcdIdGen_->GenID(&res));
    ASSERT_EQ(2501, res);
}
}  // namespace mds
}  // namespace curve
