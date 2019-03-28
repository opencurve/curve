/*
 * Project: curve
 * Created Date: Thur March 28th 2019
 * Author: lixiaocui1
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread> //NOLINT
#include <chrono> //NOLINT
#include "src/mds/nameserver2/inode_id_generator.h"
#include "src/mds/nameserver2/namespace_helper.h"

namespace curve {
namespace mds {
class TestInodeIdGenerator : public ::testing::Test {
 protected:
    TestInodeIdGenerator() {}
    ~TestInodeIdGenerator() {}

    void SetUp() override {
        client_ = std::make_shared<EtcdClientImp>();
        inodeIdGen_ = std::make_shared<InodeIdGeneratorImp>(client_);
        char endpoints[] = "127.0.0.1:2379";
        EtcdConf conf = {endpoints, strlen(endpoints), 1000};
        ASSERT_EQ(0, client_->Init(conf, 200, 3));

        ASSERT_FALSE(inodeIdGen_->Init());

        client_->SetTimeout(20000);
        system("etcd&");
    }

    void TearDown() override {
        inodeIdGen_ = nullptr;
        system("killall etcd");
        system("rm -fr default.etcd");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

 public:
    void GenInodeID1000Times() {
        for (int i = 0; i < 1000; i++) {
            InodeID res;
            bool ok = inodeIdGen_->GenInodeID(&res);
            while (!ok) {
                ok = inodeIdGen_->GenInodeID(&res);
            }
        }
    }

    void GenInodeID500Times() {
        for (int i = 0; i < 500; i++) {
            InodeID res;
            bool ok = inodeIdGen_->GenInodeID(&res);
            while (!ok) {
                ok = inodeIdGen_->GenInodeID(&res);
            }
        }
    }

 protected:
    std::shared_ptr<EtcdClientImp> client_;
    std::shared_ptr<InodeIdGeneratorImp> inodeIdGen_;
};

TEST_F(TestInodeIdGenerator, test_all) {
    // 1. test inital state
    inodeIdGen_->Init();
    uint64_t end = 2 * ::curve::mds::BUNDLEALLOCATED;
    InodeID res;
    for (int i = ::curve::mds::INODEINITIALIZE + 1; i <= end; i++) {
        ASSERT_TRUE(inodeIdGen_->GenInodeID(&res));
        ASSERT_EQ(i, res);
    }

    // 2. test restart state
    ASSERT_TRUE(inodeIdGen_->Init());
    for (int i = end + 1; i <= 3 * ::curve::mds::BUNDLEALLOCATED; i++) {
        ASSERT_TRUE(inodeIdGen_->GenInodeID(&res));
        ASSERT_EQ(i, res);
    }

    // 3. kill etcd, gen id ok
    client_->SetTimeout(500);
    system("killall etcd");
    ASSERT_FALSE(inodeIdGen_->GenInodeID(&res));
}

TEST_F(TestInodeIdGenerator, test_multiclient) {
    ASSERT_EQ(EtcdErrCode::OK, client_->Delete(::curve::mds::INODESTOREKEY));
    inodeIdGen_->Init();

    std::thread thread1 = std::thread(
        &TestInodeIdGenerator::GenInodeID1000Times, this);
    std::thread thread2 = std::thread(
        &TestInodeIdGenerator::GenInodeID1000Times, this);
    std::thread thread3 = std::thread(
        &TestInodeIdGenerator::GenInodeID500Times, this);
    thread1.join();
    thread2.join();
    thread3.join();

    InodeID res;
    ASSERT_TRUE(inodeIdGen_->GenInodeID(&res));
    ASSERT_EQ(2501, res);
}

TEST(InodeIDGen, EncodeInodeID) {
    uint64_t num = ULLONG_MAX;
    ASSERT_EQ(
        "18446744073709551615", NameSpaceStorageCodec::EncodeInodeID(num));
}

TEST(InodeIDGen, DecodeInodeID) {
    std::string str = "18446744073709551615";
    uint64_t out;
    ASSERT_TRUE(NameSpaceStorageCodec::DecodeInodeID(str, &out));
    ASSERT_EQ(ULLONG_MAX, out);

    str = "ffff";
    ASSERT_FALSE(NameSpaceStorageCodec::DecodeInodeID(str, &out));
}
}  // namespace mds
}  // namespace curve
