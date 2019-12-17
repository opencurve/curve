/*
 * Project: curve
 * File Created: Friday, 28th June 2019 2:29:14 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <fiu-control.h>

#include "test/client/fake/fakeMDS.h"
#include "src/client/client_common.h"
#include "src/tools/consistency_check.h"

std::string metaserver_addr = "127.0.0.1:9160";                                  //  NOLINT
uint32_t chunk_size = 4*1024*1024;
uint32_t segment_size = 1*1024*1024*1024;

DECLARE_string(client_config_path);
DECLARE_string(filename);
DECLARE_uint64(filesize);
DECLARE_uint64(chunksize);
DECLARE_uint64(segmentsize);
DECLARE_string(username);
DECLARE_bool(check_hash);
DECLARE_string(chunkserver_list);

class CheckFileConsistencyTest : public ::testing::Test {
 public:
    CheckFileConsistencyTest() : fakemds("test") {}
    void SetUp() {
        FLAGS_chunksize = 4*1024*1024;
        FLAGS_client_config_path = "./test/tools/config/data_check.conf";
        FLAGS_chunkserver_list =
            "127.0.0.1:9161:0,127.0.0.1:9162:0,127.0.0.1:9163:0";

        fakemds.Initialize();
        fakemds.StartService();

        curve::client::EndPoint ep;
        butil::str2endpoint("127.0.0.1", 9161, &ep);
        PeerId pd(ep);
        fakemds.StartCliService(pd);
        fakemds.CreateCopysetNode(true);
    }

    void TearDown() {
        fakemds.UnInitialize();
    }

 public:
    FakeMDS fakemds;
};


TEST_F(CheckFileConsistencyTest, Consistency) {
    FLAGS_check_hash = true;
    CheckFileConsistency cfc;
    cfc.PrintHelp();

    bool ret = cfc.Init();
    ASSERT_EQ(true, ret);

    ret = cfc.FetchFileCopyset();
    ASSERT_EQ(true, ret);

    // 获取copyset service指针用于设置hash值
    std::vector<FakeCreateCopysetService*> servec =
         fakemds.GetCreateCopysetService();
    for (auto iter : servec) {
        iter->SetHash(1111);
    }

    int rc = cfc.ReplicasConsistency() ? 0 : -1;

    ASSERT_EQ(rc, 0);

    cfc.UnInit();
}

TEST_F(CheckFileConsistencyTest, NotConsistency) {
    FLAGS_check_hash = true;
    CheckFileConsistency cfc;

    bool ret = cfc.Init();
    ASSERT_EQ(true, ret);

    ret = cfc.FetchFileCopyset();
    ASSERT_EQ(true, ret);

    // 获取copyset service指针用于设置hash值
    int peercount = 0;
    std::vector<FakeCreateCopysetService*> servec =
         fakemds.GetCreateCopysetService();
    for (auto iter : servec) {
        // peer之间设置不同的hash值，这样检查consistency就不满足
        if (peercount++ % 2) {
            iter->SetHash(1111);
        } else {
            iter->SetHash(2222);
        }
    }

    int rc = cfc.ReplicasConsistency() ? 0 : -1;

    ASSERT_EQ(rc, -1);

    cfc.UnInit();
}

TEST_F(CheckFileConsistencyTest, ApplyindexNotMatch) {
    FLAGS_check_hash = false;
    CheckFileConsistency cfc;

    bool ret = cfc.Init();
    ASSERT_EQ(true, ret);

    ret = cfc.FetchFileCopyset();
    ASSERT_EQ(true, ret);

    // 获取copyset service指针用于设置hash值
    int peercount = 0;
    std::vector<FakeCreateCopysetService*> servec =
         fakemds.GetCreateCopysetService();
    for (auto iter : servec) {
        // peer之间设置不同的hash值，这样检查consistency就不满足
        if (peercount++ % 2) {
            iter->SetApplyindex(1111);
        } else {
            iter->SetApplyindex(2222);
        }
    }

    int rc = cfc.ReplicasConsistency() ? 0 : -1;

    ASSERT_EQ(rc, -1);

    cfc.UnInit();
}


TEST_F(CheckFileConsistencyTest, ApplyindexMatch) {
    FLAGS_check_hash = false;
    CheckFileConsistency cfc;

    bool ret = cfc.Init();
    ASSERT_EQ(true, ret);

    ret = cfc.FetchFileCopyset();
    ASSERT_EQ(true, ret);

    // 获取copyset service指针用于设置hash值
    int peercount = 0;
    std::vector<FakeCreateCopysetService*> servec =
         fakemds.GetCreateCopysetService();
    for (auto iter : servec) {
        iter->SetApplyindex(1111);
    }

    int rc = cfc.ReplicasConsistency() ? 0 : -1;

    ASSERT_EQ(rc, 0);

    cfc.UnInit();
}

TEST_F(CheckFileConsistencyTest, GetCopysetStatusFail) {
    CheckFileConsistency cfc;

    bool ret = cfc.Init();
    ASSERT_EQ(true, ret);

    ret = cfc.FetchFileCopyset();
    ASSERT_EQ(true, ret);

    // 获取copyset service指针用于设置response中的status
    int peercount = 0;
    std::vector<FakeCreateCopysetService*> servec =
         fakemds.GetCreateCopysetService();
    servec[0]->SetStatus(
        COPYSET_OP_STATUS::COPYSET_OP_STATUS_COPYSET_NOTEXIST);

    FLAGS_check_hash = false;
    int rc = cfc.ReplicasConsistency() ? 0 : -1;
    ASSERT_EQ(rc, -1);

    FLAGS_check_hash = true;
    rc = cfc.ReplicasConsistency() ? 0 : -1;
    ASSERT_EQ(rc, -1);

    cfc.UnInit();
}
