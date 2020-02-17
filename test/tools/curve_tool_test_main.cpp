/*
 * Project: curve
 * File Created: 2020-02-20
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gflags/gflags.h>

DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_string(mdsDummyPort, "6667", "dummy port of mds");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string mdsMetaServerAddr = "127.0.0.1:9180";   // NOLINT

int main(int argc, char ** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);
    return RUN_ALL_TESTS();
}

