/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/11/23  Wenyu Zhou   Initial version
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#include "src/fs/fs_common.h"
#include "src/fs/local_filesystem.h"
#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserver.h"

#include "include/client/libcurve_qemu.h"
#include "test/client/fake/fakeMDS.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:6666";   // NOLINT

char* confPath = "conf/client.conf";

butil::AtExitManager atExitManager;

namespace curve {
namespace chunkserver {

void* run_chunkserver_thread(void *arg) {
    curve::chunkserver::ChunkServer* chunkserver =
        reinterpret_cast<curve::chunkserver::ChunkServer *>(arg);
    chunkserver->Run();

    return NULL;
}

using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

static int ExecCmd(const std::string& cmd) {
    LOG(INFO) << "executing command: " << cmd;

    return system(cmd.c_str());
}

class ChunkserverTest : public ::testing::Test {
 public:
    void SetUp() {
        std::string filename = "test.img";
        size_t      filesize =  10uL * 1024 * 1024 * 1024;

        mds_ = new FakeMDS(filename);

        mds_->Initialize();
        mds_->StartService();

        // Wait until MDS is ready or timed out
        int64_t t0 = butil::monotonic_time_ms();
        for (;;) {
            if (Init(confPath) != 0) {
                LOG(ERROR) << "Failed to initialize LibCurve config file";
            }

            fd_ = Open(filename.c_str(), filesize, true);
            if (fd_ >= 0) {
                LOG(INFO) << "Created file for test.";
                break;
            }

            int64_t t1 = butil::monotonic_time_ms();
            // Set timeout to 10 seconds
            if (t1 - t0 > 10 * 1000) {
                LOG(ERROR) << "Timed out retrying of creating file.";
                break;
            }

            LOG(INFO) << "Failed to create file, retrying again.";
            usleep(100 * 1000);
        }
        ASSERT_GE(fd_, 0);
    }

    void TearDown() {
        Close(fd_);
        mds_->UnInitialize();
        delete mds_;
    }

 private:
    FakeMDS*                    mds_;
    int                         fd_;
};

TEST_F(ChunkserverTest, LifeCycle) {
    int ret;
    char* argv[] = {
        "bazel-bin/test/chunkserver/chunkserver_test",
        "-bthread_concurrency=18",
        "-raft_max_segment_size=8388608",
        "-raft_sync=true",
        "-minloglevel=0",
        "-conf=conf/chunkserver.conf.example",
    };

    ChunkServer chunkserver;
    bthread_t tid;

    ret = chunkserver.Init(sizeof(argv)/sizeof(char *), argv);
    ASSERT_EQ(ret, 0);

    ret = pthread_create(&tid, NULL, run_chunkserver_thread, &chunkserver);
    ASSERT_EQ(ret, 0);

    ret = chunkserver.Stop();
    ASSERT_EQ(ret, 0);

    sleep(2);

    ret = chunkserver.Fini();
    ASSERT_EQ(ret, 0);

    ::system("rm -fr chunkfilepool.meta");
    ::system("rm -fr chunkfilepool");
}

TEST(ChunkserverCommonTest, GroupIdTest) {
    LogicPoolID poolId = 10000;
    CopysetID   copysetId = 8888;
    GroupNid    groupId0 = 42949672968888;  // (10000 << 32) | 8888;
    GroupId     groupIdStr0 = "42949672968888";
    std::string groupIdViewStr0 = "(10000, 8888)";

    GroupNid    groupId = ToGroupNid(poolId, copysetId);
    GroupId     groupIdStr = ToGroupId(poolId, copysetId);
    std::string groupIdViewStr = ToGroupIdStr(poolId, copysetId);

    ASSERT_EQ(groupId0, groupId);
    ASSERT_EQ(groupIdStr0, groupIdStr);
    ASSERT_EQ(poolId, GetPoolID(groupId));
    ASSERT_EQ(copysetId, GetCopysetID(groupId));
}

TEST(ServiceManagerTest, ExceptionTest) {
    ServiceOptions  opt;
    ServiceManager  srvMan;

    opt.ip = "1111.2.3";
    ASSERT_NE(0, srvMan.Init(opt));

    opt.ip = "127.0.0.1";
    opt.port = 65536;
    ASSERT_NE(0, srvMan.Init(opt));

    opt.ip = "8.8.8.8";
    opt.port = 22;
    ASSERT_EQ(0, srvMan.Init(opt));
    ASSERT_NE(0, srvMan.Run());
}

}  // namespace chunkserver
}  // namespace curve
