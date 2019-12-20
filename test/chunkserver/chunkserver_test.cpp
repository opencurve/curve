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

#include "test/client/fake/fakeMDS.h"

uint32_t segment_size = 1 * 1024 * 1024 * 1024ul;   // NOLINT
uint32_t chunk_size = 16 * 1024 * 1024;   // NOLINT
std::string metaserver_addr = "127.0.0.1:9301";   // NOLINT

char* confPath = "conf/client.conf";

butil::AtExitManager atExitManager;

DEFINE_string(chunkservertest, "testdefault", "default value of test");


namespace curve {
namespace chunkserver {
struct ChunkServerPackage {
    ChunkServer *chunkserver;
    int argc;
    char **argv;
};

void* run_chunkserver_thread(void *arg) {
    ChunkServerPackage *package = reinterpret_cast<ChunkServerPackage *>(arg);
    package->chunkserver->Run(package->argc, package->argv);

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
        size_t filesize =  10uL * 1024 * 1024 * 1024;

        mds_ = new FakeMDS(filename);

        mds_->Initialize();
        mds_->StartService();
    }

    void TearDown() {
        mds_->UnInitialize();
        delete mds_;
    }

 private:
    FakeMDS* mds_;
    int fd_;
};

TEST(ChunkserverCommonTest, GroupIdTest) {
    LogicPoolID poolId = 10000;
    CopysetID copysetId = 8888;
    GroupNid groupId0 = 42949672968888;  // (10000 << 32) | 8888;
    GroupId groupIdStr0 = "42949672968888";
    std::string groupIdViewStr0 = "(10000, 8888, 42949672968888)";

    GroupNid groupId = ToGroupNid(poolId, copysetId);
    GroupId groupIdStr = ToGroupId(poolId, copysetId);
    std::string groupIdViewStr = ToGroupIdStr(poolId, copysetId);

    ASSERT_EQ(groupId0, groupId);
    ASSERT_EQ(groupIdStr0, groupIdStr);
    ASSERT_EQ(groupIdViewStr0, groupIdViewStr);
    ASSERT_EQ(poolId, GetPoolID(groupId));
    ASSERT_EQ(copysetId, GetCopysetID(groupId));
}

TEST(ChunkServerGflagTest, test_load_gflag) {
    int argc = 1;
    char *argvv[] = {""};
    char **argv = argvv;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::CommandLineFlagInfo info;
    ASSERT_TRUE(GetCommandLineFlagInfo("chunkservertest", &info));
    ASSERT_TRUE(info.is_default);
    ASSERT_EQ("testdefault", FLAGS_chunkservertest);
    ASSERT_FALSE(
        GetCommandLineFlagInfo("chunkservertest", &info) && !info.is_default);

    char *argvj[] = {"", "-chunkservertest=test1"};
    argv = argvj;
    argc = 2;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ASSERT_TRUE(GetCommandLineFlagInfo("chunkservertest", &info));
    ASSERT_FALSE(info.is_default);
    ASSERT_EQ("test1", FLAGS_chunkservertest);
    ASSERT_TRUE(
        GetCommandLineFlagInfo("chunkservertest", &info) && !info.is_default);
}
}  // namespace chunkserver
}  // namespace curve
