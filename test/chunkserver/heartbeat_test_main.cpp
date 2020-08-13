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
 * Project: Curve
 *
 * History:
 *          2018/12/23  Wenyu Zhou   Initial version
 */

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/chunkserver.h"
#include "src/chunkserver/heartbeat.h"
#include "test/chunkserver/heartbeat_test_common.h"
#include "test/integration/common/config_generator.h"

static char *param[3][12] = {
    {
        "heartbeat_test",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=8200",
        "-chunkServerStoreUri=local://./0/",
        "-chunkServerMetaUri=local://./0/chunkserver.dat",
        "-copySetUri=local://./0/copysets",
        "-raftSnapshotUri=curve://./0/copysets",
        "-recycleUri=local://./0/recycler",
        "-chunkFilePoolDir=./0/chunkfilepool/",
        "-chunkFilePoolMetaPath=./0/chunkfilepool.meta",
        "-conf=./8200/chunkserver.conf",
        "-graceful_quit_on_sigterm",
    },
    {
        "heartbeat_test",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=8201",
        "-chunkServerStoreUri=local://./1/",
        "-chunkServerMetaUri=local://./1/chunkserver.dat",
        "-copySetUri=local://./1/copysets",
        "-raftSnapshotUri=curve://./1/copysets",
        "-recycleUri=local://./1/recycler",
        "-chunkFilePoolDir=./1/chunkfilepool/",
        "-chunkFilePoolMetaPath=./1/chunkfilepool.meta",
        "-conf=./8201/chunkserver.conf",
        "-graceful_quit_on_sigterm",
    },
    {
        "heartbeat_test",
        "-chunkServerIp=127.0.0.1",
        "-chunkServerPort=8202",
        "-chunkServerStoreUri=local://./2/",
        "-chunkServerMetaUri=local://./2/chunkserver.dat",
        "-copySetUri=local://./2/copysets",
        "-raftSnapshotUri=curve://./2/copysets",
        "-recycleUri=local://./2/recycler",
        "-chunkFilePoolDir=./2/chunkfilepool/",
        "-chunkFilePoolMetaPath=./2/chunkfilepool.meta",
        "-conf=./8202/chunkserver.conf",
        "-graceful_quit_on_sigterm",
    },
};

using ::curve::chunkserver::ChunkServer;

butil::AtExitManager atExitManager;

static int RunChunkServer(int i, int argc, char **argv) {
    auto chunkserver = new curve::chunkserver::ChunkServer();
    if (chunkserver == nullptr) {
        LOG(ERROR) << "Failed to create chunkserver " << i;
        return -1;
    }
    int ret = chunkserver->Run(argc, argv);
    if (ret < 0) {
        LOG(ERROR) << "Failed to run chunkserver process " << ret;
        return -1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    int ret;
    pid_t pids[3];
    testing::InitGoogleTest(&argc, argv);

    // generate config file
    for (int i = 0; i < 3; ++i) {
        std::string mkdir1("mkdir -p ");
        mkdir1 += std::to_string(8200 + i);
        ::system(mkdir1.c_str());
        curve::CSTConfigGenerator cg;
        CHECK(cg.Init(std::to_string(8200 + i)))
            << "Init config generator fail";
        cg.SetKV("mds.listen.addr", "127.0.0.1:7777,127.0.0.1:9300");
        cg.SetKV("mds.heartbeat_interval", "1");
        cg.SetKV("copyset.snapshot_interval_s", "30");
        CHECK(cg.Generate()) << "Generate config file fail";
    }

    LOG_IF(ERROR, 0 != curve::chunkserver::RemovePeersData(true))
        << "Failed to remove peers' data";

    for (int i = 0; i < 3; ++i) {
        pids[i] = fork();
        if (pids[i] < 0) {
            LOG(FATAL) << "Failed to create chunkserver process 0";
        } else if (pids[i] == 0) {
            /*
             * RunChunkServer内部会调用LOG(), 有较低概率因不兼容fork()而卡死
             */
            return RunChunkServer(i, sizeof(param[i]) / sizeof(char *),
                                  param[i]);
        }
    }

    // main test process
    {
        LOG(INFO) << "Run all test...";
        pid_t pid = fork();
        if (pid < 0) {
            LOG(FATAL) << "Failed to create test proccess";
        } else if (pid == 0) {
            /*
             * RUN_ALL_TESTS内部可能会调用LOG(),
             * 有较低概率因不兼容fork()而卡死
             */
            ret = RUN_ALL_TESTS();
            return ret;
        }
        waitpid(pid, &ret, 0);
        LOG(INFO) << "Run all test end... Return code: " << ret;
        LOG_IF(FATAL, ret != 0) << "Run all test faild";

        LOG(INFO) << "Stop all chunkserver";
        for (int i = 0; i < 3; i++) {
            kill(pids[i], SIGTERM);
            waitpid(pids[i], &ret, 0);
        }
        LOG(INFO) << "Stop all chunkserver success";

        LOG(INFO) << "Restart chunkserver 1";
        pid = fork();
        if (pid < 0) {
            LOG(FATAL) << "Failed to restart chunkserver process 1";
        } else if (pid == 0) {
            /*
             * RunChunkServer内部会调用LOG(), 有较低概率因不兼容fork()而卡死
             */
            ret =
                RunChunkServer(1, sizeof(param[1]) / sizeof(char *), param[1]);
            return ret;
        }
        sleep(2);
        kill(pid, SIGTERM);
        waitpid(pid, &ret, 0);
        LOG(INFO) << "Stop restart chunkserver 1 ok. Return code: " << ret;

        LOG(INFO) << "Heartbeat testing finished.";
        return ret;
    }
}
