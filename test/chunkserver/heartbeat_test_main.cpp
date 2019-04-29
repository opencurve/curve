/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
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
#include "test/chunkserver/heartbeat_test.h"

static char* param[3][6] = {
    {
        "bazel-bin/test/chunkserver/chunkserver_test",
        "-bthread_concurrency=18",
        "-raft_max_segment_size=8388608",
        "-raft_sync=true",
        "-minloglevel=2",
        "-conf=deploy/local/chunkserver/conf/chunkserver.conf.0",
    },
    {
        "bazel-bin/test/chunkserver/chunkserver_test",
        "-bthread_concurrency=18",
        "-raft_max_segment_size=8388608",
        "-raft_sync=true",
        "-minloglevel=2",
        "-conf=deploy/local/chunkserver/conf/chunkserver.conf.1",
    },
    {
        "bazel-bin/test/chunkserver/chunkserver_test",
        "-bthread_concurrency=18",
        "-raft_max_segment_size=8388608",
        "-raft_sync=true",
        "-minloglevel=2",
        "-conf=deploy/local/chunkserver/conf/chunkserver.conf.2",
    },
};

using ::curve::chunkserver::ChunkServer;

struct ChunkServerProcess {
    int id;
    ChunkServer* chunkserver;
    std::thread csThread;
};

butil::AtExitManager atExitManager;

static void* RunChunkServer(
    ChunkServer *chunkserver, int argc, char** argv) {
    int ret = chunkserver->Run(argc, argv);
    LOG_IF(ERROR, ret != 0) << "Failed to run chunkserver process";
    return NULL;
}

static ChunkServerProcess StartChunkServer(int i, int argc, char** argv) {
    ChunkServerProcess proc;
    proc.id = i;

    proc.chunkserver = new curve::chunkserver::ChunkServer();
    if (proc.chunkserver == nullptr) {
        LOG(ERROR) << "Failed to create chunkserver " << i;
        return proc;
    }

    proc.csThread = std::thread(RunChunkServer, proc.chunkserver, argc, argv);
    return proc;
}

static void StopChunkServer(ChunkServerProcess *proc) {
    proc->csThread.join();
    LOG(ERROR) << "stop chunkserver ok.";
}

int main(int argc, char* argv[]) {
    int ret;
    pid_t pids[3];

    testing::InitGoogleTest(&argc, argv);

    LOG_IF(ERROR, 0 != curve::chunkserver::RemovePeersData(true))
        << "Failed to remove peers' data";

    // chunkserver proccess 0
    pids[0] = fork();
    if (pids[0] < 0) {
        LOG(FATAL) << "Failed to create chunkserver process 0";
    } else if (pids[0] == 0) {
        ChunkServerProcess proc =
            StartChunkServer(0, sizeof(param[0])/sizeof(char *), param[0]);
        StopChunkServer(&proc);
        LOG(INFO) << "chunkserver-0 stopped";
        return 0;
    }

    // chunkserver proccess 1
    pids[1] = fork();
    if (pids[1] < 0) {
        LOG(FATAL) << "Failed to create chunkserver process 1";
    } else if (pids[1] == 0) {
        ChunkServerProcess proc =
            StartChunkServer(1, sizeof(param[1])/sizeof(char *), param[1]);
        StopChunkServer(&proc);
        LOG(INFO) << "chunkserver-1 stopped";
        return 0;
    }

    // chunkserver proccess 2
    pids[2] = fork();
    if (pids[2] < 0) {
        LOG(FATAL) << "Failed to create chunkserver process 2";
    } else if (pids[2] == 0) {
        ChunkServerProcess proc =
            StartChunkServer(2, sizeof(param[2])/sizeof(char *), param[2]);
        StopChunkServer(&proc);
        LOG(INFO) << "chunkserver-2 stopped";
        return 0;
    }

    // main test process
    {
        ret = RUN_ALL_TESTS();

        LOG(INFO) << "Test finished with code " << ret;
        for (int i = 0; i < 3; i++) {
            kill(pids[i], SIGINT);
            waitpid(pids[i], &ret, 0);
            LOG(INFO) << "Wait chunkserver " << i << " quiting: " << ret;
        }
        LOG(INFO) << "Heartbeat testing finished.";

        return ret;
    }
}
