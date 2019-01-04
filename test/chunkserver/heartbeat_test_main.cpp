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

static char* argv[3][6] = {
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

struct ChunkServerProcess {
    int                                 id;
    curve::chunkserver::ChunkServer*    chunkserver;
    bthread_t                           tid;
};

butil::AtExitManager atExitManager;

static void* RunChunkServer(void *arg) {
    curve::chunkserver::ChunkServer* chunkserver =
        reinterpret_cast<curve::chunkserver::ChunkServer *>(arg);
    chunkserver->Run();

    return NULL;
}

static ChunkServerProcess StartChunkServer(int i) {
    int ret;

    ChunkServerProcess proc = {0, nullptr, 0};

    proc.id = i;

    proc.chunkserver = new curve::chunkserver::ChunkServer();
    if (proc.chunkserver == nullptr) {
        LOG(ERROR) << "Failed to create chunkserver " << i;
        return proc;
    }

    ret = proc.chunkserver->Init(sizeof(argv[i])/sizeof(char *), argv[i]);
    if (ret != 0) {
        LOG(ERROR) << "Failed to initialize chunkserver " << i;
        return proc;
    }

    ret = pthread_create(&proc.tid,
                         NULL,
                         RunChunkServer,
                         proc.chunkserver);
    if (ret != 0) {
        LOG(ERROR) << "Failed to start chunkserver " << i;
        return proc;
    }

    return proc;
}

static int StopChunkServer(const ChunkServerProcess& proc) {
    int ret;

    // ret = proc.chunkserver->Stop();
    // if (ret != 0) {
    //     LOG(ERROR) << "Failed to stop chunkserver " << proc.id;
    //     return ret;
    // }

    pthread_join(proc.tid, NULL);

    ret = proc.chunkserver->Fini();
    if (ret != 0) {
        LOG(ERROR) << "Failed to cleanup chunkserver " << proc.id;
        return ret;
    }

    return ret;
}

static int StopAllChunkServers() {
    for (int i = 0; i < 3; i++) {
        butil::ip_t         ip;
        if (butil::str2ip("127.0.0.1", &ip) != 0) {
            LOG(ERROR) << "Fail to generate local ip address";
            return -1;
        }

        butil::EndPoint     csEp(ip, 8200 + i);
        brpc::Channel       channel;

        if (channel.Init(csEp, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to chunkserver " << csEp;
            return -1;
        }

        curve::chunkserver::ChunkServerService_Stub stub(&channel);

        brpc::Controller                            cntl;
        curve::chunkserver::ChunkServerStopRequest  req;
        curve::chunkserver::ChunkServerStopResponse resp;

        stub.Stop(&cntl, &req, &resp, nullptr);
        if (cntl.Failed() && resp.status() == 0) {
            LOG(ERROR) << "Fail to shutdown chunkserver " << csEp << ","
                       << " cntl error: " << cntl.ErrorText() << ","
                       << " operation status: " << resp.status() << ".";
        }
    }

    return 0;
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
        ChunkServerProcess proc = StartChunkServer(0);
        LOG_IF(ERROR, proc.tid == 0) << "Failed to start chunkserver process 0";

        ret = StopChunkServer(proc);
        LOG_IF(ERROR, ret != 0) << "Failed to stop chunkserver process 0";

        return ret;
    }

    // chunkserver proccess 1
    pids[1] = fork();
    if (pids[1] < 0) {
        LOG(FATAL) << "Failed to create chunkserver process 1";
    } else if (pids[1] == 0) {
        ChunkServerProcess proc = StartChunkServer(1);
        LOG_IF(ERROR, proc.tid == 0)
            << "Failed to create chunkserver process 1";

        ret = StopChunkServer(proc);
        LOG_IF(ERROR, ret != 0) << "Failed to stop chunkserver process 1";

        return ret;
    }

    // chunkserver proccess 2
    pids[2] = fork();
    if (pids[2] < 0) {
        LOG(FATAL) << "Failed to create chunkserver process 2";
    } else if (pids[2] == 0) {
        ChunkServerProcess proc = StartChunkServer(2);
        LOG_IF(ERROR, proc.tid == 0)
            << "Failed to create chunkserver process 2";

        ret = StopChunkServer(proc);
        LOG_IF(ERROR, ret != 0) << "Failed to stop chunkserver process 2";

        return ret;
    }

    // main test process
    {
        ret = RUN_ALL_TESTS();
        LOG(INFO) << "Test finished with code " << ret;

        ret = StopAllChunkServers();
        LOG(INFO) << "Stopping all chunkservers finished with code " << ret;

        for (int i = 0; i < 3; i ++) {
            waitpid(pids[i], &ret, 0);
            LOG(INFO) << "Wait chunkserver " << i << " quiting: " << ret;
        }
        LOG(INFO) << "Heartbeat testing finished.";

        return ret;
    }
}
