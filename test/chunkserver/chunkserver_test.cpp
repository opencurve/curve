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
#include "src/chunkserver/chunkserver.h"

butil::AtExitManager atExitManager;

void* run_chunkserver_thread(void *arg) {
    curve::chunkserver::ChunkServer* chunkserver =
        reinterpret_cast<curve::chunkserver::ChunkServer *>(arg);
    chunkserver->Run();

    return NULL;
}

using curve::fs::LocalFsFactory;
using curve::fs::FileSystemType;

TEST(ChunkserverTest, LifeCycle) {
    int ret;
    char* argv[] = {
        "bazel-bin/test/chunkserver/chunkserver_test",
        "-bthread_concurrency=18",
        "-raft_max_segment_size=8388608",
        "-raft_sync=true",
        "-minloglevel=0",
        "-conf=conf/chunkserver.conf.example",
    };

    curve::chunkserver::ChunkServer chunkserver;
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
