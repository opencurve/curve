/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/snapshot_thread_pool.h"

namespace curve {
namespace snapshotserver {

const int kThreadPoolTaskQueueCapacity = 100;

void SnapshotThreadPool::Start() {
    threadPool_.Start(threadNum_,
        kThreadPoolTaskQueueCapacity);
}

void SnapshotThreadPool::Stop() {
    threadPool_.Stop();
}

}  // namespace snapshotserver
}  // namespace curve

