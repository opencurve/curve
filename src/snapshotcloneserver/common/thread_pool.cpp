/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/common/thread_pool.h"

namespace curve {
namespace snapshotcloneserver {

int ThreadPool::Start() {
    return threadPool_.Start(threadNum_);
}

void ThreadPool::Stop() {
    threadPool_.Stop();
}

}  // namespace snapshotcloneserver
}  // namespace curve


