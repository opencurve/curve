/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/08/30  Wenyu Zhou   Initial version
 */
#include "src/chunkserver/chunkserver.h"

int main(int argc, char* argv[]) {
    butil::AtExitManager atExitManager;
    ::curve::chunkserver::ChunkServer chunkserver;

    LOG(INFO) << "ChunkServer starting.";
    // 这里不能用fork创建守护进程,bvar会存在一些问题
    // https://github.com/apache/incubator-brpc/issues/697
    // https://github.com/apache/incubator-brpc/issues/208
    chunkserver.Run(argc, argv);
}
