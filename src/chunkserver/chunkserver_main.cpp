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
    chunkserver.Run(argc, argv);
}
