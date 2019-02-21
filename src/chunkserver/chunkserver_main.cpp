/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 * 
 * History: 
 *          2018/08/30  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/chunkserver.h"

/*
 * The Entry point of Chunkserver
 */
int main(int argc, char* argv[]) {
    curve::chunkserver::ChunkServer chunkserver;
    butil::AtExitManager atExitManager;

    LOG(INFO) << "ChunkServer starting.";

    chunkserver.Init(argc, argv);
    chunkserver.Run();
    chunkserver.Fini();

    LOG(INFO) << "ChunkServer exited.";

    return 0;
}
