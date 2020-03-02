/*
 * Project: curve
 * Created Date: 2020-02-25
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include <gflags/gflags.h>
#include <iostream>
#include "src/tools/chunkserver_tool_factory.h"

DEFINE_string(fileName, "", "file name");
DEFINE_bool(example, false, "print example or not");

const char* kHelpStr = "Usage: chunkserver_tool [Command] [OPTIONS...]\n"
        "COMMANDS:\n"
        "chunk-meta : print chunk meta page info\n"
        "snapshot-meta : print snapshot meta page info\n"
        "raft-log-meta : print raft log header\n";

using curve::tool::ChunkServerToolFactory;

int main(int argc, char **argv) {
    gflags::SetUsageMessage(kHelpStr);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (argc < 2) {
        std::cout << kHelpStr << std::endl;
        return -1;
    }

    std::string command = argv[1];
    auto curveTool = ChunkServerToolFactory::GenerateChunkServerTool(command);
    if (!curveTool) {
        std::cout << kHelpStr << std::endl;
        return -1;
    }
    if (FLAGS_example) {
        curveTool->PrintHelp(command);
        return 0;
    }
    return curveTool->RunCommand(command);
}
