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
 * Project: curve
 * Created Date: 2020-02-25
 * Author: charisu
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
