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
