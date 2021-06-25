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
 * Created Date: 2020-03-16
 * Author: charisu
 */

#include <gflags/gflags.h>
#include "src/tools/curve_tool_define.h"

DEFINE_string(mdsAddr, "127.0.0.1:6666", "mds addr");
DEFINE_string(mdsDummyPort, "6667", "dummy port of mds, "
                                    "can specify one or several."
                                    "if specify several, the order "
                                    "should be the same as mds addr");
DEFINE_string(etcdAddr, "127.0.0.1:2379", "etcd addr");
DEFINE_uint64(rpcTimeout, 3000, "millisecond for rpc timeout");
DEFINE_uint64(rpcRetryTimes, 5, "rpc retry times");
DEFINE_uint64(rpcConcurrentNum, 10, "rpc concurrent number to chunkserver");
DEFINE_string(snapshotCloneAddr, "127.0.0.1:5555", "snapshot clone addr");
DEFINE_string(snapshotCloneDummyPort, "8081", "dummy port of snapshot clone, "
                                    "can specify one or several. "
                                    "if specify several, the order should "
                                    "be the same as snapshot clone addr");
DEFINE_uint64(chunkSize, 16777216, "chunk size");
