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

/*************************************************************************
 > File Name: config.h
 > Author:
 > Created Time: Wed Nov 21 11:33:46 2018
 ************************************************************************/

#ifndef SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
#define SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_


#include<string>
#include <vector>
#include "src/common/concurrent/dlock.h"

namespace curve {
namespace snapshotcloneserver {

using curve::common::DLockOpts;

// curve client options
struct CurveClientOptions {
    // config path
    std::string configPath;
    // mds root user
    std::string mdsRootUser;
    // mds root password
    std::string mdsRootPassword;
    //The total retry time for calling the client method

    uint64_t clientMethodRetryTimeSec;
    //Call client method retry interval

    uint64_t clientMethodRetryIntervalMs;
};

// snapshotcloneserver options
struct SnapshotCloneServerOptions {
    // snapshot&clone server address
    std::string  addr;
    //Total retry time for calling client asynchronous methods

    uint64_t clientAsyncMethodRetryTimeSec;
    //Call client asynchronous method retry interval

    uint64_t clientAsyncMethodRetryIntervalMs;
    //Number of snapshot worker threads

    int snapshotPoolThreadNum;
    //Scanning cycle of snapshot background thread scanning waiting queue and work queue (unit: ms)

    uint32_t snapshotTaskManagerScanIntervalMs;
    //Dump chunk shard size

    uint64_t chunkSplitSize;
    //CheckSnapShotStatus call interval

    uint32_t checkSnapshotStatusIntervalMs;
    //Maximum Snapshots

    uint32_t maxSnapshotLimit;
    // snapshotcore threadpool threadNum
    uint32_t snapshotCoreThreadNum;
    // mdsSessionTimeUs
    uint32_t mdsSessionTimeUs;
    //The number of asynchronous requests simultaneously processed by ReadChunkSnapshot

    uint32_t readChunkSnapshotConcurrency;

    //Number of thread pool threads used for Lazy clone metadata section

    int stage1PoolThreadNum;
    //Number of thread pool threads used for Lazy clone data section

    int stage2PoolThreadNum;
    //Number of thread pool threads used for requests for non Lazy clones and deletion of clones and other control surfaces

    int commonPoolThreadNum;
    //CloneTaskManager backend thread scan interval

    uint32_t cloneTaskManagerScanIntervalMs;
    //Clone chunk shard size

    uint64_t cloneChunkSplitSize;
    //Clone temporary directory

    std::string cloneTempDir;
    // mds root user
    std::string mdsRootUser;
    //Number of asynchronous requests made simultaneously by CreateCloneChunk

    uint32_t createCloneChunkConcurrency;
    //Number of asynchronous requests simultaneously made by RecoverChunk

    uint32_t recoverChunkConcurrency;
    //Reference Count Background Scan Each Record Interval

    uint32_t backEndReferenceRecordScanIntervalMs;
    //Reference Count Background Scan Every Round Interval

    uint32_t backEndReferenceFuncScanIntervalMs;
    // dlock options
    DLockOpts dlockOpts;
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
