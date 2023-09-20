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

namespace curve {
namespace snapshotcloneserver {
// curve client options
struct CurveClientOptions {
    // config path
    std::string configPath;
    // mds root user
    std::string mdsRootUser;
    // mds root password
    std::string mdsRootPassword;
    // 调用client方法的重试总时间
    uint64_t clientMethodRetryTimeSec;
    // 调用client方法重试间隔时间
    uint64_t clientMethodRetryIntervalMs;
};

// snapshotcloneserver options
struct SnapshotCloneServerOptions {
    // snapshot&clone server address
    std::string  addr;
    // disable s3 snapshot & clone
    bool disableS3SnapshotClone;

    // 调用client异步方法重试总时间
    uint64_t clientAsyncMethodRetryTimeSec;
    // 调用client异步方法重试时间间隔
    uint64_t clientAsyncMethodRetryIntervalMs;
    // 快照工作线程数
    int snapshotPoolThreadNum;
    // 快照后台线程扫描等待队列和工作队列的扫描周期(单位：ms)
    uint32_t snapshotTaskManagerScanIntervalMs;
    // 转储chunk分片大小
    uint64_t chunkSplitSize;
    // CheckSnapShotStatus调用间隔
    uint32_t checkSnapshotStatusIntervalMs;
    // 最大快照数
    uint32_t maxSnapshotLimit;
    // snapshotcore threadpool threadNum
    uint32_t snapshotCoreThreadNum;
    // mdsSessionTimeUs
    uint32_t mdsSessionTimeUs;
    // ReadChunkSnapshot同时进行的异步请求数量
    uint32_t readChunkSnapshotConcurrency;
    // localsnapshot beckend checking interval
    uint32_t localSnapshotBackendCheckIntervalMs;

    // 用于Lazy克隆元数据部分的线程池线程数
    int stage1PoolThreadNum;
    // 用于Lazy克隆数据部分的线程池线程数
    int stage2PoolThreadNum;
    // 用于非Lazy克隆和删除克隆等其他管控面的请求的线程池线程数
    int commonPoolThreadNum;
    // CloneTaskManager 后台线程扫描间隔
    uint32_t cloneTaskManagerScanIntervalMs;
    // clone chunk分片大小
    uint64_t cloneChunkSplitSize;
    // 克隆临时目录
    std::string cloneTempDir;
    // mds root user
    std::string mdsRootUser;
    // CreateCloneChunk同时进行的异步请求数量
    uint32_t createCloneChunkConcurrency;
    // RecoverChunk同时进行的异步请求数量
    uint32_t recoverChunkConcurrency;
    // 引用计数后台扫描每条记录间隔
    uint32_t backEndReferenceRecordScanIntervalMs;
    // 引用计数后台扫描每轮间隔
    uint32_t backEndReferenceFuncScanIntervalMs;

    SnapshotCloneServerOptions()
      : addr(""),
        clientAsyncMethodRetryTimeSec(5),
        clientAsyncMethodRetryIntervalMs(1),
        snapshotPoolThreadNum(256),
        snapshotTaskManagerScanIntervalMs(1000),
        chunkSplitSize(1048576),
        checkSnapshotStatusIntervalMs(1000),
        maxSnapshotLimit(1024),
        snapshotCoreThreadNum(64),
        mdsSessionTimeUs(5000000),
        readChunkSnapshotConcurrency(16),
        localSnapshotBackendCheckIntervalMs(1000),
        stage1PoolThreadNum(256),
        stage2PoolThreadNum(256),
        commonPoolThreadNum(256),
        cloneTaskManagerScanIntervalMs(1000),
        cloneChunkSplitSize(65536),
        cloneTempDir("/clone"),
        mdsRootUser("root"),
        createCloneChunkConcurrency(64),
        recoverChunkConcurrency(64),
        backEndReferenceRecordScanIntervalMs(500),
        backEndReferenceFuncScanIntervalMs(3600000) {}
};

}  // namespace snapshotcloneserver
}  // namespace curve
#endif  // SRC_SNAPSHOTCLONESERVER_COMMON_CONFIG_H_
