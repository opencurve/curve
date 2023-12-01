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
 * File Created: Saturday, 29th December 2018 3:50:45 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_CONFIG_INFO_H_
#define SRC_CLIENT_CONFIG_INFO_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace curve {
namespace client {

/**
 * Basic configuration information of log
 * @logLevel: It is the log printing level
 * @logPath: Log printing location
 */
struct LogInfo {
    int logLevel = 2;
    std::string logPath;
};

/**
 * in flight IO control information
 * @fileMaxInFlightRPCNum: is the maximum allowed number of inflight IOs in a
 * file
 */
struct InFlightIOCntlInfo {
    uint64_t fileMaxInFlightRPCNum = 2048;
};

struct MetaServerOption {
    uint64_t mdsMaxRetryMS = 8000;
    struct RpcRetryOption {
        // rpc max timeout
        uint64_t maxRPCTimeoutMS = 2000;
        // rpc normal timeout
        uint64_t rpcTimeoutMs = 500;
        // rpc retry interval
        uint32_t rpcRetryIntervalUS = 50000;
        // retry maxFailedTimesBeforeChangeAddr at a server
        uint32_t maxFailedTimesBeforeChangeAddr = 5;

        /**
         * When the failed times except RPC error
         * greater than mdsNormalRetryTimesBeforeTriggerWait,
         * it will trigger wait strategy, and sleep long time before retry
         */
        uint64_t maxRetryMsInIOPath = 86400000;  // 1 day

        // if the overall timeout passed by the user is 0, that means retry
        // until success. First try normalRetryTimesBeforeTriggerWait times,
        // then repeatedly sleep waitSleepMs and try once
        uint64_t normalRetryTimesBeforeTriggerWait = 3;  // 3 times
        uint64_t waitSleepMs = 10000;                    // 10 seconds

        std::vector<std::string> addrs;
    } rpcRetryOpt;
};

/**
 * Basic configuration of lease
 * @mdsRefreshTimesPerLease: The number of renewals within a lease, and the
 * heartbeat between client and mds is maintained through the lease If both
 * parties agree that the lease term is 10 seconds, then the client will be
 * within these 10 seconds send mdsRefreshTimesPerLease heartbeats, if
 * consecutive failures occur, So the client believes that there is an
 * abnormality in the current mds, which will block subsequent IO until
 *                           successfully renewed the contract.
 */
struct LeaseOption {
    uint32_t mdsRefreshTimesPerLease = 5;
};

/**
 * RPC timeout, parameter to determine if it is unstable
 * @maxStableChunkServerTimeoutTimes:
 *     The threshold for a chunkserver to continuously timeout requests, after
 * which the health status will be checked, If not healthy, mark as unstable
 * @checkHealthTimeoutMS:
 *      Check if chunkserver is healthy HTTP request timeout
 * @serverUnstableThreashold:
 *      More than serverUnstableThreashold chunkservers on a server are marked
 * as unstable, All chunkservers on the entire server are marked as unstable
 */
struct ChunkServerUnstableOption {
    uint32_t maxStableChunkServerTimeoutTimes = 64;
    uint32_t checkHealthTimeoutMS = 100;
    uint32_t serverUnstableThreshold = 3;
};

/**
 * Handling of failed chunk request:
 * @chunkserverOPMaxRetry:
 * Maximum retry count allowed for an RPC sent to the underlying chunk server.
 *                        If exceeded, it will be propagated to the user.
 * @chunkserverOPRetryIntervalUS:
 * Time interval between retries. After a failed RPC, the client will sleep for
 * a period determined by the RPC response status before retrying. Currently,
 *                        except for TIMEOUT and REDIRECTED, all other response
 * values require sleeping for some time before retrying.
 * @chunkserverRPCTimeoutMS: Timeout configured for each RPC sent when creating
 * its RPC controller.
 * @chunkserverMaxRPCTimeoutMS:
 * When the underlying chunkserver returns TIMEOUT, it means the current request
 * cannot be processed promptly, possibly due to a large number of queued tasks.
 * In such cases, sending requests with the same RPC timeout again may still
 * result in timeouts. To avoid this, exponential backoff logic is applied to
 * increase the timeout gradually, but it cannot exceed this maximum value.
 * @chunkserverMaxRetrySleepIntervalUS:
 * When the underlying chunk server returns OVERLOAD, indicating excessive
 * pressure, the sleep interval is exponentially extended to ensure that client
 *                        requests do not overwhelm the underlying chunk server.
 * However, the maximum sleep time cannot exceed this value.
 * @chunkserverMaxStableTimeoutTimes:
 * Threshold for consecutive timeouts on an RPC from a chunk server. If
 * exceeded, the chunk server is marked as unstable. This is because if a server
 *                        where a chunk server resides crashes, requests sent to
 * that chunk server will all time out. If the same chunk server's RPCs
 * consecutively timeout beyond this threshold, the client assumes that the
 * server where it resides may have crashed and marks all leader copysets on
 * that server as unstable,  prompting a leader retrieval before sending any
 * RPCs.
 * @chunkserverMinRetryTimesForceTimeoutBackoff:
 * When a request exceeds the retry count threshold, it continues to retry with
 * exponential backoff for its timeout duration.
 */
struct FailureRequestOption {
    uint32_t chunkserverOPMaxRetry = 3;
    uint64_t chunkserverOPRetryIntervalUS = 200;
    uint64_t chunkserverRPCTimeoutMS = 1000;
    uint64_t chunkserverMaxRPCTimeoutMS = 64000;
    uint64_t chunkserverMaxRetrySleepIntervalUS = 64ull * 1000 * 1000;
    uint64_t chunkserverMinRetryTimesForceTimeoutBackoff = 5;

    // When a request remains outstanding beyond this threshold, it is marked as
    // a slow request.
    // Default: 45s
    uint32_t chunkserverSlowRequestThresholdMS = 45 * 1000;
};

/**
 * Configuration for sending rpc to chunkserver
 * @inflightOpt: Configuration of inflight request control when a file sends a
 * request to chunkserver
 * @failRequestOpt: After rpc sending fails, relevant configuration for rpc
 * retry needs to be carried out
 */
struct IOSenderOption {
    InFlightIOCntlInfo inflightOpt;
    FailureRequestOption failRequestOpt;
};

/**
 Basic Configuration Information for the Scheduler Module
 * The scheduler module is used for distributing user requests. Each file has
 its own scheduler thread pool, and each thread in the pool is configured with
 its own queue.
 * @scheduleQueueCapacity: The queue depth configured by the schedule module
 * @scheduleThreadpoolSize: schedule module thread pool size
 */
struct RequestScheduleOption {
    uint32_t scheduleQueueCapacity = 1024;
    uint32_t scheduleThreadpoolSize = 2;
    IOSenderOption ioSenderOpt;
};

/**
 * MetaCache Module Configuration
 * @metacacheGetLeaderRetry:
 * Number of retries to get the leader. Before an RPC is sent to the
 * chunkserver, it needs to first obtain the leader for the current copyset. If
 * this information is not available in the metacache, a getleader request is
 * sent to a copyset's peers. If getleader fails, it needs to be retried, with a
 * maximum retry count defined by this value.
 * @metacacheRPCRetryIntervalUS:
 * As mentioned above, if a getleader request fails, it will be retried, but not
 * immediately. Instead, there will be a delay before the retry. This value
 * represents the length of that delay.
 * @metacacheGetLeaderRPCTimeOutMS: The maximum timeout duration for the RPC
 * controller when sending a 'getleader' RPC request
 * @metacacheGetLeaderBackupRequestMS:
 * Since a copyset has three or more peers, getleader requests are
 *                            sent to these peers in a backuprequest manner.
 *                            Internally, in brpc, these requests are sent
 * serially. If the first request takes too long to return, the next request is
 * sent to the next peer without waiting for the previous one to return or time
 * out. The time at which backup requests are triggered is determined by this
 * value.
 * @metacacheGetLeaderBackupRequestLbName: Strategy for selecting the underlying
 * service nodes for getleader backup RPCs.
 */
struct MetaCacheOption {
    uint32_t metacacheGetLeaderRetry = 3;
    uint32_t metacacheRPCRetryIntervalUS = 500;
    uint32_t metacacheGetLeaderRPCTimeOutMS = 1000;
    uint32_t metacacheGetLeaderBackupRequestMS = 100;
    uint32_t discardGranularity = 4096;
    std::string metacacheGetLeaderBackupRequestLbName = "rr";
    ChunkServerUnstableOption chunkserverUnstableOption;
};

/**
 * IO Split Module Configuration
 * @fileIOSplitMaxSizeKB:
 * The size of user-issued IOs is not restricted by the client. However, the
 * client will split the user's IOs, and the data size carried by requests sent
 * to the same chunkserver cannot exceed this value.
 */
struct IOSplitOption {
    uint64_t fileIOSplitMaxSizeKB = 64;
};

/**
 * Configuration information for thread-isolated task queues.
 * Thread isolation is primarily used to push asynchronous interface calls
 * directly into the thread pool instead of blocking them until they are placed
 * in the dispatch queue thread pool.
 * @isolationTaskQueueCapacity: The queue depth of the isolation thread pool.
 * @isolationTaskThreadPoolSize: The capacity of the isolation thread pool.
 */
struct TaskThreadOption {
    uint64_t isolationTaskQueueCapacity = 500000;
    uint32_t isolationTaskThreadPoolSize = 1;
};

// for discard
struct DiscardOption {
    bool enable = false;
    uint32_t taskDelayMs = 1000 * 60;  // 1 min
};

/**
 * timed close fd thread in SourceReader config
 * @fdTimeout: sourcereader fd timeout
 * @fdCloseTimeInterval: close sourcereader fd time interval
 */
struct CloseFdThreadOption {
    uint32_t fdTimeout = 300;
    uint32_t fdCloseTimeInterval = 600;
};

struct ThrottleOption {
    bool enable = false;
};

/**
 * IOOption stores all the configuration information required for the current IO
 * operation
 */
struct IOOption {
    IOSplitOption ioSplitOpt;
    IOSenderOption ioSenderOpt;
    MetaCacheOption metaCacheOpt;
    TaskThreadOption taskThreadOpt;
    RequestScheduleOption reqSchdulerOpt;
    CloseFdThreadOption closeFdThreadOption;
    ThrottleOption throttleOption;
    DiscardOption discardOption;
};

/**
 * Common client-side configuration options:
 * @mdsRegisterToMDS: Whether to register client information with the MDS. Since
 * the client needs to export metric information through a dummy server to
 * support Prometheus's automatic service discovery mechanism, it sends its
 * listening IP and port information to the MDS.
 * @turnOffHealthCheck: Whether to disable health checks.
 */
struct CommonConfigOpt {
    bool mdsRegisterToMDS = false;
    bool turnOffHealthCheck = false;

    // Minimal open file limit
    // open file limit will affect how may sockets we can create,
    // the number of sockets is related to the number of chunkserver and mds in
    // the cluster and during some exception handling processes, client will
    // create additional sockets the SAFE value is 2 * (#chunkserver + #mds)
    // Default: 65535
    uint32_t minimalOpenFiles = 65536;
};

/**
 * ClientConfigOption is the configuration information that needs to be set for
 * the peripheral snapshot system
 */
struct ClientConfigOption {
    LogInfo loginfo;
    IOOption ioOpt;
    CommonConfigOpt commonOpt;
    MetaServerOption metaServerOpt;
};

struct ChunkServerBroadCasterOption {
    uint32_t broadCastMaxNum;

    ChunkServerBroadCasterOption() : broadCastMaxNum(200) {}
};

struct ChunkServerClientRetryOptions {
    uint32_t rpcTimeoutMs;
    uint32_t rpcMaxTry;
    uint32_t rpcIntervalUs;
    uint32_t rpcMaxTimeoutMs;

    ChunkServerClientRetryOptions()
        : rpcTimeoutMs(500),
          rpcMaxTry(3),
          rpcIntervalUs(100000),
          rpcMaxTimeoutMs(8000) {}
};

/**
 * FileServiceOption is the overall configuration information on the QEMU side
 */
struct FileServiceOption {
    LogInfo loginfo;
    IOOption ioOpt;
    LeaseOption leaseOpt;
    CommonConfigOpt commonOpt;
    MetaServerOption metaServerOpt;
    ChunkServerClientRetryOptions csClientOpt;
    ChunkServerBroadCasterOption csBroadCasterOpt;
};

}  // namespace client
}  // namespace curve

#endif  // SRC_CLIENT_CONFIG_INFO_H_
