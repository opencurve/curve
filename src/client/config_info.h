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
 *Basic configuration information of log
 * @logLevel: It is the log printing level
 * @logPath: Log printing location
 */
struct LogInfo {
    int logLevel = 2;
    std::string logPath;
};

/**
 *In flight IO control information
 * @fileMaxInFlightRPCNum: is the maximum allowed number of inflight IOs in a file
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
 *Basic configuration of lease
 * @mdsRefreshTimesPerLease: The number of renewals within a lease, and the heartbeat between client and mds is maintained through the lease
 *If both parties agree that the lease term is 10 seconds, then the client will be within these 10 seconds
 *Send mdsRefreshTimesPerLease heartbeats, if consecutive failures occur,
 *So the client believes that there is an abnormality in the current mds, which will block subsequent IO until
 *Successfully renewed the contract.
 */
struct LeaseOption {
    uint32_t mdsRefreshTimesPerLease = 5;
};

/**
 *RPC timeout, parameter to determine if it is unstable
 * @maxStableChunkServerTimeoutTimes:
 *The threshold for a chunkserver to continuously timeout requests, after which the health status will be checked,
 *If not healthy, mark as unstable
 * @checkHealthTimeoutMS:
 *Check if chunkserver is healthy HTTP request timeout
 * @serverUnstableThreashold:
 *More than serverUnstableThreashold chunkservers on a server are marked as unstable,
 *All chunkservers on the entire server are marked as unstable
 */
struct ChunkServerUnstableOption {
    uint32_t maxStableChunkServerTimeoutTimes = 64;
    uint32_t checkHealthTimeoutMS = 100;
    uint32_t serverUnstableThreshold = 3;
};

/**
 *Send failed chunk request processing
 * @chunkserverOPMaxRetry:
 *Maximum number of retries, one RPC issued to the underlying chunkserver, maximum allowed failures
 *After exceeding the limit, the user will be returned upwards.
 * @chunkserverOPRetryIntervalUS:
 *How long does it take to retry? After an RPC fails, the client will return based on it
 *The status determines whether to sleep for a period of time before trying again. Currently, except for
 *TIMEOUT, REDIRECTED, these two return values, all other return values require
 *Sleep for a while before trying again.
 * @chunkserverRPCTimeoutMS: The timeout configured by the rpc controller for each rpc when sent
 * @chunkserverMaxRPCTimeoutMS:
 *When the underlying chunkserver returns TIMEOUT, it indicates that the current request is in the underlying
 *Unable to receive timely processing, possibly due to too many queuing tasks at the bottom. At this time, if
 *With the same rpc
 *Sending a request after a timeout is likely to result in a timeout,
 *So in order to avoid the situation where rpc has already timed out on the client side when processing requests at the bottom level
 *The situation has added exponential backoff logic to the RPC timeout, and the timeout will gradually increase,
 *The maximum cannot exceed this value.
 * @chunkserverMaxRetrySleepIntervalUS:
 * When the underlying system returns OVERLOAD, it indicates that the current chunkserver
 * is under heavy load. At this point, the sleep interval will undergo exponential backoff,
 * and the sleep time will increase. This ensures that client requests do not overwhelm the
 * underlying chunkserver, but the maximum sleep time cannot exceed this value.
 * @chunkserverMaxStableTimeoutTimes: A threshold for consecutive timeout requests from a chunkserver.
 * If exceeded, it will be marked as unstable. When a server that a chunkserver resides on crashes,
 * all requests sent to that chunkserver will timeout. If the rpc from the same chunkserver
 * continuously times out beyond this threshold, the client assumes that the server where this
 * chunkserver resides may have crashed. As a result, all leader copysets on that server are marked
 * as unstable, prompting them to first get the leader before sending rpc next time.
 * @chunkserverMinRetryTimesForceTimeoutBackoff:
 * When the number of retries for a request exceeds the threshold, the timeout time is exponentially backed off.
 * @chunkserverMaxRetryTimesBeforeConsiderSuspend:
 * When the number of rpc retries exceeds this count, it is considered as a suspended I/O.
 * Because the number of retries for rpc sent to the underlying chunkserver is very large,
 * if an rpc fails continuously beyond this threshold, it can be considered that the current I/O
 * is in a suspended state, and an alert is sent upwards through metrics.
 */
struct FailureRequestOption {
    uint32_t chunkserverOPMaxRetry = 3;
    uint64_t chunkserverOPRetryIntervalUS = 200;
    uint64_t chunkserverRPCTimeoutMS = 1000;
    uint64_t chunkserverMaxRPCTimeoutMS = 64000;
    uint64_t chunkserverMaxRetrySleepIntervalUS = 64ull * 1000 * 1000;
    uint64_t chunkserverMinRetryTimesForceTimeoutBackoff = 5;
    uint64_t chunkserverMaxRetryTimesBeforeConsiderSuspend = 20;
};

/**
 *Configuration for sending rpc to chunkserver
 * @inflightOpt: Configuration of inflight request control when a file sends a request to chunkserver
 * @failRequestOpt: After rpc sending fails, relevant configuration for rpc retry needs to be carried out
 */
struct IOSenderOption {
    InFlightIOCntlInfo inflightOpt;
    FailureRequestOption failRequestOpt;
};

/**
 *The basic configuration information of the scheduler module is used to distribute user requests, and each file has its own schedule
 *Thread pool, where each thread in the thread pool is configured with a queue
 * @scheduleQueueCapacity: The queue depth configured by the schedule module
 * @scheduleThreadpoolSize: Schedule module thread pool size
 */
struct RequestScheduleOption {
    uint32_t scheduleQueueCapacity = 1024;
    uint32_t scheduleThreadpoolSize = 2;
    IOSenderOption ioSenderOpt;
};

/**
 *Metacache module configuration information
 * @metacacheGetLeaderRetry:
 *Obtain the number of retries for the leader. Before sending an RPC to the chunkserver, it is necessary to first
 *Obtain the leader of the current copyset. If this information is not available in the metacache,
 *Send a getleader request to the peer of the copyset. If the getleader fails,
 *Need to retry, the maximum number of retries is this value.
 * @metacacheRPCRetryIntervalUS:
 *As mentioned above, if the getleader request fails, a retry will be initiated, but
 *Instead of immediately retrying, choose to sleep for a period of time before retrying. This value represents
 *Sleep length.
 * @metacacheGetLeaderRPCTimeOutMS: Send rpc for getleader rpc request
 *Controller maximum timeout time
 * @metacacheGetLeaderBackupRequestMS:
 *Because a copyset has three or more peers, getleaders
 *RPCs will be sent to these peers in the form of backuprequest, within the BRPC
 *It will be sent serially. If the first request has not returned after a certain period of time, it will be directly sent to the
 *The next peer sends a request without waiting for the previous request to return or timeout, which triggers
 *The time of the backup request is this value.
 * @metacacheGetLeaderBackupRequestLbName: for getleader backup rpc
 *Strategies for selecting underlying service nodes
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
 *IO Split Module Configuration Information
 * @fileIOSplitMaxSizeKB:
 *There is no limit on the size of IO issued by the user to the client, but the client will split the user's IO,
 *The data size carried by a request lock to the same chunkserver cannot exceed this value.
 */
struct IOSplitOption {
    uint64_t fileIOSplitMaxSizeKB = 64;
};

/**
 *Thread Isolation Task Queue Configuration Information
 *Thread isolation is mainly used to push asynchronous interface calls directly to the thread pool instead of blocking them until they are placed
 *Distribution queue thread pool.
 * @isolationTaskQueueCapacity: The queue depth of the isolation thread pool
 * @isolationTaskThreadPoolSize: Isolate thread pool capacity
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
 *IOOption stores all the configuration information required for the current IO operation
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
 *Common configuration information on the client side
 * @mdsRegisterToMDS: Do you want to register client information with mds, as the client needs to pass dummy
 *The server exports metric information, and in order to cooperate with Prometheus' automatic service discovery mechanism, it will be monitored
 *Send IP and port information to MDS.
 * @turnOffHealthCheck: Do you want to turn off the health check
 */
struct CommonConfigOpt {
    bool mdsRegisterToMDS = false;
    bool turnOffHealthCheck = false;
};

/**
 *ClientConfigOption is the configuration information that needs to be set for the peripheral snapshot system
 */
struct ClientConfigOption {
    LogInfo loginfo;
    IOOption ioOpt;
    CommonConfigOpt commonOpt;
    MetaServerOption metaServerOpt;
};

struct ChunkServerBroadCasterOption {
    uint32_t broadCastMaxNum;

    ChunkServerBroadCasterOption()
      : broadCastMaxNum(200) {}
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
 *FileServiceOption is the overall configuration information on the QEMU side
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
