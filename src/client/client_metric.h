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
 * File Created: Friday, 21st June 2019 3:09:09 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_CLIENT_METRIC_H_
#define SRC_CLIENT_CLIENT_METRIC_H_

#include <bvar/bvar.h>

#include <string>
#include <vector>

#include "src/common/timeutility.h"
#include "src/client/client_common.h"
#include "src/common/string_util.h"

using curve::common::TimeUtility;

namespace curve {
namespace client {

inline void GetStringValue(std::ostream& os, void* arg) {
    os << *static_cast<std::string*>(arg);
}

//Suspend IO statistics and file level statistics for easy positioning
struct IOSuspendMetric {
    //Current total number of second counts
    bvar::Adder<uint64_t> count;

    IOSuspendMetric(const std::string& prefix, const std::string& name)
        : count(prefix, name + "_total_count") {}
};

//Second-level information statistics
struct PerSecondMetric {
    //Current total number of second counts
    bvar::Adder<uint64_t> count;
    //persecond real data depends on the count
    bvar::PerSecond<bvar::Adder<uint64_t>> value;

    PerSecondMetric(const std::string& prefix, const std::string& name)
        : count(prefix, name + "_total_count"),
          value(prefix, name, &count, 1) {}
};

//Interface statistics information metric information statistics
struct InterfaceMetric {
    //Call qps for interface statistics information
    PerSecondMetric qps;
    // error request persecond
    PerSecondMetric eps;
    // receive request persecond
    PerSecondMetric rps;
    //Call throughput
    PerSecondMetric bps;
    //Call timeout count qps
    PerSecondMetric timeoutQps;
    //Number of calls to redirect qps
    PerSecondMetric redirectQps;
    //Call latency
    bvar::LatencyRecorder latency;

    InterfaceMetric(const std::string& prefix, const std::string& name)
        : qps(prefix, name + "_qps"),
          eps(prefix, name + "_eps"),
          rps(prefix, name + "_rps"),
          bps(prefix, name + "_bps"),
          timeoutQps(prefix, name + "_timeout_qps"),
          redirectQps(prefix, name + "_redirect_qps"),
          latency(prefix, name + "_lat") {}
};

struct DiscardMetric {
    explicit DiscardMetric(const std::string& prefix)
        : totalSuccess(prefix, "discard_total_success"),
          totalError(prefix, "discard_total_error"),
          totalCanceled(prefix, "discard_total_canceled"),
          pending(prefix, "discard_pending") {}

    bvar::Adder<int64_t> totalSuccess;
    bvar::Adder<int64_t> totalError;
    bvar::Adder<int64_t> totalCanceled;
    bvar::Adder<int64_t> pending;
};

//File level metric information statistics
struct FileMetric {
    const std::string prefix = "curve_client";

    //Which file does the current metric belong to
    std::string filename;

    //Current file inflight io quantity
    bvar::Adder<int64_t> inflightRPCNum;

    //The maximum number of request bytes for the current file request, which is a convenient statistical method to see the maximum and quantile values
    bvar::LatencyRecorder readSizeRecorder;
    bvar::LatencyRecorder writeSizeRecorder;
    bvar::LatencyRecorder discardSizeRecorder;

    //Libcurve's lowest level read rpc interface statistics information metric statistics
    InterfaceMetric readRPC;
    //Libcurve's lowest level write rpc interface statistics information metric statistics
    InterfaceMetric writeRPC;
    //User Read Request QPS, EPS, RPS
    InterfaceMetric userRead;
    //User write request QPS, EPS, RPS
    InterfaceMetric userWrite;
    // user's discard request
    InterfaceMetric userDiscard;

    //Get leader failed and retry qps
    PerSecondMetric getLeaderRetryQPS;

    //The number of suspended IOs on the current file
    IOSuspendMetric suspendRPCMetric;

    DiscardMetric discardMetric;

    explicit FileMetric(const std::string& name)
        : filename(name),
          inflightRPCNum(prefix, filename + "_inflight_rpc_num"),
          readSizeRecorder(prefix, filename + "_read_request_size_recoder"),
          writeSizeRecorder(prefix, filename + "_write_request_size_recoder"),
          discardSizeRecorder(prefix, filename + "_discard_request_size_recoder"),  // NOLINT
          readRPC(prefix, filename + "_read_rpc"),
          writeRPC(prefix, filename + "_write_rpc"),
          userRead(prefix, filename + "_read"),
          userWrite(prefix, filename + "_write"),
          userDiscard(prefix, filename + "_discard"),
          getLeaderRetryQPS(prefix, filename + "_get_leader_retry_rpc"),
          suspendRPCMetric(prefix, filename + "_suspend_io_num"),
          discardMetric(prefix + filename) {}
};

//Used for global mds interface statistics, call information statistics
struct MDSClientMetric {
    std::string prefix;

    //Address information of mds
    std::string metaserverAddr;
    bvar::PassiveStatus<std::string> metaserverAddress;

    //openFile interface statistics
    InterfaceMetric openFile;
    //createFile interface statistics
    InterfaceMetric createFile;
    //closeFile interface statistics
    InterfaceMetric closeFile;
    //GetFileInfo interface statistics
    InterfaceMetric getFile;
    //RefreshSession Interface Statistics
    InterfaceMetric refreshSession;
    //GetServerList interface statistics
    InterfaceMetric getServerList;
    //GetOrAllocateSegment interface statistics
    InterfaceMetric getOrAllocateSegment;
    //DeAllocateSegment Interface Statistics
    InterfaceMetric deAllocateSegment;
    //RenameFile Interface Statistics
    InterfaceMetric renameFile;
    //Extend Interface Statistics
    InterfaceMetric extendFile;
    //deleteFile interface statistics
    InterfaceMetric deleteFile;
    // RecoverFile interface metric
    InterfaceMetric recoverFile;
    //changeOwner Interface Statistics
    InterfaceMetric changeOwner;
    //Listdir interface statistics
    InterfaceMetric listDir;
    //Register Interface Statistics
    InterfaceMetric registerClient;
    //GetChunkServerID interface statistics
    InterfaceMetric getChunkServerId;
    //ListChunkServerInServer Interface Statistics
    InterfaceMetric listChunkserverInServer;
    // IncreaseEpoch
    InterfaceMetric increaseEpoch;

    //Total number of switching MDS server
    bvar::Adder<uint64_t> mdsServerChangeTimes;

    explicit MDSClientMetric(const std::string& prefix_ = "")
        : prefix(!prefix_.empty()
                     ? prefix_
                     : "curve_mds_client_" + common::ToHexString(this)),
          metaserverAddress(prefix, "current_metaserver_addr", GetStringValue,
                            &metaserverAddr),
          openFile(prefix, "openFile"),
          createFile(prefix, "createFile"),
          closeFile(prefix, "closeFile"),
          getFile(prefix, "getFileInfo"),
          refreshSession(prefix, "refreshSession"),
          getServerList(prefix, "getServerList"),
          getOrAllocateSegment(prefix, "getOrAllocateSegment"),
          deAllocateSegment(prefix, "deAllocateSegment"),
          renameFile(prefix, "renameFile"),
          extendFile(prefix, "extendFile"),
          deleteFile(prefix, "deleteFile"),
          recoverFile(prefix, "recoverFile"),
          changeOwner(prefix, "changeOwner"),
          listDir(prefix, "listDir"),
          registerClient(prefix, "registerClient"),
          getChunkServerId(prefix, "GetChunkServerId"),
          listChunkserverInServer(prefix, "ListChunkServerInServer"),
          increaseEpoch(prefix, "IncreaseEpoch"),
          mdsServerChangeTimes(prefix, "mds_server_change_times") {}
};

struct LatencyGuard {
    bvar::LatencyRecorder* latencyRec;
    uint64_t startTimeUs;

    explicit LatencyGuard(bvar::LatencyRecorder* latency) {
        latencyRec = latency;
        startTimeUs = TimeUtility::GetTimeofDayUs();
    }

    ~LatencyGuard() {
        *latencyRec << (TimeUtility::GetTimeofDayUs() - startTimeUs);
    }
};

class MetricHelper {
 public:
    /**
     *Count the number of retries for getleader
     * @param: fm is the metric pointer of the current file
     */
    static void IncremGetLeaderRetryTime(FileMetric* fm) {
        if (fm != nullptr) {
            fm->getLeaderRetryQPS.count << 1;
        }
    }

    /**
     *Count the current number of read and write requests from users for QPS calculation
     * @param: fm is the metric pointer of the current file
     * @param: length is the current request size
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremUserQPSCount(FileMetric* fm,
                                   uint64_t length,
                                   OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.qps.count << 1;
                    fm->userRead.bps.count << length;
                    fm->readSizeRecorder << length;
                    break;
                case OpType::WRITE:
                    fm->userWrite.qps.count << 1;
                    fm->userWrite.bps.count << length;
                    fm->writeSizeRecorder << length;
                    break;
                case OpType::DISCARD:
                    fm->userDiscard.qps.count << 1;
                    fm->userDiscard.bps.count << length;
                    fm->discardSizeRecorder << length;
                default:
                    break;
            }
        }
    }

    /**
     *Count the current number of failed read/write requests by users for EPS calculation
     * @param: fm is the metric pointer of the current file
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremUserEPSCount(FileMetric* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.eps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->userWrite.eps.count << 1;
                    break;
                case OpType::DISCARD:
                    fm->userDiscard.eps.count << 1;
                default:
                    break;
            }
        }
    }

    /**
     *Count the number of read and write requests currently received by the user for RPS calculation
     *Rps: receive request persecond, which is the number of requests received by the current interface per second
     *QPS: query request persecond, which is the number of requests processed by the current interface per second
     *Eps: error request persecond, which is the number of requests that make errors per second on the current interface
     *Rps minus qps is the number of requests that the current client is waiting for per second, which will persistently occupy the current memory for one second
     * @param: fm is the metric pointer of the current file
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremUserRPSCount(FileMetric* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.rps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->userWrite.rps.count << 1;
                    break;
                case OpType::DISCARD:
                    fm->userDiscard.rps.count << 1;
                default:
                    break;
            }
        }
    }

    /**
     *Count the current number of RPC failures for EPS calculation
     * @param: fm is the metric pointer of the current file
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremFailRPCCount(FileMetric* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->readRPC.eps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->writeRPC.eps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     *Counts the number of times a user's current read/write request has timed out, used for timeoutQps calculation
     * @param: fm is the metric pointer of the current file
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremTimeOutRPCCount(FileMetric* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->readRPC.timeoutQps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->writeRPC.timeoutQps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     *Count the number of times requests have been redirected
     * @param fileMetric The metric pointer of the current file
     * @param opType request type
     */
    static void IncremRedirectRPCCount(FileMetric* fileMetric, OpType opType) {
        if (fileMetric) {
            switch (opType) {
                case OpType::READ:
                    fileMetric->readRPC.redirectQps.count << 1;
                    break;
                case OpType::WRITE:
                    fileMetric->writeRPC.redirectQps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     *Statistics of the number of requests and bandwidth for reading and writing RPC interfaces, used for QPS and bps calculations
     * @param: fm is the metric pointer of the current file
     * @param: length is the current request size
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremRPCQPSCount(FileMetric* fm,
                                  uint64_t length,
                                  OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->readRPC.qps.count << 1;
                    fm->readRPC.bps.count << length;
                    break;
                case OpType::WRITE:
                    fm->writeRPC.qps.count << 1;
                    fm->writeRPC.bps.count << length;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     *Statistics of the number of requests and bandwidth for reading and writing RPC interfaces, used for RPS calculations
     * @param: fm is the metric pointer of the current file
     * @param: length is the current request size
     * @param: read is whether the current operation is a read or write operation
     */
    static void IncremRPCRPSCount(FileMetric* fm,
                                  OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->readRPC.rps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->writeRPC.rps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    static void LatencyRecord(FileMetric* fm,
                              uint64_t duration,
                              OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->readRPC.latency << duration;
                    break;
                case OpType::WRITE:
                    fm->writeRPC.latency << duration;
                    break;
                default:
                    break;
            }
        }
    }

    static void UserLatencyRecord(FileMetric* fm,
                                  uint64_t duration,
                                  OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.latency << duration;
                    break;
                case OpType::WRITE:
                    fm->userWrite.latency << duration;
                    break;
                case OpType::DISCARD:
                    fm->userDiscard.latency << duration;
                default:
                    break;
            }
        }
    }

    static void IncremInflightRPC(FileMetric* fm) {
        if (fm != nullptr) {
            fm->inflightRPCNum << 1;
        }
    }

    static void DecremInflightRPC(FileMetric* fm) {
        if (fm != nullptr) {
            fm->inflightRPCNum << -1;
        }
    }

    static void IncremIOSuspendNum(FileMetric* fm) {
        if (fm != nullptr) {
            fm->suspendRPCMetric.count << 1;
        }
    }

    static void DecremIOSuspendNum(FileMetric* fm) {
        if (fm != nullptr) {
            fm->suspendRPCMetric.count.get_value() > 0
                ? fm->suspendRPCMetric.count << -1
                : fm->suspendRPCMetric.count << 0;
        }
    }
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CLIENT_METRIC_H_
