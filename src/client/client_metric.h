/*
 * Project: curve
 * File Created: Friday, 21st June 2019 3:09:09 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_CLIENT_CLIENT_METRIC_H_
#define SRC_CLIENT_CLIENT_METRIC_H_

#include <bvar/bvar.h>

#include <unistd.h>
#include <string>
#include <atomic>
#include <vector>

#include "src/common/timeutility.h"
#include "src/client/client_common.h"

using curve::common::TimeUtility;

namespace curve {
namespace client {

static void GetStringValue(std::ostream& os, void* arg) {
    os << *static_cast<std::string*>(arg);
}

static uint64_t GetUnInt64Value(void* arg) {
    return *static_cast<uint64_t*>(arg);
}

// 悬挂IO统计，文件级别统计，方便定位
typedef struct IOSuspendMetric {
    // 当前persecond计数总数
    bvar::Adder<uint64_t> count;
    IOSuspendMetric(const std::string& prefix,
                    const std::string& name) :
                    count(prefix, name + "_total_count")
    {}
} IOSuspendMetric_t;

// 秒级信息统计
typedef struct PerSecondMetric {
    // 当前persecond计数总数
    bvar::Adder<uint64_t> count;
    // persecond真实数据，这个数据依赖于count
    bvar::PerSecond<bvar::Adder<uint64_t>> value;
    PerSecondMetric(const std::string& prefix,
                    const std::string& name) :
                    count(prefix, name + "_total_count")
                    , value(prefix, name, &count, 1)
    {}
} PerSecondMetric_t;

// 接口统计信息metric信息统计
typedef struct InterfaceMetric {
    // 接口统计信息调用qps
    PerSecondMetric_t       qps;
    // error request persecond
    PerSecondMetric_t       eps;
    // receive request persecond
    PerSecondMetric_t       rps;
    // 调用吞吐
    PerSecondMetric_t       bps;
    // 调用超时次数qps
    PerSecondMetric_t       timeoutQps;
    // 调用latency
    bvar::LatencyRecorder   latency;

    InterfaceMetric(const std::string& prefix,
                      const std::string& name) :
                      qps(prefix, name + "_qps")
                    , eps(prefix, name + "_eps")
                    , rps(prefix, name + "_rps")
                    , bps(prefix, name + "_bps")
                    , timeoutQps(prefix, name + "_timeout_qps")
                    , latency(prefix, name + "_lat")
    {}
} InterfaceMetric_t;

// 文件级别metric信息统计
typedef struct FileMetric {
    // 当前metric归属于哪个文件
    std::string filename;
    const std::string prefix = "curve client";

    // 当前文件inflight io数量
    bvar::Adder<int64_t>                    inflightRPCNum;
    // 当前文件请求的最大请求字节数，这种统计方式可以很方便的看到最大值，分位值
    bvar::LatencyRecorder                   sizeRecorder;

    // libcurve最底层read rpc接口统计信息metric统计
    InterfaceMetric_t                      readRPC;
    // libcurve最底层write rpc接口统计信息metric统计
    InterfaceMetric_t                      writeRPC;
    // 用户读请求qps、eps、rps
    InterfaceMetric_t                      userRead;
    // 用户写请求qps、eps、rps
    InterfaceMetric_t                      userWrite;
    // get leader失败重试qps
    PerSecondMetric_t                       getLeaderRetryQPS;

    // 当前文件上的悬挂IO数量
    IOSuspendMetric_t                       suspendRPCMetric;

    FileMetric(std::string name) :
          filename(name)
        , userRead(prefix, filename + "_read")
        , userWrite(prefix, filename + "_write")
        , readRPC(prefix, filename + "_read_rpc")
        , writeRPC(prefix, filename + "_write_rpc")
        , inflightRPCNum(prefix, filename + "_inflight_rpc_num")
        , getLeaderRetryQPS(prefix, filename + "_get_leader_retry_rpc")
        , sizeRecorder(prefix, filename + "_write_request_size_recoder")
        , suspendRPCMetric(prefix, filename + "_suspend_io_num")
    {}
} FileMetric_t;


// 用于全局mds接口统计信息调用信息统计
typedef struct MDSClientMetric {
    const std::string prefix = "curve mds client";

    // mds的地址信息
    std::string metaserverAddr;
    bvar::PassiveStatus<std::string> metaserverAddress;

    // openfile接口统计信息
    InterfaceMetric_t      openFile;
    // createFile接口统计信息
    InterfaceMetric_t      createFile;
    // closeFile接口统计信息
    InterfaceMetric_t      closeFile;
    // getFileInfo接口统计信息
    InterfaceMetric_t      getFile;
    // RefreshSession接口统计信息
    InterfaceMetric_t      refreshSession;
    // GetServerList接口统计信息
    InterfaceMetric_t      getServerList;
    // GetOrAllocateSegment接口统计信息
    InterfaceMetric_t      getOrAllocateSegment;
    // RenameFile接口统计信息
    InterfaceMetric_t      renameFile;
    // Extend接口统计信息
    InterfaceMetric_t      extendFile;
    // DeleteFile接口统计信息
    InterfaceMetric_t      deleteFile;
    // changeowner接口统计信息
    InterfaceMetric_t      changeOwner;
    // listdir接口统计信息
    InterfaceMetric_t      listDir;
    // register接口统计信息
    InterfaceMetric_t      registerClient;
    // GetChunkServerID接口统计
    InterfaceMetric getChunkServerId;
    // ListChunkServerInServer接口统计
    InterfaceMetric listChunkserverInServer;

    // 切换mds server总次数
    bvar::Adder<uint64_t>   mdsServerChangeTimes;

    MDSClientMetric()
        : metaserverAddress(prefix, "current_metaserver_addr", GetStringValue,
                            &metaserverAddr),
          mdsServerChangeTimes(prefix, "mds_server_change_times"),
          openFile(prefix, "openFile"),
          createFile(prefix, "createFile"),
          closeFile(prefix, "closeFile"),
          getFile(prefix, "getFileInfo"),
          refreshSession(prefix, "refreshSession"),
          getServerList(prefix, "getServerList"),
          getOrAllocateSegment(prefix, "getOrAllocateSegment"),
          renameFile(prefix, "renameFile"),
          extendFile(prefix, "extendFile"),
          deleteFile(prefix, "deleteFile"),
          changeOwner(prefix, "changeOwner"),
          listDir(prefix, "listDir"),
          registerClient(prefix, "registerClient"),
          getChunkServerId(prefix, "GetChunkServerId"),
          listChunkserverInServer(prefix, "ListChunkServerInServer") {}
} MDSClientMetric_t;

typedef struct LatencyGuard {
    bvar::LatencyRecorder* latencyRec;
    uint64_t timeelapse;
    LatencyGuard(bvar::LatencyRecorder* latency) {
        latencyRec = latency;
        timeelapse = TimeUtility::GetTimeofDayUs();
    }

    ~LatencyGuard() {
        timeelapse = TimeUtility::GetTimeofDayUs() - timeelapse;
        *latencyRec << timeelapse;
    }
} LatencyGuard_t;


class MetricHelper {
 public:
    /**
     * 统计getleader重试次数
     * @param: fm为当前文件的metric指针
     */
    static void IncremGetLeaderRetryTime(FileMetric_t* fm) {
        if (fm != nullptr) {
            fm->getLeaderRetryQPS.count << 1;
        }
    }

    /**
     * 统计用户当前读写请求次数，用于qps计算
     * @param: fm为当前文件的metric指针
     * @param: length为当前请求大小
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremUserQPSCount(FileMetric_t* fm,
                                      uint64_t length,
                                      OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.qps.count << 1;
                    fm->userRead.bps.count << length;
                    break;
                case OpType::WRITE:
                    fm->userWrite.qps.count << 1;
                    fm->userWrite.bps.count << length;
                    fm->sizeRecorder << length;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 统计用户当前读写请求失败次数，用于eps计算
     * @param: fm为当前文件的metric指针
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremUserEPSCount(FileMetric_t* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.eps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->userWrite.eps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 统计用户当前接收到的读写请求次数，用于rps计算
     * rps: receive request persecond, 就是当前接口每秒接收到的请求数量
     * qps: query request persecond, 就是当前接口每秒处理的请求数量
     * eps: error request persecond, 就是当前接口每秒出错的请求数量
     * rps减去qps就是当前client端每秒钟等待的请求数量，这部分请求会持久占用当前一秒内的内存
     * @param: fm为当前文件的metric指针
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremUserRPSCount(FileMetric_t* fm, OpType type) {
        if (fm != nullptr) {
            switch (type) {
                case OpType::READ:
                    fm->userRead.rps.count << 1;
                    break;
                case OpType::WRITE:
                    fm->userWrite.rps.count << 1;
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 统计当前rpc失败次数，用于eps计算
     * @param: fm为当前文件的metric指针
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremFailRPCCount(FileMetric_t* fm, OpType type) {
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
     * 统计用户当前读写请求超时次数，用于timeoutQps计算
     * @param: fm为当前文件的metric指针
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremTimeOutRPCCount(FileMetric_t* fm, OpType type) {
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
     * 统计读写RPC接口统计信息请求次数及带宽统计，用于qps及bps计算
     * @param: fm为当前文件的metric指针
     * @param: length为当前请求大小
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremRPCQPSCount(FileMetric_t* fm,
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
     * 统计读写RPC接口统计信息请求次数及带宽统计，用于rps计算
     * @param: fm为当前文件的metric指针
     * @param: length为当前请求大小
     * @param: read为当前操作是读操作还是写操作
     */
    static void IncremRPCRPSCount(FileMetric_t* fm,
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

    static void LatencyRecord(FileMetric_t* fm,
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

    static void UserLatencyRecord(FileMetric_t* fm,
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
                default:
                    break;
            }
        }
    }

    static void IncremInflightRPC(FileMetric_t* fm) {
        if (fm != nullptr) {
            fm->inflightRPCNum << 1;
        }
    }

    static void DecremInflightRPC(FileMetric_t* fm) {
        if (fm != nullptr) {
            fm->inflightRPCNum << -1;
        }
    }

    static void IncremIOSuspendNum(FileMetric_t* fm) {
        if (fm != nullptr) {
            fm->suspendRPCMetric.count << 1;
        }
    }

    static void DecremIOSuspendNum(FileMetric_t* fm) {
        if (fm != nullptr) {
            fm->suspendRPCMetric.count.get_value() > 0 ?
            fm->suspendRPCMetric.count << -1 : fm->suspendRPCMetric.count << 0;
        }
    }
};
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CLIENT_METRIC_H_
