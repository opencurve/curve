/*
 * Project: curve
 * File Created: Tuesday, 18th September 2018 3:24:40 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#ifndef SRC_CLIENT_CLIENT_COMMON_H_
#define SRC_CLIENT_CLIENT_COMMON_H_

#include <butil/endpoint.h>
#include <butil/status.h>
#include <bvar/bvar.h>

#include <unistd.h>
#include <string>
#include <atomic>
#include <vector>

#include "src/client/libcurve_define.h"

namespace curve {
namespace client {
using ChunkID       = uint64_t;
using CopysetID     = uint32_t;
using LogicPoolID   = uint32_t;
using ChunkServerID = uint32_t;
using ChunkIndex    = uint32_t;

using EndPoint  = butil::EndPoint;
using Status    = butil::Status;

/**
 * 与nameserver.proto中的FileStatus一一对应
 */
enum class FileStatus {
    Created = 0,
    Deleting,
    Cloning,
    CloneMetaInstalled,
    Cloned
};

typedef struct ChunkIDInfo {
    ChunkID         cid_;
    CopysetID       cpid_;
    LogicPoolID     lpid_;
    ChunkIDInfo() {
        cid_  = 0;
        lpid_ = 0;
        cpid_ = 0;
    }

    ChunkIDInfo(ChunkID cid, LogicPoolID lpid, CopysetID cpid) {
        cid_  = cid;
        lpid_ = lpid;
        cpid_ = cpid;
    }

    ChunkIDInfo(const ChunkIDInfo& chunkinfo) {
        cid_  = chunkinfo.cid_;
        lpid_ = chunkinfo.lpid_;
        cpid_ = chunkinfo.cpid_;
    }

    ChunkIDInfo& operator=(const ChunkIDInfo& chunkinfo) {
        cid_  = chunkinfo.cid_;
        lpid_ = chunkinfo.lpid_;
        cpid_ = chunkinfo.cpid_;
        return *this;
    }

    bool Valid() {
        return lpid_ > 0 && cpid_ > 0;
    }
} ChunkIDInfo_t;

// 保存每个chunk对应的版本信息
typedef struct ChunkInfoDetail {
    std::vector<uint64_t> chunkSn;
} ChunkInfoDetail_t;

typedef struct LeaseSession {
    std::string sessionID;
    uint32_t leaseTime;
    uint64_t createTime;
} LeaseSession_t;

// 保存logicalpool中segment对应的copysetid信息
typedef struct LogicalPoolCopysetIDInfo {
    LogicPoolID lpid;
    std::vector<CopysetID> cpidVec;

    LogicalPoolCopysetIDInfo() {
        lpid = 0;
        cpidVec.clear();
    }
} LogicalPoolCopysetIDInfo_t;

// 保存每个segment的基本信息
typedef struct SegmentInfo {
    uint32_t segmentsize;
    uint32_t chunksize;
    uint64_t startoffset;
    std::vector<ChunkIDInfo> chunkvec;
    LogicalPoolCopysetIDInfo lpcpIDInfo;
} SegmentInfo_t;

typedef struct FInfo {
    uint64_t        id;
    uint64_t        parentid;
    FileType        filetype;
    uint32_t        chunksize;
    uint32_t        segmentsize;
    uint64_t        length;
    uint64_t        ctime;
    uint64_t        seqnum;
    std::string     owner;
    std::string     filename;
    std::string     fullPathName;
    FileStatus      filestatus;

    FInfo() {
        id = 0;
        ctime = 0;
        seqnum = 0;
        length = 0;                                                        // NOLINT
        chunksize = 4 * 1024 * 1024;
        segmentsize = 1 * 1024 * 1024 * 1024ul;
    }
} FInfo_t;

// 存储用户信息
typedef struct UserInfo {
    // 当前执行的owner信息
    std::string owner;
    // 当owner=root的时候，需要提供password作为计算signature的key
    std::string password;

    UserInfo() {
        owner = "";
        password = "";
    }

    UserInfo(std::string own, std::string pwd) {
        owner = own;
        password = pwd;
    }

    bool Valid() const {
        return owner != "";
    }
} UserInfo_t;

// ChunkServerAddr 代表一个copyset group里的一个chunkserver节点
// 与braft中的PeerID对应
struct ChunkServerAddr {
    // 节点的地址信息
    EndPoint addr_;

    ChunkServerAddr() = default;
    explicit ChunkServerAddr(butil::EndPoint addr) : addr_(addr) {}
    ChunkServerAddr(const ChunkServerAddr& csaddr) : addr_(csaddr.addr_) {}

    bool IsEmpty() const {
        return (addr_.ip == butil::IP_ANY && addr_.port == 0);
    }

    // 重置当前地址信息
    void Reset() {
        addr_.ip = butil::IP_ANY;
        addr_.port = 0;
    }

    // 从字符串中将地址信息解析出来
    int Parse(const std::string& str) {
        int idx;
        char ip_str[64];
        if (2 > sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%d", ip_str,
                       &addr_.port, &idx)) {
            Reset();
            return -1;
        }
        if (0 != butil::str2ip(ip_str, &addr_.ip)) {
            Reset();
            return -1;
        }
        return 0;
    }

    // 将该节点地址信息转化为字符串形式
    // 在get leader调用中可以将该值直接传入request
    std::string ToString() const {
        char str[128];
        snprintf(str, sizeof(str), "%s:%d",
                 butil::endpoint2str(addr_).c_str(), 0);
        return std::string(str);
    }

    bool operator==(const ChunkServerAddr& other) {
        return addr_ == other.addr_;
    }
};

extern uint64_t GetAtomicUint64(void* arg);

typedef struct ClientMetric {
    const std::string prefix = "curve client";
    // 读请求累计
    bvar::Adder<uint64_t>   readRequestCount;
    // 写请求累计
    bvar::Adder<uint64_t>   writeRequestCount;
    // 失败的读请求累计
    bvar::Adder<uint64_t>   readRequestFailCount;
    // 失败的写请求累计
    bvar::Adder<uint64_t>   writeRequestFailCount;
    // 读请求每秒计数
    bvar::PerSecond<bvar::Adder<uint64_t>>  readQps;
    // 写请求每秒计数
    bvar::PerSecond<bvar::Adder<uint64_t>>  writeQps;
    // 读请求每秒计数
    bvar::PerSecond<bvar::Adder<uint64_t>>  readFailQps;
    // 写请求每秒计数
    bvar::PerSecond<bvar::Adder<uint64_t>>  writeFailQps;

    // 读请求累计字节计数
    bvar::Adder<uint64_t>   readBytesCount;
    // 写请求累计字节计数
    bvar::Adder<uint64_t>   writeBytesCount;
    // 读请求每秒累计字节计数
    bvar::PerSecond<bvar::Adder<uint64_t>> readBps;
    // 写请求每秒累计字节计数
    bvar::PerSecond<bvar::Adder<uint64_t>> writeBps;

    // 读请求平均时延
    bvar::LatencyRecorder   readRequestLatency;
    // 写请求平均时延
    bvar::LatencyRecorder   writeRequestLatency;

    ClientMetric() :
          readRequestCount(prefix, "read_request_count")
        , writeRequestCount(prefix, "write_request_count")
        , readRequestFailCount(prefix, "read_fail_request_count")
        , writeRequestFailCount(prefix, "write_fail_request_count")
        , readQps(prefix, "read_qps", &readRequestCount)
        , writeQps(prefix, "write_qps", &writeRequestCount)
        , readFailQps(prefix, "read_fail_qps", &readRequestFailCount)
        , writeFailQps(prefix, "write_fail_qps", &writeRequestFailCount)
        , readBytesCount(prefix, "read_bytes_count")
        , writeBytesCount(prefix, "write_bytes_count")
        , readBps(prefix, "read_bps", &readBytesCount)
        , writeBps(prefix, "write_bps", &writeBytesCount)
        , readRequestLatency("curve_client_read_latency")
        , writeRequestLatency("curve_client_write_latency") {}
} ClientMetric_t;

typedef struct MDSClientMetric {
    const std::string prefix = "libcurve mds client";
    std::atomic<uint64_t>    timeoutCount;
    std::atomic<uint64_t>    mdsServerChangeCount;

    // 与mds通信超时次数
    bvar::Adder<uint64_t>                   timeoutTimes;
    // 单位时间内与mds通信超时次数
    bvar::PerSecond<bvar::Adder<uint64_t>>  timeoutTimes_60s;
    // 切换mds server总次数
    bvar::Adder<uint64_t>                   mdsServerChangeTimes;
    // 单位时间内切换mds server次数
    bvar::PerSecond<bvar::Adder<uint64_t>>  mdsServerChanges_5m;

    MDSClientMetric() : timeoutTimes(prefix, "mds_server_timeout_times")
                      , timeoutTimes_60s(prefix,
                             "mds_server_timeout_times_60s",
                             &timeoutTimes,
                             60)
                      , mdsServerChangeTimes(prefix, "mds_server_change_times")
                      , mdsServerChanges_5m(prefix,
                             "mds_server_change_times_5m",
                             &mdsServerChangeTimes,
                             300) {}
} MDSClientMetric_t;

typedef struct ChunkserverClientMetric {
    const std::string prefix = "libcurve chunkserver client";

    // change leader总次数
    bvar::Adder<uint64_t>                   leaderChangeTimes;
    // 1分钟内leader变更次数
    bvar::PerSecond<bvar::Adder<uint64_t>>  leaderChanges_60s;
    // 超时引起的带宽占用Bytes
    bvar::Adder<uint64_t>                   retryBytes;
    // 超时引起的带宽占用bps
    bvar::PerSecond<bvar::Adder<uint64_t>>  retryBytesBps;
    // 与chunkserver通信超时次数统计
    bvar::Adder<uint64_t>                   retryCount;
    // 1分钟内与chunkserver通信超时次数统计
    bvar::PerSecond<bvar::Adder<uint64_t>>  retryCount_60s;

    ChunkserverClientMetric() :
           leaderChangeTimes(prefix, "leader_change_times")
         , leaderChanges_60s(prefix,
                             "leader_change_times_60s",
                             &leaderChangeTimes,
                             60)
         , retryBytes(prefix, "retry_bytes")
         , retryBytesBps(prefix,
                             "retry_bytes_bps",
                             &retryBytes,
                             1)
         , retryCount(prefix, "retry_count")
         , retryCount_60s(prefix,
                             "retry_count_bps",
                             &retryCount,
                             60) {}
} ChunkserverClientMetric_t;
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CLIENT_COMMON_H_
