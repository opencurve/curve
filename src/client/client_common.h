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
 * File Created: Tuesday, 18th September 2018 3:24:40 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_CLIENT_COMMON_H_
#define SRC_CLIENT_CLIENT_COMMON_H_

#include <butil/endpoint.h>
#include <butil/status.h>
#include <google/protobuf/stubs/callback.h>

#include <string>
#include <vector>
#include <unordered_set>

#include "include/client/libcurve.h"
#include "include/client/libcurve_define.h"
#include "src/common/throttle.h"

namespace curve {
namespace client {

using ChunkID = uint64_t;
using CopysetID = uint32_t;
using LogicPoolID = uint32_t;
using ChunkServerID = uint32_t;
using ChunkIndex = uint32_t;
using SegmentIndex = uint32_t;

using EndPoint = butil::EndPoint;
using Status = butil::Status;

using IOManagerID = uint64_t;

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;

// 操作类型
enum class OpType {
    READ = 0,
    WRITE,
    READ_SNAP,
    DELETE_SNAP,
    CREATE_CLONE,
    RECOVER_CHUNK,
    GET_CHUNK_INFO,
    DISCARD,
    UNKNOWN
};

/**
 * 与nameserver.proto中的FileStatus一一对应
 */
enum class FileStatus {
    Created = 0,
    Deleting,
    Cloning,
    CloneMetaInstalled,
    Cloned,
    BeingCloned,
};

typedef struct ChunkIDInfo {
    ChunkID cid_ = 0;
    CopysetID cpid_ = 0;
    LogicPoolID lpid_ = 0;

    bool chunkExist = true;

    ChunkIDInfo() = default;

    ChunkIDInfo(ChunkID cid, LogicPoolID lpid, CopysetID cpid)
        : cid_(cid), cpid_(cpid), lpid_(lpid) {}

    bool Valid() const {
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

struct CloneSourceInfo {
    std::string name;
    uint64_t length = 0;
    uint64_t segmentSize = 0;
    std::unordered_set<uint64_t> allocatedSegmentOffsets;

    CloneSourceInfo() = default;

    bool IsSegmentAllocated(uint64_t offset) const;
};

struct OpenContext {
    int openflags;
    std::string uuid;
    OpenContext()
        : openflags(CurveOpenFlags::CURVE_FORCE_WRITE) {}
};

inline const std::string TransformFlags(const int openflags) noexcept {
    switch (openflags) {
        case CURVE_EXCLUSIVE | CURVE_FORCE_WRITE:
            return "CURVE_EXCLUSIVE | CURVE_FORCE_WRITE";
        case CURVE_EXCLUSIVE | CURVE_RDONLY:
            return "CURVE_EXCLUSIVE | CURVE_RDONLY";
        case CURVE_EXCLUSIVE | CURVE_RDWR:
            return "CURVE_EXCLUSIVE | CURVE_RDWR";
        case CURVE_SHARED | CURVE_RDWR:
            return "CURVE_SHARED | CURVE_RDWR";
        case CURVE_SHARED | CURVE_FORCE_WRITE:
            return "CURVE_SHARED | CURVE_FORCE_WRITE";
        case CURVE_SHARED | CURVE_RDONLY:
            return "CURVE_SHARED | CURVE_RDONLY";
        default:
            return "CURVE_FLAG_ERROR";
    }
}

inline bool CheckFlags(const int openflags) noexcept {
    switch (openflags) {
        case CURVE_EXCLUSIVE | CURVE_FORCE_WRITE:
        case CURVE_EXCLUSIVE | CURVE_RDONLY:
        case CURVE_EXCLUSIVE | CURVE_RDWR:
        case CURVE_SHARED | CURVE_RDWR:
        case CURVE_SHARED | CURVE_FORCE_WRITE:
        case CURVE_SHARED | CURVE_RDONLY:
            return true;
        default:
            return false;
    }
}

typedef struct FInfo {
    uint64_t id;
    uint64_t parentid;
    FileType filetype;
    uint32_t chunksize;
    uint32_t segmentsize;
    uint64_t length;
    uint64_t ctime;
    uint64_t seqnum;
    // userinfo是当前操作这个文件的用户信息
    UserInfo_t userinfo;
    // owner是当前文件所属信息
    std::string owner;
    std::string filename;
    std::string fullPathName;
    FileStatus filestatus;

    CloneSourceInfo sourceInfo;
    std::string cloneSource;
    uint64_t cloneLength{0};
    uint64_t stripeUnit;
    uint64_t stripeCount;
    OpenContext context;
    common::ReadWriteThrottleParams throttleParams;

    FInfo() {
        id = 0;
        ctime = 0;
        seqnum = 0;
        length = 0;
        chunksize = 4 * 1024 * 1024;
        segmentsize = 1 * 1024 * 1024 * 1024ul;
        stripeUnit = 0;
        stripeCount = 0;
    }
} FInfo_t;

typedef struct FileEpoch {
    uint64_t fileId;
    uint64_t epoch;

    FileEpoch() {
        fileId = 0;
        epoch = 0;
    }
} FileEpoch_t;

// PeerAddr 代表一个copyset group里的一个chunkserver节点
// 与braft中的PeerID对应
struct PeerAddr {
    // 节点的地址信息
    EndPoint addr_;

    PeerAddr() = default;
    explicit PeerAddr(butil::EndPoint addr) : addr_(addr) {}

    bool IsEmpty() const {
        return (addr_.ip == butil::IP_ANY && addr_.port == 0) &&
                addr_.socket_file.empty();
    }

    // 重置当前地址信息
    void Reset() {
        addr_.ip = butil::IP_ANY;
        addr_.port = 0;
    }

    // 从字符串中将地址信息解析出来
    int Parse(const std::string &str) {
        int idx;
        char ip_str[64];
        if (2 > sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%d", ip_str, &addr_.port,
                       &idx)) {
            Reset();
            return -1;
        }
        int ret = butil::str2ip(ip_str, &addr_.ip);
        if (0 != ret) {
            Reset();
            return -1;
        }
        return 0;
    }

    // 将该节点地址信息转化为字符串形式
    // 在get leader调用中可以将该值直接传入request
    std::string ToString() const {
        char str[128];
        snprintf(str, sizeof(str), "%s:%d", butil::endpoint2str(addr_).c_str(),
                 0);
        return std::string(str);
    }

    bool operator==(const PeerAddr &other) const {
        return addr_ == other.addr_;
    }
};

inline const char *OpTypeToString(OpType optype) {
    switch (optype) {
    case OpType::READ:
        return "Read";
    case OpType::WRITE:
        return "Write";
    case OpType::READ_SNAP:
        return "ReadSnapshot";
    case OpType::DELETE_SNAP:
        return "DeleteSnapshot";
    case OpType::CREATE_CLONE:
        return "CreateCloneChunk";
    case OpType::RECOVER_CHUNK:
        return "RecoverChunk";
    case OpType::GET_CHUNK_INFO:
        return "GetChunkInfo";
    case OpType::DISCARD:
        return "Discard";
    case OpType::UNKNOWN:
    default:
        return "Unknown";
    }
}

struct ClusterContext {
    std::string clusterId;
};

class SnapCloneClosure : public google::protobuf::Closure {
 public:
    SnapCloneClosure() : ret(-LIBCURVE_ERROR::FAILED) {}

    void SetRetCode(int retCode) { ret = retCode; }
    int GetRetCode() { return ret; }

 private:
    int ret;
};

class ClientDummyServerInfo {
 public:
    static ClientDummyServerInfo &GetInstance() {
        static ClientDummyServerInfo clientInfo;
        return clientInfo;
    }

    void SetIP(const std::string &ip) { localIP_ = ip; }

    std::string GetIP() const {
        return localIP_;
    }

    void SetPort(uint32_t port) { localPort_ = port; }

    uint32_t GetPort() const { return localPort_; }

    void SetRegister(bool registerFlag) { register_ = registerFlag; }

    bool GetRegister() const { return register_; }

 private:
    ClientDummyServerInfo() = default;

 private:
    std::string localIP_;
    uint32_t localPort_ = 0;
    bool register_ = false;
};

inline void TrivialDeleter(void*) {}

inline const char *FileStatusToName(FileStatus status) {
    switch (status) {
    case FileStatus::Created:
        return "Created";
    case FileStatus::Deleting:
        return "Deleting";
    case FileStatus::Cloning:
        return "Cloning";
    case FileStatus::CloneMetaInstalled:
        return "CloneMetaInstalled";
    case FileStatus::Cloned:
        return "Cloned";
    case FileStatus::BeingCloned:
        return "BeingCloned";
    default:
        return "Unknown";
    }
}

inline bool CloneSourceInfo::IsSegmentAllocated(uint64_t offset) const {
    if (length == 0) {
        return false;
    }

    uint64_t segmentOffset = offset / segmentSize * segmentSize;
    return allocatedSegmentOffsets.count(segmentOffset) != 0;
}

// default flags for readonly open
inline int DefaultReadonlyOpenFlags() {
    return CURVE_EXCLUSIVE | CURVE_RDONLY;
}
}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_CLIENT_COMMON_H_
