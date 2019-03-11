/*
 * Project: curve
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_COMMON_MDS_DEFINE_H_
#define CURVE_SRC_MDS_COMMON_MDS_DEFINE_H_

#include <cstdint>

// TODO(xuchaojie): 统一MDS中类型定义和错误码定义

namespace curve {
namespace mds {
// 错误码：MDS 通用执行成功
const int kMdsSuccess = 0;
// 错误码：MDS 通用执行失败
const int kMdsFail = -1;
// 错误码：chunkserverclient内部错误
const int kCsClientInternalError = -2;
// 错误码：chunkserverclient请求非leader
const int kCsClientNotLeader = -3;
// 错误码: brpc channel init fail
const int kRpcChannelInitFail = -4;
// 错误码： rpc fail
const int kRpcFail = -5;
// 错误码： chunkserverclient请求返回失败
const int kCsClientReturnFail = -5;
// 错误码： chunkserver offline
const int kCsClientCSOffline = -6;



// TODO(xuchaojie): use config file instead.
const uint32_t kRpcTimeoutMs = 1000u;
const uint32_t kRpcRetryTime = 3u;
const uint32_t kRpcRetryIntervalMs = 500u;

const uint32_t kUpdateLeaderRetryTime = 3u;
const uint32_t kUpdateLeaderRetryIntervalMs = 500u;

}  // namespace mds
}  // namespace curve

namespace curve {
namespace mds {
namespace topology {

typedef uint16_t PoolIdType;
typedef uint32_t ZoneIdType;
typedef uint32_t ServerIdType;
typedef uint32_t ChunkServerIdType;
typedef uint32_t UserIdType;
typedef uint32_t CopySetIdType;
typedef uint64_t EpochType;
typedef uint64_t ChunkIdType;

const uint32_t UNINTIALIZE_ID = 0u;

// TODO(xuchaojie): 修改为从配置文件读取的数据
const uint64_t kChunkServerStateUpdateFreq = 600;

// topology Error Code
const int kTopoErrCodeSuccess = 0;
const int kTopoErrCodeInternalError = -1;
const int kTopoErrCodeInvalidParam = -2;
const int kTopoErrCodeInitFail = -3;
const int kTopoErrCodeStorgeFail = -4;
const int kTopoErrCodeIdDuplicated = -5;
const int kTopoErrCodeChunkServerNotFound = -6;
const int kTopoErrCodeServerNotFound = -7;
const int kTopoErrCodeZoneNotFound = -8;
const int kTopoErrCodePhysicalPoolNotFound = -9;
const int kTopoErrCodeLogicalPoolNotFound = -10;
const int kTopoErrCodeCopySetNotFound = -11;
const int kTopoErrCodeGenCopysetErr = -12;
const int kTopoErrCodeAllocateIdFail = -13;

}  // namespace topology
}  // namespace mds
}  // namespace curve


namespace curve {
namespace mds {

typedef uint64_t InodeID;
typedef uint64_t ChunkID;

typedef  uint64_t SeqNum;

const uint64_t kKB = 1024;
const uint64_t kMB = 1024*kKB;
const uint64_t kGB = 1024*kMB;

const uint64_t DefaultChunkSize = 16 * kMB;
const uint64_t DefaultSegmentSize = kGB * 1;
const uint64_t kMiniFileLength = DefaultSegmentSize * 10;

typedef uint64_t offset_t;
typedef uint16_t LogicalPoolID;
typedef uint32_t CopysetID;
typedef uint32_t SegmentSizeType;
typedef uint32_t ChunkSizeType;

typedef uint64_t FileSeqType;
}  // namespace mds
}  // namespace curve


#endif  // CURVE_SRC_MDS_COMMON_MDS_DEFINE_H_

