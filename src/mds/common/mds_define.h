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
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 */

#ifndef SRC_MDS_COMMON_MDS_DEFINE_H_
#define SRC_MDS_COMMON_MDS_DEFINE_H_

#include <cstdint>
#include <string>

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


// kStaledRequestTimeIntervalUs表示request的过期时间，防止request被截取并回放
const uint64_t kStaledRequestTimeIntervalUs = 15 * 1000 * 1000u;

}  // namespace mds
}  // namespace curve

namespace curve {
namespace mds {
namespace topology {

typedef uint16_t LogicalPoolIdType;
typedef uint16_t PhysicalPoolIdType;
typedef uint16_t PoolIdType;
typedef uint32_t ZoneIdType;
typedef uint32_t ServerIdType;
typedef uint32_t ChunkServerIdType;
typedef uint32_t UserIdType;
typedef uint32_t CopySetIdType;
typedef uint64_t EpochType;
typedef uint64_t ChunkIdType;

const uint32_t UNINTIALIZE_ID = 0u;

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
const int kTopoErrCodeCannotRemoveWhenNotEmpty = -14;
const int kTopoErrCodeIpPortDuplicated = -15;
const int kTopoErrCodeNameDuplicated = -16;
const int kTopoErrCodeCreateCopysetNodeOnChunkServerFail = -17;
const int kTopoErrCodeCannotRemoveNotRetired = -18;
const int kTopoErrCodeLogicalPoolExist = -19;

}  // namespace topology
}  // namespace mds
}  // namespace curve


namespace curve {
namespace mds {
namespace schedule {


const int kScheduleErrCodeSuccess = 0;
// RapidLeaderSchedule Error Code
const int kScheduleErrCodeInvalidLogicalPool = -1;
// QueryChunkServerRecoverStatus Error Code
const int kScheduleErrInvalidQueryChunkserverID = -2;

}  // namespace schedule
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
const uint64_t kTB = 1024*kGB;

extern uint64_t DefaultSegmentSize;
extern uint64_t kMiniFileLength;
const uint64_t kMaxFileLength = 4 * kTB;

// curve默认root目录&inodeid
const InodeID ROOTINODEID = 0;
const char ROOTFILENAME[] = "/";

// curvefs内部垃圾回收站目录&inodeid
const InodeID RECYCLEBININODEID = 1;
const std::string RECYCLEBINDIRNAME = "RecycleBin"; //NOLINT
const std::string RECYCLEBINDIR = "/" + RECYCLEBINDIRNAME;  //NOLINT
const InodeID USERSTARTINODEID = 2;

// curve root user name

const char ROOTUSERNAME[] = "root";

const SeqNum kStartSeqNum = 1;

const InodeID kUnitializedFileID = 0;

typedef uint64_t offset_t;
typedef uint16_t LogicalPoolID;
typedef uint32_t CopysetID;
typedef uint32_t SegmentSizeType;
typedef uint32_t ChunkSizeType;

typedef uint64_t FileSeqType;

// curve mds curvefs metric prefix
const char CURVE_MDS_CURVEFS_METRIC_PREFIX[] = "curve_mds_curvefs";

const char kLeastSupportSnapshotClientVersion[] = "0.0.5.3";

const uint32_t kInvalidPort = 0;

}  // namespace mds
}  // namespace curve


#endif  // SRC_MDS_COMMON_MDS_DEFINE_H_

