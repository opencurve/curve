/*
 * Project: curve
 * Created Date: Fri Aug 17 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_SRC_MDS_COMMON_TOPOLOGY_DEFINE_H_
#define CURVE_SRC_MDS_COMMON_TOPOLOGY_DEFINE_H_


namespace curve {
namespace mds {
namespace topology {

typedef uint16_t PoolIdType;
typedef uint32_t ZoneIdType;
typedef uint32_t ServerIdType;
typedef uint32_t ChunkServerIdType;
typedef uint32_t UserIdType;
typedef uint32_t CopySetIdType;

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

#endif  // CURVE_SRC_MDS_COMMON_TOPOLOGY_DEFINE_H_

