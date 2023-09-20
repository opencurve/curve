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
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 */

#ifndef SRC_COMMON_SNAPSHOTCLONE_SNAPSHOTCLONE_DEFINE_H_
#define SRC_COMMON_SNAPSHOTCLONE_SNAPSHOTCLONE_DEFINE_H_

#include <string>
#include <map>

namespace curve {
namespace snapshotcloneserver {

// snapshotcloneservice字符串常量定义
extern const char* kServiceName;
// action
extern const char* kCreateFileAction;
extern const char* kDeleteFileAction;
extern const char* kListFileAction;
extern const char* kCreateSnapshotAction;
extern const char* kDeleteSnapshotAction;
extern const char* kCancelSnapshotAction;
extern const char* kGetFileSnapshotInfoAction;
extern const char* kCloneAction;
extern const char* kRecoverAction;
extern const char* kGetCloneTasksAction;
extern const char* kCleanCloneTaskAction;
extern const char* kFlattenAction;
extern const char* kGetFileSnapshotListAction;
extern const char* kGetCloneTaskListAction;
extern const char* kGetCloneRefStatusAction;
// param
extern const char* kActionStr;
extern const char* kVersionStr;
extern const char* kUserStr;
extern const char* kFileStr;
extern const char* kNameStr;
extern const char* kUUIDStr;
extern const char* kLimitStr;
extern const char* kOffsetStr;
extern const char* kSourceStr;
extern const char* kDestinationStr;
extern const char* kLazyStr;
extern const char* kPoolset;
extern const char* kStatusStr;
extern const char* kTypeStr;
extern const char* kInodeStr;
extern const char* kSizeStr;
extern const char* kStripeUnitStr;
extern const char* kStripeCountStr;
extern const char* kPoolSetStr;
extern const char* kDirStr;

// json key
extern const char* kCodeStr;
extern const char* kMessageStr;
extern const char* kRequestIdStr;
extern const char* kTotalCountStr;
extern const char* kSnapshotsStr;
extern const char* kFileInfosStr;
extern const char* kTaskInfosStr;
extern const char* kRefStatusStr;
extern const char* kCloneFileInfoStr;

typedef std::string UUID;
using TaskIdType = UUID;

enum class CloneTaskType {
    kClone = 0,
    kRecover
};

enum class CloneRefStatus {
    kNoRef = 0,
    kHasRef = 1,
    kNeedCheck = 2
};

// 未初始序列号
const uint64_t kUnInitializeSeqNum = 0;
// 初始序列号
const uint64_t kInitializeSeqNum = 1;

// 错误码：执行成功
const int kErrCodeSuccess = 0;
// 错误码: 内部错误
const int kErrCodeInternalError = -1;
// 错误码：服务器初始化失败
const int kErrCodeServerInitFail = -2;
// 错误码：服务器启动失败
const int kErrCodeServerStartFail = -3;
// 错误码：服务已停止
const int kErrCodeServiceIsStop = -4;
// 错误码：非法请求
const int kErrCodeInvalidRequest = -5;
// 错误码：任务已存在
const int kErrCodeTaskExist = -6;
// 错误码：非法的用户
const int kErrCodeInvalidUser = -7;
// 错误码：文件不存在
const int kErrCodeFileNotExist = -8;
// 错误码：文件状态异常
const int kErrCodeFileStatusInvalid = -9;
// 错误码：chunk大小未按chunk分片大小对齐
const int kErrCodeChunkSizeNotAligned = -10;
// 错误码：文件名不匹配
const int kErrCodeFileNameNotMatch = -11;
// 错误码： 不能删除未完成的快照
const int kErrCodeSnapshotCannotDeleteUnfinished = -12;
// 错误码: 不能对存在异常快照的文件打快照，或不能对存在错误的目标文件克隆/恢复
const int kErrCodeSnapshotCannotCreateWhenError = -13;
// 错误码：取消的快照已完成
const int kErrCodeCannotCancelFinished = -14;
// 错误码：不能从未完成或存在错误的快照克隆
const int kErrCodeInvalidSnapshot = -15;
// 错误码：不能删除正在克隆的快照
const int kErrCodeSnapshotCannotDeleteCloning = -16;
// 错误码：不能清理未完成的克隆
const int kErrCodeCannotCleanCloneUnfinished = -17;
// 错误码：快照到达上限
const int kErrCodeSnapshotCountReachLimit = -18;
// 错误码：文件已存在
const int kErrCodeFileExist = -19;
// 错误码：克隆任务已满
const int kErrCodeTaskIsFull = -20;
// 错误码：不支持
const int kErrCodeNotSupport = -21;
// errcode: file has snapshot
const int kErrCodeUnderSnapShot = -22;
// errcode: file occupied
const int kErrCodeFileOccupied = -23;
// errcode: invalid argument
const int kErrCodeInvalidArgument = -24;

extern std::map<int, std::string> code2Msg;

std::string BuildErrorMessage(
    int errCode,
    const std::string &requestId,
    const std::string &uuid = "");


// clone progress
constexpr uint32_t kProgressCloneStart = 0;
constexpr uint32_t kProgressCloneError = kProgressCloneStart;
constexpr uint32_t kProgressCreateCloneFile = 1;
constexpr uint32_t kProgressCreateCloneMeta = 2;
constexpr uint32_t kProgressMetaInstalled = 5;
constexpr uint32_t kProgressRecoverChunkBegin = kProgressMetaInstalled;
constexpr uint32_t kProgressRecoverChunkEnd = 95;
constexpr uint32_t kProgressCloneComplete = 100;



}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_COMMON_SNAPSHOTCLONE_SNAPSHOTCLONE_DEFINE_H_
