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

#ifndef SRC_COMMON_DEFINE_H_
#define SRC_COMMON_DEFINE_H_

#include <string>
#include <map>

namespace curve {
namespace snapshotcloneserver {

// snapshotcloneservice string constant definition
extern const char* kServiceName;
// action
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
extern const char* kStatusStr;
extern const char* kTypeStr;

// json key
extern const char* kCodeStr;
extern const char* kMessageStr;
extern const char* kRequestIdStr;
extern const char* kTotalCountStr;
extern const char* kSnapshotsStr;
extern const char* kTaskInfosStr;


typedef std::string UUID;
using TaskIdType = UUID;

enum class CloneTaskType {
    kClone = 0,
    kRecover
};

// Uninitialized serial number
const uint64_t kUnInitializeSeqNum = 0;
// Initial serial number
const uint64_t kInitializeSeqNum = 1;

// Error code: Execution successful
const int kErrCodeSuccess = 0;
// Error code: Internal error
const int kErrCodeInternalError = -1;
// Error code: Server initialization failed
const int kErrCodeServerInitFail = -2;
// Error code: Server startup failed
const int kErrCodeServerStartFail = -3;
// Error code: Service stopped
const int kErrCodeServiceIsStop = -4;
// Error code: Illegal request
const int kErrCodeInvalidRequest = -5;
// Error code: Task already exists
const int kErrCodeTaskExist = -6;
// Error code: Illegal user
const int kErrCodeInvalidUser = -7;
// Error code: File does not exist
const int kErrCodeFileNotExist = -8;
// Error code: File status abnormal
const int kErrCodeFileStatusInvalid = -9;
// Error code: Chunk size not aligned with chunk partition size
const int kErrCodeChunkSizeNotAligned = -10;
// Error code: File name mismatch
const int kErrCodeFileNameNotMatch = -11;
// Error code: Unable to delete incomplete snapshot
const int kErrCodeSnapshotCannotDeleteUnfinished = -12;
// Error code: Cannot take a snapshot of files with abnormal snapshots, or cannot clone/recover target files with errors
const int kErrCodeSnapshotCannotCreateWhenError = -13;
// Error code: Canceled snapshot completed
const int kErrCodeCannotCancelFinished = -14;
// Error code: Cannot clone a snapshot that has never been completed or has errors
const int kErrCodeInvalidSnapshot = -15;
// Error code: Unable to delete snapshot being cloned
const int kErrCodeSnapshotCannotDeleteCloning = -16;
// Error code: Unable to clean up incomplete clones
const int kErrCodeCannotCleanCloneUnfinished = -17;
// Error code: The snapshot has reached the upper limit
const int kErrCodeSnapshotCountReachLimit = -18;
// Error code: File already exists
const int kErrCodeFileExist = -19;
// Error code: Clone task is full
const int kErrCodeTaskIsFull = -20;
// Error code: not supported
const int kErrCodeNotSupport = -21;

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

#endif  // SRC_COMMON_DEFINE_H_
