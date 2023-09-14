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
 * Created Date: Mon Aug 05 2019
 * Author: xuchaojie
 */

#include <json/json.h>

#include "src/common/snapshotclone/snapshotclone_define.h"

namespace curve {
namespace snapshotcloneserver {

// String constant definition
const char* kServiceName = "SnapshotCloneService";
const char* kCreateSnapshotAction = "CreateSnapshot";
const char* kDeleteSnapshotAction = "DeleteSnapshot";
const char* kCancelSnapshotAction = "CancelSnapshot";
const char* kGetFileSnapshotInfoAction = "GetFileSnapshotInfo";
const char* kCloneAction = "Clone";
const char* kRecoverAction = "Recover";
const char* kGetCloneTasksAction = "GetCloneTasks";
const char* kCleanCloneTaskAction = "CleanCloneTask";
const char* kFlattenAction = "Flatten";
const char* kGetFileSnapshotListAction = "GetFileSnapshotList";
const char* kGetCloneTaskListAction = "GetCloneTaskList";
const char* kGetCloneRefStatusAction = "GetCloneRefStatus";

const char* kActionStr = "Action";
const char* kVersionStr = "Version";
const char* kUserStr = "User";
const char* kFileStr = "File";
const char* kNameStr = "Name";
const char* kUUIDStr = "UUID";
const char* kLimitStr = "Limit";
const char* kOffsetStr = "Offset";
const char* kSourceStr = "Source";
const char* kDestinationStr = "Destination";
const char* kLazyStr = "Lazy";
const char* kPoolset = "Poolset";
const char* kStatusStr = "Status";
const char* kTypeStr = "Type";
const char* kInodeStr = "Inode";

const char* kCodeStr = "Code";
const char* kMessageStr = "Message";
const char* kRequestIdStr = "RequestId";
const char* kTotalCountStr = "TotalCount";
const char* kSnapshotsStr = "Snapshots";
const char* kTaskInfosStr = "TaskInfos";
const char* kRefStatusStr = "RefStatus";
const char* kCloneFileInfoStr = "CloneFileInfo";

std::map<int, std::string> code2Msg = {
    {kErrCodeSuccess, "Exec success."},
    {kErrCodeInternalError, "Internal error."},
    {kErrCodeServerInitFail, "Server init fail."},
    {kErrCodeServerStartFail, "Server start fail."},
    {kErrCodeServiceIsStop, "Sevice is stop."},
    {kErrCodeInvalidRequest, "BadRequest:\"Invalid request.\""},
    {kErrCodeTaskExist, "Task already exist."},
    {kErrCodeInvalidUser, "Invalid user."},
    {kErrCodeFileNotExist, "File not exist."},
    {kErrCodeFileStatusInvalid, "File status invalid."},
    {kErrCodeChunkSizeNotAligned, "Chunk size not aligned."},
    {kErrCodeFileNameNotMatch, "FileName not match."},
    {kErrCodeSnapshotCannotDeleteUnfinished, "Cannot delete unfinished."},
    {kErrCodeSnapshotCannotCreateWhenError, "Cannot create when has error."},
    {kErrCodeCannotCancelFinished, "Cannot cancel finished."},
    {kErrCodeInvalidSnapshot, "Invalid snapshot."},
    {kErrCodeSnapshotCannotDeleteCloning, "Cannot delete when using."},
    {kErrCodeCannotCleanCloneUnfinished, "Cannot clean task unfinished."},
    {kErrCodeSnapshotCountReachLimit, "Snapshot count reach the limit."},
    {kErrCodeFileExist, "File exist."},
    {kErrCodeTaskIsFull, "Task is full."},
    {kErrCodeNotSupport, "Not support."},
};

std::string BuildErrorMessage(
    int errCode,
    const std::string &requestId,
    const std::string &uuid) {
    Json::Value mainObj;
    mainObj[kCodeStr] = std::to_string(errCode);
    mainObj[kMessageStr] = code2Msg[errCode];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kUUIDStr] = uuid;
    return mainObj.toStyledString();
}

}  // namespace snapshotcloneserver
}  // namespace curve
