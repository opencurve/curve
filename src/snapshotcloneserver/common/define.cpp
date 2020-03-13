/*
 * Project: curve
 * Created Date: Mon Aug 05 2019
 * Author: xuchaojie
 * Copyright (c) 2019 netease
 */

#include <json/json.h>

#include "src/snapshotcloneserver/common/define.h"

namespace curve {
namespace snapshotcloneserver {

// 字符串常量定义
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

const char* kCodeStr = "Code";
const char* kMessageStr = "Message";
const char* kRequestIdStr = "RequestId";
const char* kTotalCountStr = "TotalCount";
const char* kSnapshotsStr = "Snapshots";
const char* kTaskInfosStr = "TaskInfos";

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
    {kErrCodeTaskIsFull, "Task is full."}
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


