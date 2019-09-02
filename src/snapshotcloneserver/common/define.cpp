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
    {kErrCodeFileExist, "File exist."}
};

std::string BuildErrorMessage(
    int errCode,
    const std::string &requestId) {
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(errCode);
    mainObj["Message"] = code2Msg[errCode];
    mainObj["RequestId"] = requestId;
    return mainObj.toStyledString();
}

}  // namespace snapshotcloneserver
}  // namespace curve


