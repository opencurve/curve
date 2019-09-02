/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/snapshotclone_service.h"

#include <string>
#include <vector>
#include <limits>

#include "json/json.h"
#include "src/snapshotcloneserver/common/define.h"
#include "src/common/uuid.h"
#include "src/common/string_util.h"
#include "src/snapshotcloneserver/clone/clone_closure.h"

using ::curve::common::UUIDGenerator;

namespace curve {
namespace snapshotcloneserver {

void SnapshotCloneServiceImpl::default_method(RpcController* cntl,
                    const HttpRequest* req,
                    HttpResponse* resp,
                    Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* bcntl =
        static_cast<brpc::Controller*>(cntl);
    const std::string *action = bcntl->http_request().uri().GetQuery("Action");

    std::string requestId = UUIDGenerator().GenerateUUID();
    if (action == nullptr) {
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest, requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    if (*action == "CreateSnapshot") {
        HandleCreateSnapshotAction(bcntl, requestId);
        return;
    }
    if (*action == "DeleteSnapshot") {
        HandleDeleteSnapshotAction(bcntl, requestId);
        return;
    }
    if (*action == "CancelSnapshot") {
        HandleCancelSnapshotAction(bcntl, requestId);
        return;
    }
    if (*action == "GetFileSnapshotInfo") {
        HandleGetFileSnapshotInfoAction(bcntl, requestId);
        return;
    }
    if (*action == "Clone") {
        HandleCloneAction(bcntl, requestId, done);
        done_guard.release();
        return;
    }
    if (*action == "Recover") {
        HandleRecoverAction(bcntl, requestId, done);
        done_guard.release();
        return;
    }
    if (*action == "GetCloneTasks") {
        HandleGetCloneTasksAction(bcntl, requestId);
        return;
    }
    if (*action == "CleanCloneTask") {
        HandleCleanCloneTaskAction(bcntl, requestId);
        return;
    }

    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
    butil::IOBufBuilder os;
    std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
    os << msg;
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCreateSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *file =
        bcntl->http_request().uri().GetQuery("File");
    const std::string *name =
        bcntl->http_request().uri().GetQuery("Name");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (name == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (file->empty()) ||
        (name->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "CreateSnapshot:"
              << " Version = "
              << *version
              << ", User = "
              << *user
              << ", File = "
              << *file
              << ", Name = "
              << *name;
    UUID uuid;
    int ret = snapshotManager_->CreateSnapshot(*file, *user, *name, &uuid);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    mainObj["UUID"] = uuid;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleDeleteSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery("UUID");
    const std::string *file =
        bcntl->http_request().uri().GetQuery("File");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (uuid == nullptr) ||
        (file == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (uuid->empty()) ||
        (file->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "DeleteSnapshot:"
              << " Version = "
              << *version
              << ", User = "
              << *user
              << ", UUID = "
              << *uuid
              << ", File = "
              << *file;
    int ret = snapshotManager_->DeleteSnapshot(*uuid, *user, *file);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCancelSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery("UUID");
    const std::string *file =
        bcntl->http_request().uri().GetQuery("File");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (uuid == nullptr) ||
        (file == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (uuid->empty()) ||
        (file->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "CancelSnapshot:"
              << " Version = "
              << *version
              << ", User = "
              << *user
              << ", UUID = "
              << *uuid
              << ", File = "
              << *file;
    int ret = snapshotManager_->CancelSnapshot(*uuid, *user, *file);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleGetFileSnapshotInfoAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *file =
        bcntl->http_request().uri().GetQuery("File");
    const std::string *limit =
        bcntl->http_request().uri().GetQuery("Limit");
    const std::string *offset =
        bcntl->http_request().uri().GetQuery("Offset");
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery("UUID");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (file->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            butil::IOBufBuilder os;
            std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                requestId);
            os << msg;
            os.move_to(bcntl->response_attachment());
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            butil::IOBufBuilder os;
            std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                requestId);
            os << msg;
            os.move_to(bcntl->response_attachment());
            return;
        }
    }

    std::string uuidStr = "null";
    if (uuid != nullptr) {
        uuidStr = *uuid;
    }
    LOG(INFO) << "GetFileSnapshotInfo:"
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << *file
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr;
    std::vector<FileSnapshotInfo> info;
    int ret = snapshotManager_->GetFileSnapshotInfo(*file, *user, uuid, &info);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    mainObj["TotalCount"] = info.size();
    Json::Value listSnapObj;
    for (std::vector<FileSnapshotInfo>::size_type i = offsetNum;
        i < info.size() && i < limitNum;
        i++) {
        Json::Value fileSnapObj;
        const SnapshotInfo &snap = info[i].GetSnapshotInfo();
        fileSnapObj["UUID"] = snap.GetUuid();
        fileSnapObj["User"] = snap.GetUser();
        fileSnapObj["File"] = snap.GetFileName();
        fileSnapObj["SeqNum"] = snap.GetSeqNum();
        fileSnapObj["Name"] = snap.GetSnapshotName();
        fileSnapObj["Time"] = snap.GetCreateTime();
        fileSnapObj["FileLength"] = snap.GetFileLength();
        fileSnapObj["Status"] = static_cast<int>(snap.GetStatus());
        fileSnapObj["Progress"] = info[i].GetSnapProgress();
        listSnapObj.append(fileSnapObj);
    }
    mainObj["Snapshots"] = listSnapObj;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCloneAction(
    brpc::Controller* bcntl,
    const std::string &requestId,
    Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *source =
        bcntl->http_request().uri().GetQuery("Source");
    const std::string *destination =
        bcntl->http_request().uri().GetQuery("Destination");
    const std::string *lazy =
        bcntl->http_request().uri().GetQuery("Lazy");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (source == nullptr) ||
        (destination == nullptr) ||
        (lazy == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (source->empty()) ||
        (destination->empty()) ||
        (lazy->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }

    bool lazyFlag = false;
    if (!CheckBoolParamter(lazy, &lazyFlag)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "Clone:"
              << " Version = " << *version
              << " User = " << *user
              << " Source = " << *source
              << " Destination = " << *destination
              << " Lazy = " << *lazy;


    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>(bcntl, done);
    cloneManager_->CloneFile(
    *source, *user, *destination, lazyFlag, closure, &taskId);
    done_guard.release();
    return;
}

void SnapshotCloneServiceImpl::HandleRecoverAction(
    brpc::Controller* bcntl,
    const std::string &requestId,
    Closure* done) {
    brpc::ClosureGuard done_guard(done);
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *source =
        bcntl->http_request().uri().GetQuery("Source");
    const std::string *destination =
        bcntl->http_request().uri().GetQuery("Destination");
    const std::string *lazy =
        bcntl->http_request().uri().GetQuery("Lazy");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (source == nullptr) ||
        (destination == nullptr) ||
        (lazy == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (source->empty()) ||
        (destination->empty()) ||
        (lazy->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }

    bool lazyFlag = false;
    if (!CheckBoolParamter(lazy, &lazyFlag)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "Recover:"
              << " Version = " << *version
              << " User = " << *user
              << " Source = " << *source
              << " Destination = " << *destination
              << " Lazy = " << *lazy;

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>(bcntl, done);
    cloneManager_->RecoverFile(
    *source, *user, *destination, lazyFlag, closure, &taskId);
    done_guard.release();
    return;
}

void SnapshotCloneServiceImpl::HandleGetCloneTasksAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *limit =
        bcntl->http_request().uri().GetQuery("Limit");
    const std::string *offset =
        bcntl->http_request().uri().GetQuery("Offset");
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery("UUID");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (version->empty()) ||
        (user->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            butil::IOBufBuilder os;
            std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                requestId);
            os << msg;
            os.move_to(bcntl->response_attachment());
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            butil::IOBufBuilder os;
            std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                requestId);
            os << msg;
            os.move_to(bcntl->response_attachment());
            return;
        }
    }

    std::string uuidStr = "null";
    if (uuid != nullptr) {
        uuidStr = *uuid;
    }
    LOG(INFO) << "GetTasks:"
              << " Version = " << *version
              << ", User = " << *user
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr;

    std::vector<TaskCloneInfo> cloneTaskInfos;
    int ret = cloneManager_->GetCloneTaskInfo(*user, uuid, &cloneTaskInfos);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    mainObj["TotalCount"] = cloneTaskInfos.size();
    Json::Value listObj;
    for (std::vector<TaskCloneInfo>::size_type i = offsetNum;
        i < cloneTaskInfos.size() && i < limitNum;
        i++) {
        Json::Value cloneTaskObj;
        const TaskCloneInfo &info = cloneTaskInfos[i];
        cloneTaskObj["UUID"] = info.GetCloneInfo().GetTaskId();
        cloneTaskObj["User"] = info.GetCloneInfo().GetUser();
        cloneTaskObj["File"] = info.GetCloneInfo().GetDest();
        cloneTaskObj["TaskType"] = static_cast<int> (
            info.GetCloneInfo().GetTaskType());
        cloneTaskObj["TaskStatus"] = static_cast<int> (
            info.GetCloneInfo().GetStatus());
        cloneTaskObj["Time"] = info.GetCloneInfo().GetTime();
        listObj.append(cloneTaskObj);
    }
    mainObj["TaskInfos"] = listObj;

    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

bool SnapshotCloneServiceImpl::CheckBoolParamter(
    const std::string *param, bool *valueOut) {
    if (*param == "true" || *param == "TRUE" || *param == "1") {
        *valueOut = true;
    } else if (*param == "false" || *param == "FALSE" || *param == "0") {
        *valueOut = false;
    } else {
        return false;
    }
    return true;
}

void SnapshotCloneServiceImpl::HandleCleanCloneTaskAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *taskId =
        bcntl->http_request().uri().GetQuery("UUID");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (taskId == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (taskId->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }

    LOG(INFO) << "CleanCloneTask:"
              << " Version = " << *version
              << " User = " << *user
              << " UUID = " << *taskId;


    int ret = cloneManager_->CleanCloneTask(*user, *taskId);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["Code"] = std::to_string(kErrCodeSuccess);
    mainObj["Message"] = code2Msg[kErrCodeSuccess];
    mainObj["RequestId"] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}


}  // namespace snapshotcloneserver
}  // namespace curve


