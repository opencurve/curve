/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshotcloneserver/snapshotclone_service.h"

#include <string>
#include <vector>

#include "json/json.h"
#include "src/snapshotcloneserver/common/define.h"

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

    if (action == nullptr) {
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                std::string("BadRequest:\"Action is NULL\""),
                0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    if (*action == "CreateSnapshot") {
        HandleCreateSnapshotAction(bcntl);
        return;
    }
    if (*action == "DeleteSnapshot") {
        HandleDeleteSnapshotAction(bcntl);
        return;
    }
    if (*action == "CancelSnapshot") {
        HandleCancelSnapshotAction(bcntl);
        return;
    }
    if (*action == "GetFileSnapshotInfo") {
        HandleGetFileSnapshotInfoAction(bcntl);
        return;
    }
    if (*action == "Clone") {
        HandleCloneAction(bcntl);
        return;
    }
    if (*action == "Recover") {
        HandleRecoverAction(bcntl);
        return;
    }
    if (*action == "GetCloneTasks") {
        HandleGetCloneTasksAction(bcntl);
        return;
    }
    // TODO(xuchaojie): 需提供运维接口，清理失败的clone任务。

    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
    butil::IOBufBuilder os;
    std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"Invalid Action \"") + *action,
            0);
    os << msg;
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCreateSnapshotAction(
    brpc::Controller* bcntl) {
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
        (name == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
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
    // todo(xuchaojie): handle version and user info.
    UUID uuid;
    int ret = snapshotManager_->CreateSnapshot(*file, *user, *name, &uuid);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "CreateSnapshot internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    // TODO(xuchaojie) 返回数据暂定
    os << "UUID:"
       << uuid;
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleDeleteSnapshotAction(
    brpc::Controller* bcntl) {
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
        (file == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
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
    // todo(xuchaojie): handle version info.
    int ret = snapshotManager_->DeleteSnapshot(*uuid, *user, *file);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "DeleteSnapshot internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    return;
}

void SnapshotCloneServiceImpl::HandleCancelSnapshotAction(
    brpc::Controller* bcntl) {
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
        (file == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
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
    // todo(xuchaojie): handle version info.
    int ret = snapshotManager_->CancelSnapshot(*uuid, *user, *file);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "CancelSnapshot internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    return;
}

void SnapshotCloneServiceImpl::HandleGetFileSnapshotInfoAction(
    brpc::Controller* bcntl) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    const std::string *file =
        bcntl->http_request().uri().GetQuery("File");
    const std::string *limit =
        bcntl->http_request().uri().GetQuery("Limit");
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (limit == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "GetFileSnapshotInfo:"
              << " Version = "
              << *version
              << ", User = "
              << *user
              << ", File = "
              << *file
              << ", limit = "
              << *limit;
    // todo(xuchaojie): handle version and limit  info.
    std::vector<FileSnapshotInfo> info;
    int ret = snapshotManager_->GetFileSnapshotInfo(*file, *user, &info);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "GetFileSnapshotInfo internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value jsonObj;
    for (std::vector<FileSnapshotInfo>::size_type i = 0u;
        i < info.size();
        i++) {
        Json::Value fileSnapObj;
        SnapshotInfo snap = info[i].GetSnapshotInfo();
        fileSnapObj["UUID"] = snap.GetUuid();
        fileSnapObj["User"] = snap.GetUser();
        fileSnapObj["File"] = snap.GetFileName();
        fileSnapObj["SeqNum"] = snap.GetSeqNum();
        fileSnapObj["Name"] = snap.GetSnapshotName();
        fileSnapObj["Time"] = snap.GetCreateTime();
        fileSnapObj["FileLength"] = snap.GetFileLength();
        fileSnapObj["Status"] = static_cast<int>(snap.GetStatus());
        fileSnapObj["Progress"] = info[i].GetSnapProgress();
        jsonObj.append(fileSnapObj);
    }
    os << jsonObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCloneAction(
    brpc::Controller* bcntl) {
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
        (lazy == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
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
            std::string("BadRequest:\"invalid parameter.\"."),
            0);
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


    int ret = cloneManager_->CloneFile(*source, *user, *destination, lazyFlag);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "Clone Action internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    return;
}

void SnapshotCloneServiceImpl::HandleRecoverAction(brpc::Controller* bcntl) {
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
        (lazy == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
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
            std::string("BadRequest:\"invalid parameter.\"."),
            0);
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

    int ret = cloneManager_->RecoverFile(
        *source, *user, *destination, lazyFlag);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "Recover Action internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    return;
}

void SnapshotCloneServiceImpl::HandleGetCloneTasksAction(
    brpc::Controller* bcntl) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery("Version");
    const std::string *user =
        bcntl->http_request().uri().GetQuery("User");
    if ((version == nullptr) ||
        (user == nullptr)) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            std::string("BadRequest:\"missing parameter\"."),
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    LOG(INFO) << "GetTasks:"
              << " Version = " << *version
              << " User = " << *user;

    std::vector<TaskCloneInfo> cloneTaskInfos;
    int ret = cloneManager_->GetCloneTaskInfo(*user, &cloneTaskInfos);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            "GetCloneTask internal error",
            0);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj["RequestId"] = bcntl->log_id();
    mainObj["TotalCount"] = cloneTaskInfos.size();
    Json::Value listObj;
    for (auto &info :  cloneTaskInfos) {
        Json::Value cloneTaskObj;
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

std::string SnapshotCloneServiceImpl::BuildErrorMessage(
    int errCode,
    const std::string &errMsg,
    uint64_t requestId) {
    Json::Value mainObj;
    Json::Value errObj;
    errObj["Code"] = std::to_string(errCode);
    errObj["Message"] = errMsg;
    errObj["RequestId"] = std::to_string(requestId);
    mainObj["Error"] = errObj;
    return mainObj.toStyledString();
}

}  // namespace snapshotcloneserver
}  // namespace curve


