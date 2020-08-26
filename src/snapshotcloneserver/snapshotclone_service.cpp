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
    const std::string *action =
        bcntl->http_request().uri().GetQuery(kActionStr);

    std::string requestId = UUIDGenerator().GenerateUUID();
    if (action == nullptr) {
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest, requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = null"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    if (*action == kCreateSnapshotAction) {
        HandleCreateSnapshotAction(bcntl, requestId);
    } else if (*action == kDeleteSnapshotAction) {
        HandleDeleteSnapshotAction(bcntl, requestId);
    } else if (*action == kCancelSnapshotAction) {
        HandleCancelSnapshotAction(bcntl, requestId);
    } else if (*action == kGetFileSnapshotInfoAction) {
        HandleGetFileSnapshotInfoAction(bcntl, requestId);
    } else if (*action == kCloneAction) {
        HandleCloneAction(bcntl, requestId, done);
        done_guard.release();
        return;
    } else if (*action == kRecoverAction) {
        HandleRecoverAction(bcntl, requestId, done);
        done_guard.release();
        return;
    } else if (*action == kGetCloneTasksAction) {
        HandleGetCloneTasksAction(bcntl, requestId);
    } else if (*action == kCleanCloneTaskAction) {
        HandleCleanCloneTaskAction(bcntl, requestId);
    } else if (*action == kFlattenAction) {
        HandleFlattenAction(bcntl, requestId);
    } else {
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
                requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
    }

    LOG(INFO) << "SnapshotCloneServiceImpl Return : "
              << "action = " << *action
              << ", requestId = " << requestId
              << ", context = " << bcntl->response_attachment();
    return;
}

void SnapshotCloneServiceImpl::HandleCreateSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *name =
        bcntl->http_request().uri().GetQuery(kNameStr);
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
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << *file
              << ", Name = " << *name
              << ", requestId = " << requestId;
    UUID uuid;
    int ret = snapshotManager_->CreateSnapshot(*file, *user, *name, &uuid);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(ret,
            requestId,
            uuid);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kUUIDStr] = uuid;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleDeleteSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (uuid == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (uuid->empty())) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_BAD_REQUEST);
        butil::IOBufBuilder os;
        std::string msg = BuildErrorMessage(kErrCodeInvalidRequest,
            requestId);
        os << msg;
        os.move_to(bcntl->response_attachment());
        return;
    }
    std::string fileStr = "null";
    std::string fileName = "";
    if (file != nullptr) {
        fileStr = *file;
        fileName = *file;
    }
    LOG(INFO) << "DeleteSnapshot:"
              << " Version = " << *version
              << ", User = " << *user
              << ", UUID = " << *uuid
              << ", File = " << fileStr
              << ", requestId = " << requestId;
    int ret = snapshotManager_->DeleteSnapshot(*uuid, *user, fileName);
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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleCancelSnapshotAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
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
              << " Version = " << *version
              << ", User = " << *user
              << ", UUID = " << *uuid
              << ", File = " << *file
              << ", requestId = " << requestId;
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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleGetFileSnapshotInfoAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *limit =
        bcntl->http_request().uri().GetQuery(kLimitStr);
    const std::string *offset =
        bcntl->http_request().uri().GetQuery(kOffsetStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
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
    std::string fileStr = "null";
    std::string fileName = "";
    if (file != nullptr) {
        fileStr = *file;
        fileName = *file;
    }
    LOG(INFO) << "GetFileSnapshotInfo:"
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << fileStr
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr
              << ", requestId = " << requestId;

    std::vector<FileSnapshotInfo> info;
    int ret = kErrCodeSuccess;
    if (uuid != nullptr) {
        ret = snapshotManager_->GetFileSnapshotInfoById(
        fileName, *user, *uuid, &info);
    } else {
        ret = snapshotManager_->GetFileSnapshotInfo(
            fileName, *user, &info);
    }
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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kTotalCountStr] = info.size();
    Json::Value listSnapObj;
    for (std::vector<FileSnapshotInfo>::size_type i = offsetNum;
        i < info.size() && i < limitNum;
        i++) {
        Json::Value fileSnapObj = info[i].ToJsonObj();
        listSnapObj.append(fileSnapObj);
    }
    mainObj[kSnapshotsStr] = listSnapObj;
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
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *source =
        bcntl->http_request().uri().GetQuery(kSourceStr);
    const std::string *destination =
        bcntl->http_request().uri().GetQuery(kDestinationStr);
    const std::string *lazy =
        bcntl->http_request().uri().GetQuery(kLazyStr);
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
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Clone"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
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
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Clone"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    LOG(INFO) << "Clone:"
              << " Version = " << *version
              << ", User = " << *user
              << ", Source = " << *source
              << ", Destination = " << *destination
              << ", Lazy = " << *lazy
              << ", requestId = " << requestId;


    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>(bcntl, done);
    closure->SetRequestId(requestId);
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
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *source =
        bcntl->http_request().uri().GetQuery(kSourceStr);
    const std::string *destination =
        bcntl->http_request().uri().GetQuery(kDestinationStr);
    const std::string *lazy =
        bcntl->http_request().uri().GetQuery(kLazyStr);
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
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Recover"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
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
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Recover"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    LOG(INFO) << "Recover:"
              << " Version = " << *version
              << ", User = " << *user
              << ", Source = " << *source
              << ", Destination = " << *destination
              << ", Lazy = " << *lazy
              << ", requestId = " << requestId;

    TaskIdType taskId;
    auto closure = std::make_shared<CloneClosure>(bcntl, done);
    closure->SetRequestId(requestId);
    cloneManager_->RecoverFile(
    *source, *user, *destination, lazyFlag, closure, &taskId);
    done_guard.release();
    return;
}

void SnapshotCloneServiceImpl::HandleFlattenAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *taskId =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
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
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Flatten"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    LOG(INFO) << "Flatten:"
              << " Version = " << *version
              << ", User = " << *user
              << ", UUID = " << *taskId
              << ", requestId = " << requestId;
    int ret = cloneManager_->Flatten(*user, *taskId);
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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleGetCloneTasksAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *limit =
        bcntl->http_request().uri().GetQuery(kLimitStr);
    const std::string *offset =
        bcntl->http_request().uri().GetQuery(kOffsetStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
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

    std::string fileStr = "null";
    if (file != nullptr) {
        fileStr = *file;
    }

    LOG(INFO) << "GetTasks:"
              << " Version = " << *version
              << ", User = " << *user
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr
              << ", File = " << fileStr
              << ", requestId = " << requestId;

    std::vector<TaskCloneInfo> cloneTaskInfos;
    int ret = kErrCodeSuccess;
    if (uuid != nullptr) {
        ret = cloneManager_->GetCloneTaskInfoById(
            *user, *uuid, &cloneTaskInfos);
    } else if (file != nullptr) {
        ret = cloneManager_->GetCloneTaskInfoByName(
            *user, *file, &cloneTaskInfos);
    } else {
        ret = cloneManager_->GetCloneTaskInfo(
            *user, &cloneTaskInfos);
    }
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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kTotalCountStr] = cloneTaskInfos.size();
    Json::Value listObj;
    for (std::vector<TaskCloneInfo>::size_type i = offsetNum;
        i < cloneTaskInfos.size() && i < limitNum;
        i++) {
        Json::Value cloneTaskObj = cloneTaskInfos[i].ToJsonObj();
        listObj.append(cloneTaskObj);
    }
    mainObj[kTaskInfosStr] = listObj;

    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

bool SnapshotCloneServiceImpl::CheckBoolParamter(
    const std::string *param, bool *valueOut) {
    if (*param == "true" ||
        *param == "True" ||
        *param == "TRUE" ||
        *param == "1") {
        *valueOut = true;
    } else if (*param == "false" ||
               *param == "False" ||
               *param == "FALSE" ||
               *param == "0") {
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
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *taskId =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
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
              << ", Version = " << *version
              << ", User = " << *user
              << ", UUID = " << *taskId
              << ", requestId = " << requestId;


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
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}


}  // namespace snapshotcloneserver
}  // namespace curve


