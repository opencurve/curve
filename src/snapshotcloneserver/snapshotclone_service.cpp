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
#include "src/common/snapshotclone/snapshotclone_define.h"
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
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = null"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    if (*action == kCreateFileAction) {
        HandleCreateFileAction(bcntl, requestId);
    } else if (*action == kDeleteFileAction) {
        HandleDeleteFileAction(bcntl, requestId);
    } else if (*action == kListFileAction) {
        HandleListFileAction(bcntl, requestId);
    } else if (*action == kCreateSnapshotAction) {
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
    } else if (*action == kGetFileSnapshotListAction) {
        HandleGetFileSnapshotListAction(bcntl, requestId);
    } else if (*action == kGetCloneTaskListAction) {
        HandleGetCloneTaskListAction(bcntl, requestId);
    } else if (*action == kGetCloneRefStatusAction) {
        HandleGetCloneRefStatusAction(bcntl, requestId);
    } else {
        HandleBadRequestError(bcntl, requestId);
    }

    LOG(INFO) << "SnapshotCloneServiceImpl Return : "
            << "action = " << *action
            << ", requestId = " << requestId
            << ", context = " << bcntl->response_attachment();
    return;
}

void SnapshotCloneServiceImpl::HandleCreateFileAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *sizeStr =
        bcntl->http_request().uri().GetQuery(kSizeStr);
    const std::string *stripeUnitStr =
        bcntl->http_request().uri().GetQuery(kStripeUnitStr);
    const std::string *stripCountStr =
        bcntl->http_request().uri().GetQuery(kStripeCountStr);
    const std::string *poolSetStr =
        bcntl->http_request().uri().GetQuery(kPoolSetStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (sizeStr == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (file->empty()) ||
        (sizeStr->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    uint64_t stripeUnit = 0;
    if (stripeUnitStr != nullptr && !stripeUnitStr->empty()) {
        if (!curve::common::StringToUll(*stripeUnitStr, &stripeUnit)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }

    uint64_t stripeCount = 0;
    if (stripCountStr != nullptr && !stripCountStr->empty()) {
        if (!curve::common::StringToUll(*stripCountStr, &stripeCount)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }

    std::string poolset = "";
    if (poolSetStr != nullptr) {
        poolset = *poolSetStr;
    }

    uint64_t size;
    if (!curve::common::StringToUll(*sizeStr, &size)) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    LOG(INFO) << "CreateFile: "
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << *file
              << ", Size = " << *sizeStr
              << ", StripeUnit = " << stripeUnit
              << ", StripeCount = " << stripeCount
              << ", PoolSet = " << poolset
              << ", requestId = " << requestId;

    int ret = volumeManager_->CreateFile(*file, *user, size,
        stripeUnit, stripeCount, poolset);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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

void SnapshotCloneServiceImpl::HandleDeleteFileAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (file->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    LOG(INFO) << "DeleteFile: "
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << *file
              << ", requestId = " << requestId;

    int ret = volumeManager_->DeleteFile(*file, *user);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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

void SnapshotCloneServiceImpl::HandleListFileAction(
    brpc::Controller* bcntl,
    const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *dir =
        bcntl->http_request().uri().GetQuery(kDirStr);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *limit =
        bcntl->http_request().uri().GetQuery(kLimitStr);
    const std::string *offset =
        bcntl->http_request().uri().GetQuery(kOffsetStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (version->empty()) ||
        (user->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }

    LOG(INFO) << "ListFile: "
              << " Version = " << *version
              << ", User = " << *user
              << ", Dir = " << (dir == nullptr ? "" : *dir)
              << ", File = " << (file == nullptr ? "" : *file)
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", requestId = " << requestId;

    std::vector<FileInfo> files;
    int ret = kErrCodeSuccess;
    if (file != nullptr) {
        FileInfo fileInfo;
        ret = volumeManager_->GetFile(*file, *user, &fileInfo);
        files.push_back(fileInfo);
    } else if (dir != nullptr) {
        ret = volumeManager_->ListFile(*dir, *user, &files);
    } else {
        LOG(ERROR) << "Dir or File query string must be specified";
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
        return;
    }
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kTotalCountStr] = files.size();
    Json::Value listFileObj;
    for (std::vector<FileInfo>::size_type i = offsetNum;
        i < files.size() && i < offsetNum + limitNum; i++) {
        Json::Value fileObj = files[i].ToJsonObj();
        listFileObj.append(fileObj);
    }
    mainObj[kFileInfosStr] = listFileObj;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
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
        HandleBadRequestError(bcntl, requestId);
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
        SetErrorMessage(bcntl, ret, requestId);
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
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
    const std::string *name =
        bcntl->http_request().uri().GetQuery(kNameStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (file == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (file->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    int ret = kErrCodeSuccess;
    if ((name != nullptr) && (!name->empty())) {
        LOG(INFO) << "DeleteSnapshot:"
                  << " Version = " << *version
                  << ", User = " << *user
                  << ", File = " << *file
                  << ", name = " << *name
                  << ", requestId = " << requestId;
        ret = snapshotManager_->DeleteSnapshotBySnapshotName(*name,
             *user, *file);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            SetErrorMessage(bcntl, ret, requestId);
            return;
        }
    } else if ((uuid != nullptr) && (!uuid->empty())) {
        LOG(INFO) << "DeleteSnapshot:"
                  << " Version = " << *version
                  << ", User = " << *user
                  << ", File = " << *file
                  << ", UUID = " << *uuid
                  << ", requestId = " << requestId;
        ret = snapshotManager_->DeleteSnapshot(*uuid, *user, *file);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            SetErrorMessage(bcntl, ret, requestId);
            return;
        }
    } else {
        HandleBadRequestError(bcntl, requestId);
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
        HandleBadRequestError(bcntl, requestId);
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
        SetErrorMessage(bcntl, ret, requestId);
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
    const std::string *name =
        bcntl->http_request().uri().GetQuery(kNameStr);
    const std::string *uuid =
        bcntl->http_request().uri().GetQuery(kUUIDStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (version->empty()) ||
        (user->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }

    LOG(INFO) << "GetFileSnapshotInfo:"
              << " Version = " << *version
              << ", User = " << *user
              << ", File = " << (file == nullptr ? "" : *file)
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", Name = " << (name == nullptr ? "" : *name)
              << ", UUID = " << (uuid == nullptr ? "" : *uuid)
              << ", requestId = " << requestId;

    std::vector<FileSnapshotInfo> info;
    int ret = kErrCodeSuccess;
    if (uuid != nullptr) {
        ret = snapshotManager_->GetFileSnapshotInfoById(
        (file == nullptr ? "" : *file),
        *user, *uuid, &info);
    } else if (file != nullptr && !file->empty()) {
        if (name != nullptr) {
            ret = snapshotManager_->GetFileSnapshotInfoBySnapshotName(
                *file, *user, *name, &info);
        } else {
            ret = snapshotManager_->GetFileSnapshotInfo(
                *file, *user, &info);
        }
    } else {
        LOG(ERROR) << "File query string must be specified";
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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
        i < info.size() && i < limitNum + offsetNum;
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
    const std::string *poolset =
        bcntl->http_request().uri().GetQuery(kPoolset);
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    const std::string *name =
        bcntl->http_request().uri().GetQuery(kNameStr);
    const std::string *readonly =
        bcntl->http_request().uri().GetQuery(kReadonlyStr);

    if ((version == nullptr) ||
        (user == nullptr) ||
        (destination == nullptr) ||
        (version->empty()) ||
        (user->empty()) ||
        (destination->empty()) ||
        // poolset is optional, but if it exists, it should not be empty
        (poolset != nullptr && poolset->empty())) {
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Clone"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }

    if ((file != nullptr) &&
        (!file->empty()) &&
        (name != nullptr) &&
        (!name->empty())) {
        bool readonlyFlag = false;
        if ((readonly != nullptr) &&
            (!readonly->empty()) &&
            (!CheckBoolParamter(readonly, &readonlyFlag))) {
            HandleBadRequestError(bcntl, requestId);
            LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                      << "action = Clone"
                      << ", requestId = " << requestId
                      << ", context = " << bcntl->response_attachment();
            return;
        }

        LOG(INFO) << "Clone:"
                  << " Version = " << *version
                  << ", User = " << *user
                  << ", File = " << *file
                  << ", Name = " << *name
                  << ", Destination = " << *destination
                  << ", Readonly = " << readonlyFlag
                  << ", Poolset = " << (poolset != nullptr ? *poolset : "")
                  << ", requestId = " << requestId;

        int ret = cloneManager_->CloneLocal(*file,
            *name,
            *user,
            *destination,
            (poolset != nullptr ? *poolset : ""),
            readonlyFlag);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            SetErrorMessage(bcntl, ret, requestId);
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
    } else if ((source != nullptr) &&  // old stytle clone
        (!source->empty()) &&
        (lazy != nullptr) &&
        (!lazy->empty())) {
        bool lazyFlag = false;
        if (!CheckBoolParamter(lazy, &lazyFlag)) {
            HandleBadRequestError(bcntl, requestId);
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
                  << ", Poolset = " << (poolset != nullptr ? *poolset : "")
                  << ", requestId = " << requestId;
        TaskIdType taskId;
        auto closure = std::make_shared<CloneClosure>(bcntl, done);
        closure->SetRequestId(requestId);
        cloneManager_->CloneFile(*source, *user, *destination,
                                 (poolset != nullptr ? *poolset : ""), lazyFlag,
                                 closure, &taskId);
        done_guard.release();
    } else {
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Clone"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
    }
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
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Recover"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }

    bool lazyFlag = false;
    if (!CheckBoolParamter(lazy, &lazyFlag)) {
        HandleBadRequestError(bcntl, requestId);
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
    const std::string *file =
        bcntl->http_request().uri().GetQuery(kFileStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (version->empty()) ||
        (user->empty())) {
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Flatten"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    int ret = kErrCodeSuccess;
    if ((file != nullptr) &&
        (!file->empty())) {
        LOG(INFO) << "Flatten:"
                  << " Version = " << *version
                  << ", User = " << *user
                  << ", File = " << *file
                  << ", requestId = " << requestId;
        ret = cloneManager_->FlattenLocal(*file, *user);
    } else if ((taskId != nullptr) &&  // old stytle clone
               (!taskId->empty())) {
        LOG(INFO) << "Flatten:"
                  << " Version = " << *version
                  << ", User = " << *user
                  << ", UUID = " << *taskId
                  << ", requestId = " << requestId;
        ret = cloneManager_->Flatten(*user, *taskId);
    } else {
        HandleBadRequestError(bcntl, requestId);
        LOG(INFO) << "SnapshotCloneServiceImpl Return : "
                  << "action = Flatten"
                  << ", requestId = " << requestId
                  << ", context = " << bcntl->response_attachment();
        return;
    }
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            HandleBadRequestError(bcntl, requestId);
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
        SetErrorMessage(bcntl, ret, requestId);
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
        i < cloneTaskInfos.size() && i < limitNum + offsetNum;
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
        HandleBadRequestError(bcntl, requestId);
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
        SetErrorMessage(bcntl, ret, requestId);
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

void SnapshotCloneServiceImpl::HandleGetFileSnapshotListAction(
    brpc::Controller* bcntl, const std::string &requestId) {
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
    const std::string *status =
        bcntl->http_request().uri().GetQuery(kStatusStr);
    if ((version == nullptr) ||
        (version->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            HandleBadRequestError(bcntl, requestId);
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
    std::string userStr = "null";
    if (user != nullptr) {
        userStr = *user;
    }
    std::string statusStr = "null";
    if (status != nullptr) {
        statusStr = *status;
    }

    LOG(INFO) << "GetFileSnapshotInfo:"
              << " Version = " << *version
              << ", User = " << userStr
              << ", File = " << fileStr
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr
              << ", Status = " << statusStr
              << ", requestId = " << requestId;

    std::vector<FileSnapshotInfo> info;
    int ret = kErrCodeSuccess;

    SnapshotFilterCondition filter(uuid, file, user, status);
    ret = snapshotManager_->GetSnapshotListByFilter(filter, &info);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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
        i < info.size() && i < limitNum + offsetNum;
        i++) {
        Json::Value fileSnapObj = info[i].ToJsonObj();
        listSnapObj.append(fileSnapObj);
    }
    mainObj[kSnapshotsStr] = listSnapObj;
    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleGetCloneTaskListAction(
    brpc::Controller* bcntl, const std::string &requestId) {
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
    const std::string *source =
        bcntl->http_request().uri().GetQuery(kSourceStr);
    const std::string *destination =
        bcntl->http_request().uri().GetQuery(kDestinationStr);
    const std::string *status =
        bcntl->http_request().uri().GetQuery(kStatusStr);
    const std::string *type =
        bcntl->http_request().uri().GetQuery(kTypeStr);
    if ((version == nullptr) ||
        (version->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }
    // 默认值为10
    uint64_t limitNum = 10;
    if ((limit != nullptr) && !limit->empty()) {
        if (!curve::common::StringToUll(*limit, &limitNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }
    // 默认值为0
    uint64_t offsetNum = 0;
    if ((offset != nullptr) && !offset->empty()) {
        if (!curve::common::StringToUll(*offset, &offsetNum)) {
            HandleBadRequestError(bcntl, requestId);
            return;
        }
    }

    std::string uuidStr = "null";
    if (uuid != nullptr) {
        uuidStr = *uuid;
    }

    std::string destinationStr = "null";
    if (destination != nullptr) {
        destinationStr = *destination;
    }

    std::string sourceStr = "null";
    if (source != nullptr) {
        sourceStr = *source;
    }

    std::string userStr = "null";
    if (user != nullptr) {
        userStr = *user;
    }

    std::string statusStr = "null";
    if (status != nullptr) {
        statusStr = *status;
    }

    std::string typeStr = "null";
    if (type != nullptr) {
        typeStr = *type;
    }

    LOG(INFO) << "GetTaskList:"
              << " Version = " << *version
              << ", User = " << userStr
              << ", Limit = " << limitNum
              << ", Offset = " << offsetNum
              << ", UUID = " << uuidStr
              << ", Source = " << sourceStr
              << ", Destination = " << destinationStr
              << ", Status = " << statusStr
              << ", Type = " << typeStr
              << ", requestId = " << requestId;

    std::vector<TaskCloneInfo> cloneTaskInfos;
    CloneFilterCondition filter(uuid, source, destination, user, status, type);
    int ret = cloneManager_->GetCloneTaskInfoByFilter(filter, &cloneTaskInfos);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
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
        i < cloneTaskInfos.size() && i < limitNum + offsetNum;
        i++) {
        Json::Value cloneTaskObj = cloneTaskInfos[i].ToJsonObj();
        listObj.append(cloneTaskObj);
    }

    mainObj[kTaskInfosStr] = listObj;

    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleGetCloneRefStatusAction(
    brpc::Controller* bcntl, const std::string &requestId) {
    const std::string *version =
        bcntl->http_request().uri().GetQuery(kVersionStr);
    const std::string *user =
        bcntl->http_request().uri().GetQuery(kUserStr);
    const std::string *source =
        bcntl->http_request().uri().GetQuery(kSourceStr);
    if ((version == nullptr) ||
        (user == nullptr) ||
        (source == nullptr) ||
        (version->empty()) ||
        (source->empty()) ||
        (user->empty())) {
        HandleBadRequestError(bcntl, requestId);
        return;
    }

    LOG(INFO) << "GetCloneRefStatus:"
              << " Version = " << *version
              << ", User = " << *user
              << ", Source = " << *source
              << ", requestId = " << requestId;

    std::vector<CloneInfo> cloneInfos;
    CloneRefStatus refStatus;
    int ret = cloneManager_->GetCloneRefStatus(*source, &refStatus,
                                                &cloneInfos);
    if (ret < 0) {
        bcntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        SetErrorMessage(bcntl, ret, requestId);
        return;
    }

    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    butil::IOBufBuilder os;
    Json::Value mainObj;
    mainObj[kCodeStr] = std::to_string(kErrCodeSuccess);
    mainObj[kMessageStr] = code2Msg[kErrCodeSuccess];
    mainObj[kRequestIdStr] = requestId;
    mainObj[kRefStatusStr] = static_cast<int> (refStatus);
    mainObj[kTotalCountStr] = 0;
    if (refStatus == CloneRefStatus::kNeedCheck) {
        mainObj[kTotalCountStr] = cloneInfos.size();
        Json::Value listObj;
        for (int i = 0; i < cloneInfos.size(); i++) {
            Json::Value cloneTaskObj;
            cloneTaskObj[kUserStr] = cloneInfos[i].GetUser();
            cloneTaskObj[kFileStr] = cloneInfos[i].GetDest();
            if (cloneInfos[i].GetTaskType() == CloneTaskType::kClone) {
                cloneTaskObj[kInodeStr] = cloneInfos[i].GetDestId();
            } else {
                cloneTaskObj[kInodeStr] = cloneInfos[i].GetOriginId();
            }

            listObj.append(cloneTaskObj);
        }

        mainObj[kCloneFileInfoStr] = listObj;
    }

    os << mainObj.toStyledString();
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::SetErrorMessage(brpc::Controller* bcntl,
                        int errCode,
                        const std::string &requestId,
                        const std::string &uuid) {
    butil::IOBufBuilder os;
    std::string msg = BuildErrorMessage(errCode,
        requestId, uuid);
    os << msg;
    os.move_to(bcntl->response_attachment());
    return;
}

void SnapshotCloneServiceImpl::HandleBadRequestError(brpc::Controller* bcntl,
                        const std::string &requestId,
                        const std::string &uuid) {
    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
    SetErrorMessage(bcntl, kErrCodeInvalidRequest, requestId, uuid);
}

}  // namespace snapshotcloneserver
}  // namespace curve
