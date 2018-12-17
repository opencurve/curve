/*
 * Project: curve
 * Created Date: Mon Dec 24 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#include "src/snapshot/snapshot_service.h"

#include <string>
#include <vector>

#include "json/json.h"
#include "src/snapshot/snapshot_define.h"

namespace curve {
namespace snapshotserver {

void SnapshotServiceImpl::default_method(RpcController* cntl,
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
        os << "BadRequest:\"Action is NULL\"";
        os.move_to(bcntl->response_attachment());
        return;
    }
    if (*action == "CreateSnapshot") {
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
            os << "BadRequest:\"missing parameter\".";
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
        int ret = manager_->CreateSnapshot(*file, *user, *name, &uuid);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            butil::IOBufBuilder os;
            os << "CreateSnapshot internal error, ret = "
               << ret;
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
    if (*action == "DeleteSnapshot") {
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
            os << "BadRequest:\"missing parameter.\".";
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
        int ret = manager_->DeleteSnapshot(*uuid, *user, *file);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            butil::IOBufBuilder os;
            os << "DeleteSnapshot internal error, ret = "
               << ret;
            os.move_to(bcntl->response_attachment());
            return;
        }
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
        return;
    }
    if (*action == "CancelSnapshot") {
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
            os << "BadRequest:\"missing parameter.\".";
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
        int ret = manager_->CancelSnapshot(*uuid, *user, *file);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            butil::IOBufBuilder os;
            os << "CancelSnapshot internal error, ret = "
               << ret;
            os.move_to(bcntl->response_attachment());
            return;
        }
        bcntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
        return;
    }
    if (*action == "GetFileSnapshotInfo") {
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
            os << "BadRequest:\"missing parameter\".";
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
        // todo(xuchaojie): handle version and user and limit and offset info.
        std::vector<FileSnapshotInfo> info;
        int ret = manager_->GetFileSnapshotInfo(*file, *user, &info);
        if (ret < 0) {
            bcntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
            butil::IOBufBuilder os;
            os << "GetFileSnapshotInfo internal error, ret = "
               << ret;
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

    bcntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
    butil::IOBufBuilder os;
    os << "BadRequest:\"Invalid Action \""
       << *action;
    os.move_to(bcntl->response_attachment());
    return;
}

}  // namespace snapshotserver
}  // namespace curve

