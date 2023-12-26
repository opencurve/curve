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
 * Created Date: 2020/11/27
 * Author: hzchenwei7
 */

#include "src/mds/snapshotcloneclient/snapshotclone_client.h"
#include <brpc/channel.h>
#include <json/json.h>
#include <utility>
#include "src/common/string_util.h"

using curve::snapshotcloneserver::kServiceName;
using curve::snapshotcloneserver::kActionStr;
using curve::snapshotcloneserver::kGetCloneRefStatusAction;
using curve::snapshotcloneserver::kVersionStr;
using curve::snapshotcloneserver::kUserStr;
using curve::snapshotcloneserver::kSourceStr;
using curve::snapshotcloneserver::kCodeStr;
using curve::snapshotcloneserver::kRefStatusStr;
using curve::snapshotcloneserver::kTotalCountStr;
using curve::snapshotcloneserver::kCloneFileInfoStr;
using curve::snapshotcloneserver::kFileStr;
using curve::snapshotcloneserver::kInodeStr;


namespace curve {
namespace mds {
namespace snapshotcloneclient {

StatusCode SnapshotCloneClient::GetCloneRefStatus(std::string filename,
                                    std::string user,
                                    CloneRefStatus *status,
                                    std::vector<DestFileInfo> *fileCheckList) {
    if (!inited_) {
        LOG(WARNING) << "GetCloneRefStatus, snapshot clone server not inited"
                     << ", filename = " << filename
                     << ", user = " << user;
        return StatusCode::kSnapshotCloneServerNotInit;
    }

    std::string data;
    size_t index = 0;
    StatusCode st = StatusCode::KInternalError;
    while (index < addrs_.size()) {
        st = GetCloneRefStatus(addrs_[index], filename, user, &data);
        if (st == StatusCode::kOK) {
            break;
        }

        LOG(WARNING) << "GetCloneRefStatus, Fail to get status from "
                     << addrs_[index];
        ++index;
    }

    if (index >= addrs_.size()) {
        LOG(ERROR)
            << "GetCloneRefStatus, Fail to get status from all addresses";
        return st;
    }

    Json::Reader jsonReader;
    Json::Value jsonObj;
    if (!jsonReader.parse(data, jsonObj)) {
        LOG(ERROR) << "GetCloneRefStatus, parse json fail, data = " << data
                   << ", filename = " << filename
                   << ", user = " << user;
        return StatusCode::KInternalError;
    }

    LOG(INFO) << "GetCloneRefStatus, " << data;

    std::string requestCode = jsonObj[kCodeStr].asCString();
    if (requestCode != "0") {
        LOG(ERROR) << "GetCloneRefStatus, Code is not 0, data = " << data
                   << ", filename = " << filename
                   << ", user = " << user;
        return StatusCode::KInternalError;
    }

    CloneRefStatus tempStatus =
            static_cast<CloneRefStatus>(jsonObj[kRefStatusStr].asInt());
    *status = tempStatus;
    if (tempStatus == CloneRefStatus::kNoRef
        || tempStatus == CloneRefStatus::kHasRef) {
        return StatusCode::kOK;
    }

    if (tempStatus != CloneRefStatus::kNeedCheck) {
        LOG(ERROR) << "GetCloneRefStatus, invalid status, data = " << data
                   << ", filename = " << filename
                   << ", user = " << user;
        return StatusCode::KInternalError;
    }

    int totalCount = jsonObj[kTotalCountStr].asInt();
    int listSize = jsonObj[kCloneFileInfoStr].size();
    for (int i = 0; i < listSize; i++) {
        DestFileInfo file;
        file.filename = jsonObj[kCloneFileInfoStr][i][kFileStr].asCString();
        file.inodeid = jsonObj[kCloneFileInfoStr][i][kInodeStr].asUInt64();
        fileCheckList->push_back(file);
    }
    return StatusCode::kOK;
}

bool SnapshotCloneClient::Init(const SnapshotCloneClientOption &option) {
    if (option.snapshotCloneAddr.empty()) {
        LOG(WARNING) << "Fail to init snapshot clone client, addr is empty";
        return false;
    }

    std::vector<std::string> addresses;
    curve::common::SplitString(option.snapshotCloneAddr, ",", &addresses);
    if (addresses.empty()) {
        LOG(WARNING) << "Fail to split address";
        return false;
    }

    addrs_ = std::move(addresses);
    inited_ = true;

    return true;
}

bool SnapshotCloneClient::GetInitStatus() {
    return inited_;
}

StatusCode SnapshotCloneClient::GetCloneRefStatus(const std::string& addr,
                                                  const std::string& filename,
                                                  const std::string& user,
                                                  std::string* response) {
    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = addr
                    + "/" + kServiceName + "?"
                    + kActionStr+ "=" + kGetCloneRefStatusAction + "&"
                    + kVersionStr + "=1&"
                    + kUserStr + "=" + user + "&"
                    + kSourceStr + "=" + filename;

    if (channel.Init(url.c_str(), "", &option) != 0) {
        LOG(WARNING) << "GetCloneRefStatus, Fail to init channel, url is "
                     << url << ", filename = " << filename
                     << ", user = " << user;
        return StatusCode::kSnapshotCloneConnectFail;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "GetCloneRefStatus, CallMethod failed, errMsg :"
                     << cntl.ErrorText() << ", filename = " << filename
                     << ", user = " << user;
        return StatusCode::KInternalError;
    }

    *response = cntl.response_attachment().to_string();
    return StatusCode::kOK;
}

}  // namespace snapshotcloneclient
}  // namespace mds
}  // namespace curve

