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

#include <memory>

using curve::snapshotcloneserver::kActionStr;
using curve::snapshotcloneserver::kCloneFileInfoStr;
using curve::snapshotcloneserver::kCodeStr;
using curve::snapshotcloneserver::kFileStr;
using curve::snapshotcloneserver::kGetCloneRefStatusAction;
using curve::snapshotcloneserver::kInodeStr;
using curve::snapshotcloneserver::kRefStatusStr;
using curve::snapshotcloneserver::kServiceName;
using curve::snapshotcloneserver::kSourceStr;
using curve::snapshotcloneserver::kTotalCountStr;
using curve::snapshotcloneserver::kUserStr;
using curve::snapshotcloneserver::kVersionStr;


namespace curve {
namespace mds {
namespace snapshotcloneclient {

StatusCode SnapshotCloneClient::GetCloneRefStatus(
    std::string filename, std::string user, CloneRefStatus *status,
    std::vector<DestFileInfo> *fileCheckList) {
    if (!inited_) {
        LOG(WARNING) << "GetCloneRefStatus, snapshot clone server not inited"
                     << ", filename = " << filename << ", user = " << user;
        return StatusCode::kSnapshotCloneServerNotInit;
    }

    brpc::Channel channel;
    brpc::ChannelOptions option;
    option.protocol = "http";

    std::string url = addr_ + "/" + kServiceName + "?" + kActionStr + "=" +
                      kGetCloneRefStatusAction + "&" + kVersionStr + "=1&" +
                      kUserStr + "=" + user + "&" + kSourceStr + "=" + filename;

    if (channel.Init(url.c_str(), "", &option) != 0) {
        LOG(ERROR) << "GetCloneRefStatus, Fail to init channel, url is " << url
                   << ", filename = " << filename << ", user = " << user;
        return StatusCode::kSnapshotCloneConnectFail;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url.c_str();

    channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    if (cntl.Failed()) {
        LOG(ERROR) << "GetCloneRefStatus, CallMethod faile, errMsg :"
                   << cntl.ErrorText() << ", filename = " << filename
                   << ", user = " << user;
        return StatusCode::KInternalError;
    }

    std::stringstream ss;
    ss << cntl.response_attachment();
    std::string data = ss.str();

    Json::CharReaderBuilder jsonBuilder;
    std::unique_ptr<Json::CharReader> jsonReader(jsonBuilder.newCharReader());
    Json::Value jsonObj;
    JSONCPP_STRING errormsg;
    if (!jsonReader->parse(data.data(), data.data() + data.length(), &jsonObj,
                           &errormsg)) {
        LOG(ERROR) << "GetCloneRefStatus, parse json fail, data = " << data
                   << ", filename = " << filename << ", user = " << user
                   << ", error = " << errormsg;
        return StatusCode::KInternalError;
    }

    LOG(INFO) << "GetCloneRefStatus, " << data;

    std::string requestCode = jsonObj[kCodeStr].asCString();
    if (requestCode != "0") {
        LOG(ERROR) << "GetCloneRefStatus, Code is not 0, data = " << data
                   << ", filename = " << filename << ", user = " << user;
        return StatusCode::KInternalError;
    }

    CloneRefStatus tempStatus =
        static_cast<CloneRefStatus>(jsonObj[kRefStatusStr].asInt());
    *status = tempStatus;
    if (tempStatus == CloneRefStatus::kNoRef ||
        tempStatus == CloneRefStatus::kHasRef) {
        return StatusCode::kOK;
    }

    if (tempStatus != CloneRefStatus::kNeedCheck) {
        LOG(ERROR) << "GetCloneRefStatus, invalid status, data = " << data
                   << ", filename = " << filename << ", user = " << user;
        return StatusCode::KInternalError;
    }

    int listSize = jsonObj[kCloneFileInfoStr].size();
    for (int i = 0; i < listSize; i++) {
        DestFileInfo file;
        file.filename = jsonObj[kCloneFileInfoStr][i][kFileStr].asCString();
        file.inodeid = jsonObj[kCloneFileInfoStr][i][kInodeStr].asUInt64();
        fileCheckList->push_back(file);
    }
    return StatusCode::kOK;
}

void SnapshotCloneClient::Init(const SnapshotCloneClientOption &option) {
    if (!option.snapshotCloneAddr.empty()) {
        addr_ = option.snapshotCloneAddr;
        inited_ = true;
    }
}

bool SnapshotCloneClient::GetInitStatus() { return inited_; }
}  // namespace snapshotcloneclient
}  // namespace mds
}  // namespace curve
