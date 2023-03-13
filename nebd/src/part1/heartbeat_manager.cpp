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

/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 */

#include "nebd/src/part1/heartbeat_manager.h"

#include <unistd.h>
#include <ostream>
#include <vector>
#include <string>

#include "nebd/proto/heartbeat.pb.h"
#include "nebd/src/common/nebd_version.h"

namespace nebd {
namespace client {

HeartbeatManager::HeartbeatManager(
    std::shared_ptr<NebdClientMetaCache> metaCache)
    : metaCache_(metaCache)
    , running_(false)
    , logId_(1) {
}

int HeartbeatManager::Init(const HeartbeatOption& option) {
    heartbeatOption_ = option;

    int ret = channel_.InitWithSockFile(
        option.serverAddress.c_str(), nullptr);
    if (ret != 0) {
        LOG(ERROR) << "Connection Manager channel init failed";
        return -1;
    }

    pid_ = getpid();
    nebdVersion_ = nebd::common::NebdVersion();

    return 0;
}

void HeartbeatManager::HeartBetaThreadFunc() {
    LOG(INFO) << "Heartbeat thread started";

    while (running_) {
        SendHeartBeat();
        sleeper_.wait_for(std::chrono::seconds(
            heartbeatOption_.intervalS));
    }
}

void HeartbeatManager::Run() {
    running_ = true;
    heartbeatThread_ = std::thread(
        &HeartbeatManager::HeartBetaThreadFunc, this);
}

void HeartbeatManager::Stop() {
    if (running_ == true) {
        running_ = false;
        sleeper_.interrupt();
        heartbeatThread_.join();
    }
}

void HeartbeatManager::SendHeartBeat() {
    std::vector<NebdClientFileInfo> fileInfos(metaCache_->GetAllFileInfo());
    if (fileInfos.empty()) {
        return;
    }

    HeartbeatRequest request;
    HeartbeatResponse response;

    brpc::Controller cntl;
    cntl.set_log_id(logId_.fetch_add(1, std::memory_order_relaxed));
    cntl.set_timeout_ms(heartbeatOption_.rpcTimeoutMs);

    NebdHeartbeatService_Stub stub(&channel_);

    request.set_pid(pid_);
    request.set_nebdversion(nebdVersion_);

    std::ostringstream oss;
    for (const auto& fileInfo : fileInfos) {
        nebd::client::HeartbeatFileInfo* info = request.add_info();
        info->set_fd(fileInfo.fd);
        info->set_name(fileInfo.fileName);

        oss << fileInfo.fd << " : " << fileInfo.fileName << ", ";
    }

    // zzw: it's possible to cause undefined behavior if the deinitialization of
    //      this LOG object is happening after the deinitialization of static nebdclient 
    //      when app exits.
     
    // LOG(INFO) << "Send Heartbeat request, log id = " << cntl.log_id()
    //           << ", pid = " << request.pid()
    //           << ", nebd version = " << request.nebdversion()
    //           << ", files [" << oss.str() << ']';

    stub.KeepAlive(&cntl, &request, &response, nullptr);

    bool isCntlFailed = cntl.Failed();
    if (isCntlFailed) {
        // LOG(WARNING) << "Heartbeat request failed, error = "
        //              << cntl.ErrorText()
        //              << ", log id = " << cntl.log_id();
    }
}

}  // namespace client
}  // namespace nebd
