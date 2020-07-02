/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
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

    LOG(INFO) << "Heartbeat thread stopped";
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

        LOG(INFO) << "Connection Manager stopped success";
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

    LOG(INFO) << "Send Heartbeat request, log id = " << cntl.log_id()
              << ", pid = " << request.pid()
              << ", nebd version = " << request.nebdversion()
              << ", files [" << oss.str() << ']';

    stub.KeepAlive(&cntl, &request, &response, nullptr);

    bool isCntlFailed = cntl.Failed();
    if (isCntlFailed) {
        LOG(WARNING) << "Heartbeat request failed, error = "
                     << cntl.ErrorText()
                     << ", log id = " << cntl.log_id();
    }
}

}  // namespace client
}  // namespace nebd
