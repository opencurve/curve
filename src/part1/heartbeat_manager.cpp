/**
 * Project: nebd
 * Create Date: 2020-01-20
 * Author: wuhanqing
 * Copyright (c) 2020 netease
 */

#include "src/part1/heartbeat_manager.h"

#include <ostream>
#include <vector>
#include <string>

#include "proto/heartbeat.pb.h"

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
    running_ = false;
    sleeper_.interrupt();

    if (heartbeatThread_.joinable()) {
        heartbeatThread_.join();
    }

    LOG(INFO) << "Connection Manager stopped success";
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

    std::ostringstream oss;
    for (const auto& fileInfo : fileInfos) {
        nebd::client::HeartbeatFileInfo* info = request.add_info();
        info->set_fd(fileInfo.fd);
        info->set_name(fileInfo.fileName);

        oss << fileInfo.fd << " : " << fileInfo.fileName << ", ";
    }

    LOG(INFO) << "Send Heartbeat request, log id = " << cntl.log_id()
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
