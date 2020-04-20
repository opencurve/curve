/*
 * Project: curve
 * Created Date: 2020-03-17
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#include "src/tools/snapshot_clone_client.h"

namespace curve {
namespace tool {

int SnapshotCloneClient::Init(const std::string& serverAddr,
                              const std::string& dummyPort) {
    curve::common::SplitString(serverAddr, ",", &serverAddrVec_);
    if (serverAddrVec_.empty()) {
        std::cout << "Split snapshot clone server address fail!" << std::endl;
        return -1;
    }

    int res = InitDummyServerMap(dummyPort);
    if (res != 0) {
        std::cout << "init dummy server map fail!" << std::endl;
        return -1;
    }
    return 0;
}

int SnapshotCloneClient::InitDummyServerMap(const std::string& dummyPort) {
    std::vector<std::string> dummyPortVec;
    curve::common::SplitString(dummyPort, ",", &dummyPortVec);
    if (dummyPortVec.size() == 0) {
        std::cout << "split dummy server fail!" << std::endl;
        return -1;
    }
    // 只指定了一个端口，对所有mds采用这个端口
    if (dummyPortVec.size() == 1) {
        for (uint64_t i = 0; i < serverAddrVec_.size() - 1; ++i) {
            dummyPortVec.emplace_back(dummyPortVec[0]);
        }
    }

    if (dummyPortVec.size() != serverAddrVec_.size()) {
        std::cout << "snapshot clone server dummy port list must be correspond"
                     " as snapshot clone addr list" << std::endl;
        return -1;
    }

    for (uint64_t i = 0; i < serverAddrVec_.size(); ++i) {
        std::vector<std::string> strs;
        curve::common::SplitString(serverAddrVec_[i], ":", &strs);
        if (strs.size() != 2) {
            std::cout << "split snapshot clone addr fail!" << std::endl;
            return -1;
        }
        std::string dummyAddr = strs[0] + ":" + dummyPortVec[i];
        dummyServerMap_[serverAddrVec_[i]] = dummyAddr;
    }
    return 0;
}

std::vector<std::string> SnapshotCloneClient::GetActiveAddrs() {
    std::vector<std::string> activeAddrs;
    for (const auto item : dummyServerMap_) {
        // 获取status来判断正在服务的地址
        std::string status;
        MetricRet ret = metricClient_->GetMetric(item.second,
                            kSnapshotCloneStatusMetricName, &status);
        if (ret != MetricRet::kOK) {
            std::cout << "Get status metric from " << item.second
                      << " fail" << std::endl;
            continue;
        }
        if (status == kSnapshotCloneStatusActive) {
            // 如果是active状态，再访问一下服务端口
            MetricRet ret = metricClient_->GetMetric(item.first,
                            kSnapshotCloneStatusMetricName, &status);
            if (ret != MetricRet::kOK) {
                std::cout << "Get status metric from " << item.first
                          << " fail" << std::endl;
                continue;
            }
            activeAddrs.emplace_back(item.first);
        }
    }
    return activeAddrs;
}

void SnapshotCloneClient::GetOnlineStatus(
                                std::map<std::string, bool>* onlineStatus) {
    onlineStatus->clear();
    int result = 0;
    for (const auto item : dummyServerMap_) {
        std::string listenAddr;
        int res = GetListenAddrFromDummyPort(item.second, &listenAddr);
        // 如果获取到的监听地址与记录的mds地址不一致，也认为不在线
        if (res != 0 || listenAddr != item.first) {
            onlineStatus->emplace(item.first, false);
            continue;
        }
        onlineStatus->emplace(item.first, true);
    }
}

int SnapshotCloneClient::GetListenAddrFromDummyPort(
                                const std::string& dummyAddr,
                                std::string* listenAddr) {
    MetricRet res = metricClient_->GetConfValueFromMetric(dummyAddr,
                        kSnapshotCloneConfMetricName, listenAddr);
    if (res != MetricRet::kOK) {
        return -1;
    }
    return 0;
}

}  // namespace tool
}  // namespace curve
