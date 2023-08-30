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
 * Created Date: 2020-03-17
 * Author: charisu
 */

#include "src/tools/snapshot_clone_client.h"

namespace curve {
namespace tool {

int SnapshotCloneClient::Init(const std::string& serverAddr,
                              const std::string& dummyPort) {
    curve::common::SplitString(serverAddr, ",", &serverAddrVec_);
    if (serverAddrVec_.empty()) {
        // no snapshot clone server
        return 1;
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
    //Only one port has been specified, and this port is used for all mds
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
    for (const auto &item : dummyServerMap_) {
        //Obtain status to determine the address being served
        std::string status;
        MetricRet ret = metricClient_->GetMetric(item.second,
                            kSnapshotCloneStatusMetricName, &status);
        if (ret != MetricRet::kOK) {
            std::cout << "Get status metric from " << item.second
                      << " fail" << std::endl;
            continue;
        }
        if (status == kSnapshotCloneStatusActive) {
            //If it is in an active state, please visit the service port again
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
    for (const auto &item : dummyServerMap_) {
        std::string listenAddr;
        int res = GetListenAddrFromDummyPort(item.second, &listenAddr);
        //If the obtained listening address does not match the recorded MDS address, it is also considered offline
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
