/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-10-29
 * Author: chengyi01
 */
#include "curvefs/src/tools/status/curvefs_etcd_status.h"

DECLARE_string(etcdAddr);

namespace curvefs {
namespace tools {
namespace status {

void EtcdStatusTool::PrintHelp() {
    StatusBaseTool::PrintHelp();
    std::cout << " [-etcdAddr=" << FLAGS_etcdAddr << "]";
    std::cout << std::endl;
}

int EtcdStatusTool::Init() {
    int ret = CurvefsToolMetric::Init();
    InitHostAddr();

    if (!hostAddr_.empty()) {
        // get version from 1 host, just ok
        AddAddr2Suburi({hostAddr_[0], kEtcdVersionUri});
    }

    //  get status(leader or not) from all host
    for (auto const& i : hostAddr_) {
        AddAddr2Suburi({i, kEtcdStatusUri});
    }

    return ret;
}

void EtcdStatusTool::AfterGetMetric(const std::string hostAddr,
                                    const std::string& subUri,
                                    const std::string& value,
                                    const MetricStatusCode& statusCode) {
    if (statusCode == MetricStatusCode::kOK) {
        if (subUri == kEtcdStatusUri) {
            std::string keyValue;
            if (!metricClient_->GetKeyValueFromJson(value, kEtcdStateKey,
                                                    &keyValue)) {
                if (keyValue == kEtcdFollowerValue) {
                    // standby host
                    standbyHost_.push_back(hostAddr);
                } else if (keyValue == kEtcdLeaderValue) {
                    // leader host
                    leaderHost_.push_back(hostAddr);
                } else {
                    // state is unkown
                    std::cerr << "etcd' state in" << hostAddr
                              << "/v2/stats/self is unkown." << std::endl;
                    standbyHost_.push_back(hostAddr);
                }
            } else {
                // etcd version is not compatible uri:/v2/stats/self
                std::cerr << "etcd in" << hostAddr
                          << " is not compatible with /v2/stats/self."
                          << std::endl;
                offlineHost_.push_back(hostAddr);
            }
        } else if (subUri == kEtcdVersionUri) {
            std::string keyValue;

            if (!metricClient_->GetKeyValueFromJson(
                    value, kEtcdClusterVersionKey, &keyValue)) {
                version_ = keyValue;
            }
        }
    } else {
        // offline host
        offlineHost_.push_back(hostAddr);
    }
}

void EtcdStatusTool::InitHostAddr() {
    curve::common::SplitString(FLAGS_etcdAddr, ",", &hostAddr_);
}

void EtcdStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetEtcdAddr);
    StatusBaseTool::AddUpdateFlags();
}
}  // namespace status
}  // namespace tools
}  // namespace curvefs
