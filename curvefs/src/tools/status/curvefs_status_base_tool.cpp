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
 * Created Date: 2021-10-26
 * Author: chengyi01
 */

#include "curvefs/src/tools/status/curvefs_status_base_tool.h"

namespace curvefs {
namespace tools {
namespace status {

int StatusBaseTool::Init() {
    if (CurvefsToolMetric::Init() != 0) {
        return -1;
    }

    // get version from one host
    if (!hostsAddr_.empty()) {
        // get version from 1 host, just ok
        AddAddr2Suburi({hostsAddr_[0], versionSubUri_});
    }

    //  get status(leader or not) from all host
    for (auto const& i : hostsAddr_) {
        AddAddr2Suburi({i, StatusSubUri_});
    }

    return 0;
}

void StatusBaseTool::AfterGetMetric(const std::string hostAddr,
                                    const std::string& subUri,
                                    const std::string& value,
                                    const MetricStatusCode& statusCode) {
    if (statusCode == MetricStatusCode::kOK) {
        onlineHosts_.push_back(hostAddr);
        if (subUri == StatusSubUri_) {
            std::string keyValue;
            if (!metricClient_->GetKeyValueFromString(value, statusKey_,
                                                      &keyValue)) {
                if (keyValue == hostStandbyValue_) {
                    // standby host
                    standbyHost_.push_back(hostAddr);
                } else if (keyValue == hostLeaderValue_) {
                    // leader host
                    leaderHosts_.push_back(hostAddr);
                } else {
                    // error host
                    errorHosts_.push_back(hostAddr);
                }
            } else {
                std::cerr << "parse " << statusKey_ << " form " << hostAddr
                          << subUri << " error." << std::endl;
                errorHosts_.push_back(hostAddr);
            }
        } else if (subUri == versionSubUri_) {
            version_ = value;
        }

    } else {
        // offline host
        offlineHosts_.push_back(hostAddr);
    }
}

int StatusBaseTool::ProcessMetrics() {
    int ret = 0;

    // version
    if (show_) {
        std::cout << hostType_ << " version: " << version_ << std::endl;
    }

    // leader host
    if (leaderHosts_.empty()) {
        ret = -1;
        if (show_) {
            std::cerr << "no leader " << hostType_ << "." << std::endl;
        }
    } else if (leaderHosts_.size() > 1) {
        ret = -1;
        if (show_) {
            std::cerr << "more than 1 leader " << hostType_ << ":[ ";
            for (auto const& i : leaderHosts_) {
                std::cerr << i << " ";
            }
            std::cerr << "]." << std::endl;
        }
    } else if (show_) {
        std::cout << "leader " << hostType_ << ": " << leaderHosts_[0]
                  << std::endl;
    }

    // standby host
    if (show_) {
        std::cout << "standy " << hostType_ << ": [ ";
        for (auto const& i : standbyHost_) {
            std::cerr << i << " ";
        }
        std::cout << "]." << std::endl;
    }

    // error host
    if (!errorHosts_.empty()) {
        ret = -1;
        if (show_) {
            std::cerr << "error " << hostType_ << ": [";
            for (auto const& i : errorHosts_) {
                std::cerr << i << " ";
            }
            std::cout << "]." << std::endl;
        }
    }

    // offline host
    if (!offlineHosts_.empty()) {
        ret = -1;
        if (show_) {
            std::cout << "offline " << hostType_ << ": [ ";
            for (auto const& i : offlineHosts_) {
                std::cerr << i << " ";
            }
            std::cout << "]." << std::endl;
        }
    }
    return ret;
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
