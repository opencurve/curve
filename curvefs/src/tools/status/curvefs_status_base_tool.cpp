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
    int ret = CurvefsToolMetric::Init();
    InitHostAddr();

    // get version from one host
    if (!hostAddr_.empty()) {
        // get version from 1 host, just ok
        AddAddr2Suburi({hostAddr_[0], versionSubUri_});
    }

    //  get status(leader or not) from all host
    for (auto const& i : hostAddr_) {
        AddAddr2Suburi({i, StatusSubUri_});
    }

    return ret;
}

void StatusBaseTool::AfterGetMetric(const std::string hostAddr,
                                    const std::string& subUri,
                                    const std::string& value,
                                    const MetricStatusCode& statusCode) {
    if (statusCode == MetricStatusCode::kOK) {
        onlineHost_.push_back(hostAddr);
        if (subUri == StatusSubUri_) {
            std::string keyValue;
            if (!metricClient_->GetKeyValueFromString(value, statusKey_,
                                                      &keyValue)) {
                if (keyValue == hostStandbyValue_) {
                    // standby host
                    standbyHost_.push_back(hostAddr);
                } else if (keyValue == hostLeaderValue_) {
                    // leader host
                    leaderHost_.push_back(hostAddr);
                } else {
                    // error host
                    errorHost_.push_back(hostAddr);
                }
            } else {
                std::cerr << "parse " << statusKey_ << " form " << hostAddr
                          << subUri << " error." << std::endl;
                offlineHost_.push_back(hostAddr);
            }
        } else if (subUri == versionSubUri_) {
            version_ = value;
        }

    } else {
        // offline host
        offlineHost_.push_back(hostAddr);
    }
}

int StatusBaseTool::ProcessMetrics() {
    int ret = 0;

    // version
    std::cout << hostType_ << " version: " << version_ << std::endl;

    // leader host
    if (leaderHost_.empty()) {
        std::cerr << "no leader " << hostType_ << "." << std::endl;
        ret = -1;
    } else if (leaderHost_.size() > 1) {
        ret = -1;
        std::cerr << "more than 1 leader " << hostType_ << ":[ ";
        for (auto const& i : leaderHost_) {
            std::cerr << i << " ";
        }
        std::cerr << "]." << std::endl;
        ret = -1;
    } else {
        std::cout << "leader " << hostType_ << ": " << leaderHost_[0]
                  << std::endl;
    }

    // standby host
    std::cout << "standy " << hostType_ << ": [ ";
    for (auto const& i : standbyHost_) {
        std::cerr << i << " ";
    }
    std::cout << "]." << std::endl;

    // error host
    if (!errorHost_.empty()) {
        ret = -1;
        std::cerr << "error " << hostType_ << ": [";
        for (auto const& i : errorHost_) {
            std::cerr << i << " ";
        }
        std::cout << "]." << std::endl;
    }

    // offline host
    if (offlineHost_.size() > 0) {
        ret = -1;
        std::cout << "offline " << hostType_ << ": [ ";
        for (auto const& i : offlineHost_) {
            std::cerr << i << " ";
        }
        std::cout << "]." << std::endl;
    }
    return ret;
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
