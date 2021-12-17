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
 * Created Date: 2021-10-28
 * Author: chengyi01
 */
#include "curvefs/src/tools/status/curvefs_metaserver_status.h"

DECLARE_string(metaserverAddr);

namespace curvefs {
namespace tools {
namespace status {

void MetaserverStatusTool::PrintHelp() {
    StatusBaseTool::PrintHelp();
    std::cout << " [-metaserverAddr=" << FLAGS_metaserverAddr << "]";
    std::cout << std::endl;
}

void MetaserverStatusTool::InitHostsAddr() {
    curve::common::SplitString(FLAGS_metaserverAddr, ",", &hostsAddr_);
}

void MetaserverStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMetaserverAddr);
    StatusBaseTool::AddUpdateFlags();
}

int MetaserverStatusTool::ProcessMetrics() {
    int ret = 0;

    // version
    if (show_) {
        std::cout << hostType_ << " version: " << version_ << std::endl;
    }

    // online host
    if (onlineHosts_.empty()) {
        if (show_) {
            std::cerr << "no online " << hostType_ << "." << std::endl;
        }
        ret = -1;
    } else if (show_) {
        std::cout << "online " << hostType_ << ": [ ";
        for (auto const& i : onlineHosts_) {
            std::cerr << i << " ";
        }
        std::cout << "]." << std::endl;
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

int MetaserverStatusTool::Init() {
    versionSubUri_ = kVersionUri;
    statusSubUri_ = kMetaserverStatusUri;
    versionKey_ = kVersionKey;

    return StatusBaseTool::Init();
}

void MetaserverStatusTool::AfterGetMetric(const std::string hostAddr,
                                          const std::string& subUri,
                                          const std::string& value,
                                          const MetricStatusCode& statusCode) {
    if (statusCode == MetricStatusCode::kOK) {
        onlineHosts_.insert(hostAddr);
        if (subUri == statusSubUri_) {
            // get response is ok
            onlineHosts_.insert(hostAddr);
        } else if (subUri == versionSubUri_) {
            std::string keyValue;
            if (!metricClient_->GetKeyValueFromString(value, versionKey_,
                                                      &keyValue)) {
                version_ = keyValue;
            } else {
                std::cerr << "parse " << versionKey_ << " form " << hostAddr
                          << subUri << " error." << std::endl;
                version_ = "unknown";
            }
        }

    } else if (subUri == statusSubUri_) {
        // offline host
        offlineHosts_.insert(hostAddr);
    }
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
