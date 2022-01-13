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
#include "curvefs/src/tools/status/curvefs_mds_status.h"

DECLARE_string(mdsAddr);
DECLARE_string(mdsDummyAddr);

namespace curvefs {
namespace tools {
namespace status {

void MdsStatusTool::PrintHelp() {
    StatusBaseTool::PrintHelp();
    std::cout << " [-mdsAddr=" << FLAGS_mdsAddr << "]"
              << "[-mdsDummyAddr=" << FLAGS_mdsDummyAddr << "]";
    std::cout << std::endl;
}

int MdsStatusTool::Init() {
    versionSubUri_ = kVersionUri;
    statusSubUri_ = kMdsStatusUri;
    statusKey_ = kMdsStatusKey;
    versionKey_ = kVersionKey;

    int ret = StatusBaseTool::Init();
    if (ret != 0) {
        return ret;
    }

    std::vector<std::string> hostsMainAddr;
    curve::common::SplitString(FLAGS_mdsAddr, ",", &hostsMainAddr);
    if (hostsMainAddr.size() != hostsAddr_.size()) {
        std::cerr << "mdsDummyAddr and mdsAddr do not match, please check."
                  << std::endl;
        ret = -1;
    } else {
        for (size_t i = 0; i < hostsAddr_.size(); i++) {
            dummy2MainAddr_[hostsAddr_[i]] = hostsMainAddr[i];
        }
    }

    return ret;
}

void MdsStatusTool::InitHostsAddr() {
    // use dummy addr
    curve::common::SplitString(FLAGS_mdsDummyAddr, ",", &hostsAddr_);
}

void MdsStatusTool::AddUpdateFlags() {
    AddUpdateFlagsFunc(curvefs::tools::SetMdsAddr);
    AddUpdateFlagsFunc(curvefs::tools::SetMdsDummyAddr);
    StatusBaseTool::AddUpdateFlags();
}

void MdsStatusTool::AfterGetMetric(const std::string hostAddr,
                                   const std::string& subUri,
                                   const std::string& value,
                                   const MetricStatusCode& statusCode) {
    auto mainAddr = hostAddr;
    if (statusCode == MetricStatusCode::kOK) {
        onlineHosts_.insert(mainAddr);
        if (subUri == statusSubUri_) {
            std::string keyValue;
            if (!metricClient_->GetKeyValueFromString(value, statusKey_,
                                                      &keyValue)) {
                if (keyValue == hostStandbyValue_) {
                    // standby host
                    standbyHost_.insert(mainAddr);
                } else if (keyValue == hostLeaderValue_) {
                    // leader host
                    mainAddr = dummy2MainAddr_[hostAddr];
                    leaderHosts_.insert(mainAddr);
                } else {
                    // error host
                    errorHosts_.insert(mainAddr);
                }
            } else {
                std::cerr << "parse " << statusKey_ << " form " << mainAddr
                          << subUri << " error." << std::endl;
                errorHosts_.insert(mainAddr);
            }
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
    } else if (statusCode == MetricStatusCode::kNotFound) {
        // standby host main port
        standbyHost_.insert(mainAddr);
    } else {
        // offline host
        offlineHosts_.insert(mainAddr);
    }
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
