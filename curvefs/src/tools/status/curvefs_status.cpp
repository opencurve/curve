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
 * Created Date: 2021-12-15
 * Author: chengyi01
 */

#include "curvefs/src/tools/status/curvefs_status.h"

namespace curvefs {
namespace tools {
namespace status {

void StatusTool::PrintHelp() {
    CurvefsTool::PrintHelp();
    std::cout << std::endl;
}

int StatusTool::Init() {
    mdsStatusTool_ = std::make_shared<MdsStatusTool>();
    metaserverStatusTool_ = std::make_shared<MetaserverStatusTool>();
    etcdStatusTool_ = std::make_shared<EtcdStatusTool>();
    copysetStatutsTool_ = std::make_shared<CopysetStatusTool>();
    return 0;
}

int StatusTool::RunCommand() {
    std::cout << "[mds]" << std::endl;
    int mds_health = mdsStatusTool_->Run();
    std::cout << std::endl << "[metaserver]" << std::endl;
    int metaserver_health = metaserverStatusTool_->Run();
    std::cout << std::endl << "[etcd]" << std::endl;
    int etcd_health = etcdStatusTool_->Run();
    std::cout << std::endl << "[copyset]" << std::endl;
    int copyset_health = copysetStatutsTool_->Run();
    std::cout << std::endl << "[cluster]" << std::endl;
    if (mds_health != 0 || metaserver_health != 0 || etcd_health != 0 ||
        copyset_health != 0) {
        std::cout << "cluster is unhealthy!" << std::endl;
        return -1;
    }
    std::cout << "cluster is healthy!" << std::endl;
    return 0;
}

}  // namespace status
}  // namespace tools
}  // namespace curvefs
