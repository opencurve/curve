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
 * File Created: 20200108
 * Author: lixiaocui
 */

#include <gflags/gflags.h>
#include <vector>
#include "src/tools/schedule_tool.h"
#include "src/tools/curve_tool_define.h"

DEFINE_int32(logical_pool_id, 1, "logical pool");
DECLARE_string(mdsAddr);

namespace curve {
namespace tool {

bool ScheduleTool::SupportCommand(const std::string& command) {
    return command == kRapidLeaderSchedule;
}

void ScheduleTool::PrintHelp(const std::string& cmd) {
    if (kRapidLeaderSchedule == cmd) {
        PrintRapidLeaderScheduleHelp();
    } else {
        std::cout << cmd << " not supported!" << std::endl;
    }
}

void ScheduleTool::PrintRapidLeaderScheduleHelp() {
    std::cout << "Example :" << std::endl
        << "curve_ops_tool " << kRapidLeaderSchedule
        << " -logicalPoolId=1 [-mdsAddr=127.0.0.1:6666]"
        << " [-confPath=/etc/curve/tools.conf]"
        << std::endl;
}

int ScheduleTool::RunCommand(const std::string &cmd) {
    if (kRapidLeaderSchedule == cmd) {
        return DoRapidLeaderSchedule();
    }

    return -1;
}

int ScheduleTool::DoRapidLeaderSchedule() {
    // 初始化mds client
    if (0 != mdsClient_->Init(FLAGS_mdsAddr)) {
        return -1;
    }

    // 解析逻辑池id
    if (FLAGS_logical_pool_id < 0) {
        std::cout << "logicalPoolId must >=0" << std::endl;
        return -1;
    }

    // 给mds发送rpc
    return mdsClient_->RapidLeaderSchedule(FLAGS_logical_pool_id);
}

}  // namespace tool
}  // namespace curve
