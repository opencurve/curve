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

DEFINE_uint32(logical_pool_id, 1, "logical pool");
DECLARE_string(mdsAddr);
DEFINE_bool(scheduleAll, true, "schedule all logical pool or not");
DEFINE_bool(scanEnable, true, "Enable(true)/Disable(false) scan "
                               "for specify logical pool");

namespace curve {
namespace tool {

bool ScheduleTool::SupportCommand(const std::string& command) {
    return command == kRapidLeaderSchedule ||
           command == kSetScanState;
}

void ScheduleTool::PrintHelp(const std::string& cmd) {
    if (kRapidLeaderSchedule == cmd) {
        PrintRapidLeaderScheduleHelp();
    } else if (cmd == kSetScanState) {
        PrintSetScanStateHelp();
    } else {
        std::cout << cmd << " not supported!" << std::endl;
    }
}

void ScheduleTool::PrintRapidLeaderScheduleHelp() {
    std::cout << "Example :" << std::endl
        << "curve_ops_tool " << kRapidLeaderSchedule
        << " -logical_pool_id=1 -scheduleAll=false [-mdsAddr=127.0.0.1:6666]"
        << " [-confPath=/etc/curve/tools.conf]"
        << std::endl;
    std::cout << "curve_ops_tool " << kRapidLeaderSchedule
        << " [-mdsAddr=127.0.0.1:6666]"
        << " [-confPath=/etc/curve/tools.conf]"
        << std::endl;
}

void ScheduleTool::PrintSetScanStateHelp() {
    std::cout
        << "Example:" << std::endl
        << "  curve_ops_tool " << kSetScanState
        << " -logical_pool_id=1 -scanEnable=true/false"
        << " [-mdsAddr=127.0.0.1:6666]"
        << " [-confPath=/etc/curve/tools.conf]"
        << std::endl;
}

int ScheduleTool::RunCommand(const std::string &cmd) {
    if (kRapidLeaderSchedule == cmd) {
        return DoRapidLeaderSchedule();
    }  else if (cmd == kSetScanState) {
        return DoSetScanState();
    }
    std::cout << "Command not supported!" << std::endl;
    return -1;
}

int ScheduleTool::DoSetScanState() {
    if (mdsClient_->Init(FLAGS_mdsAddr) != 0) {
        std::cout << "Init mds client fail" << std::endl;
        return -1;
    }

    auto lpid = FLAGS_logical_pool_id;
    auto scanEnable = FLAGS_scanEnable;
    auto retCode = mdsClient_->SetLogicalPoolScanState(lpid, scanEnable);
    std::cout << (scanEnable ? "Enable" : "Disable")
              << " scan for logicalpool(" << lpid << ")"
              << (retCode == 0 ? " success" : " fail") << std::endl;
    return retCode;
}

int ScheduleTool::DoRapidLeaderSchedule() {
     if (0 != mdsClient_->Init(FLAGS_mdsAddr)) {
        std::cout << "Init mds client fail!" << std::endl;
        return -1;
    }
    if (FLAGS_scheduleAll) {
        return ScheduleAll();
    } else {
        return ScheduleOne(FLAGS_logical_pool_id);
    }
}

int ScheduleTool::ScheduleOne(PoolIdType lpoolId) {
    // 给mds发送rpc
    int res = mdsClient_->RapidLeaderSchedule(lpoolId);
    if (res != 0) {
        std::cout << "RapidLeaderSchedule pool " << lpoolId
                  << " fail" << std::endl;
        return -1;
    }
    return 0;
}

int ScheduleTool::ScheduleAll() {
    std::vector<LogicalPoolInfo> pools;
    int res = mdsClient_->ListLogicalPoolsInCluster(&pools);
    if (res != 0) {
        std::cout << "ListLogicalPoolsInCluster fail" << std::endl;
        return -1;
    }
    for (const auto& pool : pools) {
        if (mdsClient_->RapidLeaderSchedule(pool.logicalpoolid()) != 0) {
            std::cout << "RapidLeaderSchedule pool " << pool.logicalpoolid()
                      << " fail" << std::endl;
            res = -1;
        }
    }
    return res;
}

}  // namespace tool
}  // namespace curve
