/*
 * Project: curve
 * File Created: 20200108
 * Author: lixiaocui
 * Copyright (c)￼ 2018 netease
 */

#include <gflags/gflags.h>
#include <vector>
#include "src/tools/schedule_tool.h"
#include "src/tools/curve_tool_define.h"

DEFINE_int32(logicalPoolId, 1, "logical pool");
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
        << " -mdsAddr=127.0.0.1:6666 -logicalPoolId=1"
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
    if (FLAGS_logicalPoolId < 0) {
        std::cout << "logicalPoolId must >=0" << std::endl;
        return -1;
    }

    // 给mds发送rpc
    return mdsClient_->RapidLeaderSchedule(FLAGS_logicalPoolId);
}

}  // namespace tool
}  // namespace curve
