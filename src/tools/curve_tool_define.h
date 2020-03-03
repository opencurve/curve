/*
 * Project: curve
 * Created Date: 2019-12-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CURVE_TOOL_DEFINE_H_
#define SRC_TOOLS_CURVE_TOOL_DEFINE_H_

#include <string>

namespace curve {
namespace tool {
// StatusTool相关命令
const char kStatusCmd[] = "status";
const char kSpaceCmd[] = "space";
const char kChunkserverStatusCmd[] = "chunkserver-status";
const char kMdsStatusCmd[] = "mds-status";
const char kEtcdStatusCmd[] = "etcd-status";
const char kChunkserverListCmd[] = "chunkserver-list";
const char kClientStatusCmd[] = "client-status";

// NamesPaceTool相关命令
const char kGetCmd[] = "get";
const char kListCmd[] = "list";
const char kSegInfoCmd[] = "seginfo";
const char kDeleteCmd[] = "delete";
const char kCreateCmd[] = "create";
const char kCleanRecycleCmd[] = "clean-recycle";
const char kChunkLocatitonCmd[] = "chunk-location";

// CopysetCheck相关命令
const char kCheckCopysetCmd[] = "check-copyset";
const char kCheckChunnkServerCmd[] = "check-chunkserver";
const char kCheckServerCmd[] = "check-server";
const char kCopysetsStatusCmd[] = "copysets-status";
const char kCheckOperatorCmd[] = "check-operator";

// 一致性检查命令
const char kCheckConsistencyCmd[] = "check-consistency";

// 配置变更命令
const char kRemovePeerCmd[] = "remove-peer";
const char kTransferLeaderCmd[] = "transfer-leader";
const char kResetPeerCmd[] = "reset-peer";

// 调度模块命令
const char kRapidLeaderSchedule[] = "rapid-leader-schedule";

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_DEFINE_H_
