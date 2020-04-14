/*
 * Project: curve
 * Created Date: 2019-12-30
 * Author: charisu
 * Copyright (c) 2018 netease
 */

#ifndef SRC_TOOLS_CURVE_TOOL_DEFINE_H_
#define SRC_TOOLS_CURVE_TOOL_DEFINE_H_

#include <string>

DECLARE_string(mdsAddr);
DECLARE_string(mdsDummyPort);
DECLARE_string(etcdAddr);
DECLARE_uint64(rpcTimeout);
DECLARE_uint64(rpcRetryTimes);
DECLARE_string(snapshotCloneAddr);
DECLARE_string(snapshotCloneDummyPort);
DECLARE_uint64(chunkSize);

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
const char kSnapshotCloneStatusCmd[] = "snapshot-clone-status";

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

// 快照检查命令
const char kSnapshotCheckCmd[] = "snapshot-check";
// 调度模块命令
const char kRapidLeaderSchedule[] = "rapid-leader-schedule";

// curve文件meta相关的命令
const char kChunkMeta[] = "chunk-meta";
const char kSnapshotMeta[] = "snapshot-meta";

// raft log相关命令
const char kRaftLogMeta[] = "raft-log-meta";

const char kOldVersion[] = "before0.0.5.2";
const char kOffline[] = "offline";
const char kVars[] = "/vars/";
const char kConfValue[] = "conf_value";

// raft state 相关常量
const char kState[] = "state";
const char kStateLeader[] = "LEADER";
const char kStateFollower[] = "FOLLOWER";
const char kStateTransferring[] = "TRANSFERRING";
const char kStateCandidate[] = "CANDIDATE";
const char kLeader[] = "leader";
const char kGroupId[] = "groupId";
const char kPeers[] = "peers";
const char kReplicator[] = "replicator";
const char kStorage[] = "storage";
const char kSnapshot[] = "snapshot";
const char kNextIndex[] = "next_index";

const int kDefaultMdsDummyPort = 6667;

}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_CURVE_TOOL_DEFINE_H_
