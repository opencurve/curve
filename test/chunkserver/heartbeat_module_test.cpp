/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/23  Wenyu Zhou   Initial version
 */

#include "src/chunkserver/heartbeat.h"

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

TEST(HeartbeatTaskTest, NewTask) {
    HeartbeatTask   task;

    TASK_TYPE       type = curve::chunkserver::TASK_TYPE_NONE;
    PeerId          peerId;

    task.NewTask(type, peerId);
    ASSERT_EQ(type, task.GetType());
    ASSERT_EQ(peerId, task.GetPeerId());
}

TEST(HeartbeatTaskTest, GetSetStatus) {
    HeartbeatTask   task;

    TaskStatus      status(-1, "test message");

    task.SetStatus(status);
    ASSERT_EQ(status.error_code(), task.GetStatus().error_code());
    ASSERT_EQ(status.error_str(), task.GetStatus().error_str());
}

TEST(CopysetInfoTest, GetIDs) {
    LogicPoolID     poolId = 666;
    CopysetID       copysetId = 888;
    GroupNid        groupId = ToGroupNid(poolId, copysetId);

    CopysetInfo     info(poolId, copysetId);

    ASSERT_EQ(poolId, info.GetLogicPoolId());
    ASSERT_EQ(copysetId, info.GetCopysetId());
    ASSERT_EQ(groupId, info.GetGroupId());
}

TEST(CopysetInfoTest, GetSetTerm) {
    LogicPoolID     poolId = 666;
    CopysetID       copysetId = 888;

    CopysetInfo     info(poolId, copysetId);

    uint64_t        term;

    info.UpdateTerm(term);
    ASSERT_EQ(term, info.GetTerm());
}

TEST(CopysetInfoTest, CopysetNodeTest) {
    LogicPoolID     poolId = 666;
    CopysetID       copysetId = 888;
    Configuration   conf;

    CopysetNodePtr  copyset(new CopysetNode(poolId, copysetId, conf));
    CopysetInfo     info(poolId, copysetId);

    info.SetCopysetNode(copyset);
    ASSERT_EQ(copyset, info.GetCopysetNode());
    ASSERT_EQ(0, info.GetEpoch());

    int                 ret;
    PeerId              peerId;
    std::vector<PeerId> peers;

    scoped_refptr<braft::NodeImpl> node;

    ret = info.GetLeader(&peerId);
    ASSERT_NE(0, ret);
    ASSERT_EQ(true, peerId.is_empty());

    ret = info.GetNode(&node);
    ASSERT_NE(0, ret);
    ASSERT_EQ(nullptr, node.get());

    ret = info.ListPeers(&peers);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, peers.size());

    ASSERT_EQ(false, info.IsLeader());
}

TEST(CopysetInfoTest, TaskTest) {
    LogicPoolID     poolId = 666;
    CopysetID       copysetId = 888;

    CopysetInfo     info(poolId, copysetId);

    TaskStatus      status(-1, "test message");
    TASK_TYPE       type = curve::chunkserver::TASK_TYPE_NONE;
    PeerId          peerId;

    ASSERT_EQ(false, info.HasTask());
    ASSERT_EQ(false, info.IsTaskOngoing());

    info.NewTask(type, peerId);

    ASSERT_EQ(true, info.HasTask());
    ASSERT_EQ(true, info.IsTaskOngoing());
    ASSERT_EQ(peerId, info.GetPeerInTask());

    info.ReleaseTask();

    ASSERT_EQ(false, info.HasTask());
    ASSERT_EQ(true, info.IsTaskOngoing());

    info.FinishTask(status);

    ASSERT_EQ(false, info.HasTask());
    ASSERT_EQ(false, info.IsTaskOngoing());

    ASSERT_EQ(status.error_code(), info.GetStatus().error_code());
    ASSERT_EQ(status.error_str(), info.GetStatus().error_str());
}

}  // namespace chunkserver
}  // namespace curve
