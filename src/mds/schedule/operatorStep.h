/*
 * Project: curve
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef SRC_MDS_SCHEDULE_OPERATORSTEP_H_
#define SRC_MDS_SCHEDULE_OPERATORSTEP_H_

#include <cstdint>
#include <string>
#include "src/mds/schedule/topoAdapter.h"

namespace curve {
namespace mds {
namespace schedule {
enum ApplyStatus {
  Finished,
  Failed,
  Ordered,
  OnGoing
};

/**
 * @brief OperatorStep is used to abstract different operator step.
 * An Operator is composed of several OperatorStep.
 */
class OperatorStep {
 public:
  /**
   * @brief execute OperatorStep
   */
  virtual ApplyStatus Apply(const CopySetInfo &originInfo,
                            CopySetConf *newConf) = 0;
};

class TransferLeader : public OperatorStep {
 public:
  TransferLeader(ChunkServerIdType from, ChunkServerIdType to);

  /**
   * @brief 可能的场景如下：
   * 1. to_已经是leader,变更成功
   * 2. 上报的信息没有configchangeItem, 下发变更命令
   * 3. 上报的信息有configchangeItem, 但是和目的leader不匹配, 说明有正在执行的operator,可能由于
   * mds重启丢掉了, 此时应该直接让新的operator失败并移除
   * 4. 上报配置变更失败, transferleader失败并移除
   * 5. 正在配置变更过程中, 不做任何操作
   */
  ApplyStatus Apply(const CopySetInfo &originInfo,
                    CopySetConf *newConf) override;

 private:
  ChunkServerIdType from_;
  ChunkServerIdType to_;
};

class AddPeer : public OperatorStep {
 public:
  explicit AddPeer(ChunkServerIdType peerID);

  /**
   * @brief
   * 1. add_已经是replica中的一个,变更成功
   * 2. 上报的信息没有configchangeItem, 下发变更命令
   * 3. 上报的信息有configchangeItem, 但是和add_不匹配, 说明有正在执行的operator,可能由于
   * mds重启丢掉了, 此时应该直接让新的operator失败并移除
   * 4. 上报配置变更失败, addPeer失败并移除
   * 5. 正在配置变更过程中, 不做任何操作
   */
  ApplyStatus Apply(const CopySetInfo &originInfo,
                    CopySetConf *newConf) override;
  ChunkServerIdType GetTargetPeer() const;

 private:
  ChunkServerIdType add_;
};

class RemovePeer : public OperatorStep {
 public:
  explicit RemovePeer(ChunkServerIdType peerID);

  /**
   * @brief
   * 1. remove_已经不是replica中的一个,变更成功
   * 2. 上报的信息没有configchangeItem, 下发变更命令
   * 3. 上报的信息有candidate, 但是和remove_不匹配, 说明有正在执行的operator,可能由于
   * mds重启丢掉了, 此时应该直接让新的operator失败并移除
   * 4. 上报配置变更失败, removePeer失败并移除
   * 5. 正在配置变更过程中, 不做任何操作
   */
  ApplyStatus Apply(const CopySetInfo &originInfo,
                    CopySetConf *newConf) override;

 private:
  ChunkServerIdType remove_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_OPERATORSTEP_H_
