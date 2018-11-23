/*
 * Project: curve
 * Created Date: Wed Nov 28 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <cstdint>
#include <vector>
#include "src/mds/schedule/operator.h"
#include "src/mds/schedule/topoAdapter.h"
#include "src/mds/schedule/operatorStep.h"

#ifndef CURVE_SRC_MDS_SCHEDULE_OPERATOR_FACTORY_H_
#define CURVE_SRC_MDS_SCHEDULE_OPERATOR_FACTORY_H_
namespace curve {
namespace mds {
namespace schedule {

static class OperatorFactory {
 public:
  /**
   * @brief generate operator to transfer leader
   */
  Operator CreateTransferLeaderOperator(const CopySetInfo &info,
                                        ChunkServerIdType newLeader,
                                        OperatorPriority pri);

  /**
   * @brief  generate operator to safely remove peer
   */
  Operator CreateRemovePeerOperator(
      const CopySetInfo &info, ChunkServerIdType rmPeer, OperatorPriority pri);

  /**
   * @brief generate operator to add peer
   */
  Operator CreateAddPeerOperator(
      const CopySetInfo &info, ChunkServerIdType addPeer, OperatorPriority pri);
} operatorFactory;

}  // namespace schedule
}  // namespace mds
}  // namespace curve
#endif  // CURVE_SRC_MDS_SCHEDULE_OPERATOR_FACTORY_H_
