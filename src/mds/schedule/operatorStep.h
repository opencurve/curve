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
 * Created Date: Thu Nov 15 2018
 * Author: lixiaocui
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
    virtual std::string OperatorStepToString() = 0;

    virtual ChunkServerIdType GetTargetPeer() const = 0;
};

class TransferLeader : public OperatorStep {
 public:
    TransferLeader(ChunkServerIdType from, ChunkServerIdType to);

    /**
     * @brief possible scenario and reaction:
     * 1. to_ is already the leader, the operation succeeded
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has configchangeItem but doesn't match the target
     *    leader. this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, transferleader
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfo &originInfo,
                        CopySetConf *newConf) override;

    std::string OperatorStepToString() override;

    ChunkServerIdType GetTargetPeer() const override;

 private:
    ChunkServerIdType from_;
    ChunkServerIdType to_;
};

class AddPeer : public OperatorStep {
 public:
    explicit AddPeer(ChunkServerIdType peerID);
    /**
     * @brief possible scenario and reaction:
     * 1. add_ is already one of the replica, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has configchangeItem but doesn't match add_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, AddPeer
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfo &originInfo,
                        CopySetConf *newConf) override;

    ChunkServerIdType GetTargetPeer() const override;

    std::string OperatorStepToString() override;

 private:
    ChunkServerIdType add_;
};

class RemovePeer : public OperatorStep {
 public:
    explicit RemovePeer(ChunkServerIdType peerID);
    /**
     * @brief possible scenario and reaction:
     * 1. remove_ is not one of the replica, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has candidate but doesn't match remove_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, RemovePeer
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfo &originInfo,
                        CopySetConf *newConf) override;

    std::string OperatorStepToString() override;

    ChunkServerIdType GetTargetPeer() const override;

 private:
    ChunkServerIdType remove_;
};

class ChangePeer : public OperatorStep {
 public:
    ChangePeer(ChunkServerIdType oldOne, ChunkServerIdType newOne);

    /**
     * @brief possible scenario and reaction:
     * 1. new_ is one of the replica, and old_ is not, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has candidate but doesn't match new_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, ChangePeer
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(
        const CopySetInfo &originInfo, CopySetConf *newConf) override;

    std::string OperatorStepToString() override;

    ChunkServerIdType GetTargetPeer() const override;

    ChunkServerIdType GetOldPeer() const;

 private:
    ChunkServerIdType old_;
    ChunkServerIdType new_;
};

class ScanPeer : public OperatorStep {
 public:
    explicit ScanPeer(ChunkServerIdType peerID, ConfigChangeType opType)
        : scan_(peerID), opType_(opType) {}

    ApplyStatus Apply(const CopySetInfo& originInfo,
                      CopySetConf* newConf) override;

    std::string OperatorStepToString() override;

    ChunkServerIdType GetTargetPeer() const override;

    bool IsStartScanOp() {
        return opType_ == ConfigChangeType::START_SCAN_PEER;
    }

    bool IsCancelScanOp() {
        return opType_ == ConfigChangeType::CANCEL_SCAN_PEER;
    }

 private:
    ChunkServerIdType scan_;
    ConfigChangeType opType_;
};
}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_OPERATORSTEP_H_
