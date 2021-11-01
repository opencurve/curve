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
 * @Project: curve
 * @Date: 2021-11-8 11:01:48
 * @Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_
#define CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_

#include <cstdint>
#include <string>
#include "curvefs/src/mds/schedule/topoAdapter.h"

namespace curvefs {
namespace mds {
namespace schedule {
enum ApplyStatus {
    Finished,
    Failed,
    Ordered,
    OnGoing
};

// TODO(chenwei) : reuse curvebs code
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

    virtual MetaServerIdType GetTargetPeer() const = 0;
};

class TransferLeader : public OperatorStep {
 public:
    TransferLeader(MetaServerIdType from, MetaServerIdType to);

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

    MetaServerIdType GetTargetPeer() const override;

 private:
    MetaServerIdType from_;
    MetaServerIdType to_;
};

class AddPeer : public OperatorStep {
 public:
    explicit AddPeer(MetaServerIdType peerID);
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

    MetaServerIdType GetTargetPeer() const override;

    std::string OperatorStepToString() override;

 private:
    MetaServerIdType add_;
};

class RemovePeer : public OperatorStep {
 public:
    explicit RemovePeer(MetaServerIdType peerID);
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

    MetaServerIdType GetTargetPeer() const override;

 private:
    MetaServerIdType remove_;
};

class ChangePeer : public OperatorStep {
 public:
    ChangePeer(MetaServerIdType oldOne, MetaServerIdType newOne);

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

    MetaServerIdType GetTargetPeer() const override;

    MetaServerIdType GetOldPeer() const;

 private:
    MetaServerIdType old_;
    MetaServerIdType new_;
};

}  // namespace schedule
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_SCHEDULE_OPERATORSTEP_H_
