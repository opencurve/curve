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
#ifndef SRC_MDS_SCHEDULE_OPERATORSTEPTEMPLATE_H_
#define SRC_MDS_SCHEDULE_OPERATORSTEPTEMPLATE_H_

#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "proto/heartbeat.pb.h"

namespace curve {
namespace mds {
namespace schedule {
using ::curve::mds::heartbeat::ConfigChangeType;

enum ApplyStatus { Finished, Failed, Ordered, OnGoing };

/**
 * @brief OperatorStepT is used to abstract different operator step.
 * An Operator is composed of several OperatorStepT.
 */
/**
 * IdType: MetaserverIdTpye or IdType
 */
template <class IdType, class CopySetInfoT, class CopySetConfT>
class OperatorStepT {
 public:
    virtual ~OperatorStepT() {}
    /**
     * @brief execute OperatorStepT
     */
    virtual ApplyStatus Apply(const CopySetInfoT &originInfo,
                              CopySetConfT *newConf) = 0;
    virtual std::string OperatorStepToString() = 0;

    virtual IdType GetTargetPeer() const = 0;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
class TransferLeaderT
    : public OperatorStepT<IdType, CopySetInfoT, CopySetConfT> {
 public:
    TransferLeaderT(IdType from, IdType to);

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
    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newConf) override;

    std::string OperatorStepToString() override;

    IdType GetTargetPeer() const override;

 private:
    IdType from_;
    IdType to_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
class AddPeerT : public OperatorStepT<IdType, CopySetInfoT, CopySetConfT> {
 public:
    explicit AddPeerT(IdType peerID);
    /**
     * @brief possible scenario and reaction:
     * 1. add_ is already one of the replica, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has configchangeItem but doesn't match add_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, AddPeerT
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newConf) override;

    IdType GetTargetPeer() const override;

    std::string OperatorStepToString() override;

 private:
    IdType add_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
class RemovePeerT : public OperatorStepT<IdType, CopySetInfoT, CopySetConfT> {
 public:
    explicit RemovePeerT(IdType peerID);
    /**
     * @brief possible scenario and reaction:
     * 1. remove_ is not one of the replica, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has candidate but doesn't match remove_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, RemovePeerT
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newConf) override;

    std::string OperatorStepToString() override;

    IdType GetTargetPeer() const override;

 private:
    IdType remove_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
class ChangePeerT : public OperatorStepT<IdType, CopySetInfoT, CopySetConfT> {
 public:
    ChangePeerT(IdType oldOne, IdType newOne);

    /**
     * @brief possible scenario and reaction:
     * 1. new_ is one of the replica, and old_ is not, changed successfully
     * 2. the info reported has no configchangeItem, dispatch change command
     * 3. the info reported has candidate but doesn't match new_.
     *    this means there's operator under execution lost due to the
     *    MDS restart, the new operator should suspend and remove in this case.
     * 4. configuration change fail reported, ChangePeerT
     *    failed and should be removed
     * 5. configuration change undergoing, do nothing
     */
    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newConf) override;

    std::string OperatorStepToString() override;

    IdType GetTargetPeer() const override;

    IdType GetOldPeer() const;

 private:
    IdType old_;
    IdType new_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
class ScanPeerT
    : public OperatorStepT<IdType, CopySetInfoT, CopySetConfT> {
 public:
    ScanPeerT(IdType peerID, ConfigChangeType opType)
        : scan_(peerID), opType_(opType) {}

    ApplyStatus Apply(const CopySetInfoT &originInfo,
                      CopySetConfT *newConf) override;

    std::string OperatorStepToString() override;

    IdType GetTargetPeer() const override;

    bool IsStartScanOp() const {
        return opType_ == ConfigChangeType::START_SCAN_PEER;
    }

    bool IsCancelScanOp() const {
        return opType_ == ConfigChangeType::CANCEL_SCAN_PEER;
    }

 private:
    IdType scan_;
    ConfigChangeType opType_;
};

template <class IdType, class CopySetInfoT, class CopySetConfT>
TransferLeaderT<IdType, CopySetInfoT, CopySetConfT>::TransferLeaderT(
    IdType from, IdType to) {
    this->from_ = from;
    this->to_ = to;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus TransferLeaderT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newConf) {
    assert(newConf != nullptr);

    // success transfer, no instruction to copyset
    if (originInfo.leader == to_) {
        return ApplyStatus::Finished;
    }

    // report leader is not to_ or from_
    // maybe leader election happen before or between transferring,
    // operator should be set failed and removed
    if (originInfo.leader != to_ && originInfo.leader != from_) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                     << " apply transfer leader from " << this->from_ << " to "
                     << this->to_ << " failed, current leader is "
                     << originInfo.leader;
        return ApplyStatus::Failed;
    }

    // not finish and no candidate, instruct copyset to do transfer_leader
    if (!originInfo.configChangeInfo.IsInitialized()) {
        newConf->id.first = originInfo.id.first;
        newConf->id.second = originInfo.id.second;
        newConf->epoch = originInfo.epoch;
        newConf->peers = originInfo.peers;
        newConf->type = ConfigChangeType::TRANSFER_LEADER;
        newConf->configChangeItem = this->to_;
        return ApplyStatus::Ordered;
    }

    // has candidate, but configChange item not match,
    // may be mds reboot and drop the operator, but old operator is ongoing,
    // so the new generated operator must be failed and removed
    if (originInfo.candidatePeerInfo.id != to_) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                     << " apply transfer leader from " << this->from_ << " to "
                     << this->to_
                     << " failed, config change item do not match, "
                        "report candidatePeerId is "
                     << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() !=
        ConfigChangeType::TRANSFER_LEADER) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                     << " apply transfer leader from " << this->from_ << " to "
                     << this->to_
                     << " failed, config change type do not match, "
                        "report type is "
                     << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail transfer, no instruction and warning,
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr()
                   << " apply transfer leader from " << this->from_ << " to "
                   << this->to_ << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // transfer not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string
TransferLeaderT<IdType, CopySetInfoT, CopySetConfT>::OperatorStepToString() {
    return "transfer leader from " + std::to_string(from_) + " to " +
           std::to_string(to_);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType TransferLeaderT<IdType, CopySetInfoT, CopySetConfT>::GetTargetPeer()
    const {
    return to_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
AddPeerT<IdType, CopySetInfoT, CopySetConfT>::AddPeerT(IdType peerID) {
    this->add_ = peerID;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType AddPeerT<IdType, CopySetInfoT, CopySetConfT>::GetTargetPeer() const {
    return add_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus AddPeerT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newConf) {
    assert(newConf != nullptr);

    // success add peer, no instruction to copyset
    if (originInfo.ContainPeer(this->add_)) {
        return ApplyStatus::Finished;
    }

    // not finish and no candidate, instruct copyset to add_peer
    if (!originInfo.configChangeInfo.IsInitialized()) {
        newConf->id.first = originInfo.id.first;
        newConf->id.second = originInfo.id.second;
        newConf->epoch = originInfo.epoch;
        newConf->peers = originInfo.peers;
        newConf->type = ConfigChangeType::ADD_PEER;
        newConf->configChangeItem = this->add_;
        return ApplyStatus::Ordered;
    }

    // has candidate,but configChange item not match,
    // may be mds reboot and drop the operator, but old operator is ongoing,
    // so the new generated operator must be failed and removed
    if (originInfo.candidatePeerInfo.id != add_) {
        LOG(WARNING) << originInfo.CopySetInfoStr() << " apply add peer "
                     << this->add_
                     << " failed, config change item do not match, "
                        "report candidatePeerId is "
                     << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() != ConfigChangeType::ADD_PEER) {
        LOG(WARNING) << originInfo.CopySetInfoStr() << " apply add peer "
                     << this->add_
                     << " failed, config change type do not match, "
                        "report type is "
                     << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail add, no instruction and warning
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr() << " apply add peer "
                   << this->add_ << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // add not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string
AddPeerT<IdType, CopySetInfoT, CopySetConfT>::OperatorStepToString() {
    return "add peer " + std::to_string(add_);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
RemovePeerT<IdType, CopySetInfoT, CopySetConfT>::RemovePeerT(IdType peerID) {
    this->remove_ = peerID;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus RemovePeerT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newConf) {
    assert(newConf != nullptr);

    // success remove peer, no instruction to copyset
    if (!originInfo.ContainPeer(this->remove_)) {
        return ApplyStatus::Finished;
    }

    // not finish and no candidate, instruct copyset to remove peer
    if (!originInfo.configChangeInfo.IsInitialized()) {
        newConf->id.first = originInfo.id.first;
        newConf->id.second = originInfo.id.second;
        newConf->epoch = originInfo.epoch;
        newConf->peers = originInfo.peers;
        newConf->type = ConfigChangeType::REMOVE_PEER;
        newConf->configChangeItem = this->remove_;
        return ApplyStatus::Ordered;
    }

    // configuration change item do not match,
    // may be mds reboot and drop the operator, but old operator is ongoing,
    // so the new generated operator must be failed and removed
    if (originInfo.candidatePeerInfo.id != remove_) {
        LOG(WARNING) << originInfo.CopySetInfoStr() << " apply remove peer "
                     << this->remove_
                     << " failed, config change item do not match, "
                        "report candidatePeerId is "
                     << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() != ConfigChangeType::REMOVE_PEER) {
        LOG(WARNING) << originInfo.CopySetInfoStr() << " apply remove peer "
                     << this->remove_
                     << " failed, config change type do not match, "
                        "report type is "
                     << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail remove, no instruction and warning
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr() << " apply remove peer "
                   << this->remove_ << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // remove not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string
RemovePeerT<IdType, CopySetInfoT, CopySetConfT>::OperatorStepToString() {
    return "remove peer " + std::to_string(remove_);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType RemovePeerT<IdType, CopySetInfoT, CopySetConfT>::GetTargetPeer() const {
    return remove_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ChangePeerT<IdType, CopySetInfoT, CopySetConfT>::ChangePeerT(IdType oldOne,
                                                             IdType newOne) {
    old_ = oldOne;
    new_ = newOne;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string
ChangePeerT<IdType, CopySetInfoT, CopySetConfT>::OperatorStepToString() {
    return "change peer from " + std::to_string(old_) + " to " +
           std::to_string(new_);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType ChangePeerT<IdType, CopySetInfoT, CopySetConfT>::GetTargetPeer() const {
    return new_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType ChangePeerT<IdType, CopySetInfoT, CopySetConfT>::GetOldPeer() const {
    return old_;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus ChangePeerT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newConf) {
    assert(newConf != nullptr);

    // if new_ is contained in origin info, the configuration change succeeded
    if (originInfo.ContainPeer(new_) && !originInfo.ContainPeer(old_)) {
        return ApplyStatus::Finished;
    }

    // if there isn't any candidate info, dispatch the operator for copyset to
    // change peer
    if (!originInfo.configChangeInfo.IsInitialized()) {
        newConf->id.first = originInfo.id.first;
        newConf->id.second = originInfo.id.second;
        newConf->epoch = originInfo.epoch;
        newConf->peers = originInfo.peers;
        newConf->type = ConfigChangeType::CHANGE_PEER;
        newConf->configChangeItem = new_;
        newConf->oldOne = old_;
        return ApplyStatus::Ordered;
    }

    // the info reported has candidate but doesn't match new_.
    // this means there's operator under execution lost due to the
    // MDS restart, the new operator should suspend and remove in this case.
    if (originInfo.candidatePeerInfo.id != new_) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                     << " apply change peer from " << old_ << " to " << new_
                     << " failed, config change item do not match, "
                        "report candidatePeerId is "
                     << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    // config change type reported is different from the record in MDS
    if (originInfo.configChangeInfo.type() != ConfigChangeType::CHANGE_PEER) {
        LOG(WARNING)
            << originInfo.CopySetInfoStr() << " apply change peer from " << old_
            << " to " << new_
            << " failed, config change type do not match, report type is "
            << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // configuration change fail reported, ChangePeerT failed and should be
    // removed //NOLINT
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr() << " apply change peer from "
                   << old_ << " to " << new_ << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // configuration change undergoing, do nothing
    return ApplyStatus::OnGoing;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
ApplyStatus ScanPeerT<IdType, CopySetInfoT, CopySetConfT>::Apply(
    const CopySetInfoT &originInfo, CopySetConfT *newConf) {
    // (1) copyset success on start/cancel scan
    if ((IsStartScanOp() && originInfo.scaning) ||
        (IsCancelScanOp() && !originInfo.scaning)) {
        return ApplyStatus::Finished;
    }

    // (2) copyset has no config change, instruct it to start/cancel scan
    if (!originInfo.configChangeInfo.IsInitialized()) {
        newConf->id.first = originInfo.id.first;
        newConf->id.second = originInfo.id.second;
        newConf->epoch = originInfo.epoch;
        newConf->peers = originInfo.peers;
        newConf->type = opType_;
        newConf->configChangeItem = scan_;
        return ApplyStatus::Ordered;
    }

    // (3) copyset current config change doesn't match the new config change,
    //     drop the new config change
    auto configChangeCsId = originInfo.candidatePeerInfo.id;
    auto configChangeType = originInfo.configChangeInfo.type();
    if (configChangeCsId != scan_ || configChangeType != opType_) {
        LOG(WARNING)
            << originInfo.CopySetInfoStr() << " " << OperatorStepToString()
            << " failed, report config change does't match the new one: "
            << "report candidate id is " << configChangeCsId
            << ", the new is " << scan_
            << "; report change type is " << configChangeType
            << ", the new is " << opType_;
        return ApplyStatus::Failed;
    }

    // (4) there is an error on starting/canceling scan, drop the config change
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr() << " "
                   << OperatorStepToString() << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // (5) copyset is on starting/canceling scan
    return ApplyStatus::OnGoing;
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
std::string
ScanPeerT<IdType, CopySetInfoT, CopySetConfT>::OperatorStepToString() {
    std::string opstr = "unknown operator";
    if (IsStartScanOp()) {
        opstr = "start scan";
    } else if (IsCancelScanOp()) {
        opstr = "cancel scan";
    }

    return opstr + " on " + std::to_string(scan_);
}

template <class IdType, class CopySetInfoT, class CopySetConfT>
IdType ScanPeerT<IdType, CopySetInfoT, CopySetConfT>::GetTargetPeer() const {
    return scan_;
}

}  // namespace schedule
}  // namespace mds
}  // namespace curve

#endif  // SRC_MDS_SCHEDULE_OPERATORSTEPTEMPLATE_H_
