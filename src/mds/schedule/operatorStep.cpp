/*
 * Project: curve
 * Created Date: Mon Nov 17 2018
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#include <glog/logging.h>
#include "src/mds/schedule/operatorStep.h"

using curve::mds::heartbeat::ConfigChangeType;

namespace curve {
namespace mds {
namespace schedule {
TransferLeader::TransferLeader(ChunkServerIdType from, ChunkServerIdType to) {
    this->from_ = from;
    this->to_ = to;
}

ApplyStatus TransferLeader::Apply(const CopySetInfo &originInfo,
                                  CopySetConf *newConf) {
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
                   << " apply transfer leader from "
                   << this->from_ << " to " << this->to_
                   << " failed, current leader is " << originInfo.leader;
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
                   << " apply transfer leader from "
                   << this->from_ << " to " << this->to_
                   << " failed, config change item do not match, "
                      "report candidatePeerId is "
                   << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() !=
        ConfigChangeType::TRANSFER_LEADER) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                   << " apply transfer leader from " << this->from_
                   << " to " << this->to_
                   << " failed, config change type do not match, "
                      "report type is " << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail transfer, no instruction and warning,
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr()
                   << " apply transfer leader from "
                   << this->from_ << " to " << this->to_
                   << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // transfer not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

std::string TransferLeader::OperatorStepToString() {
    return "transfer leader from " + std::to_string(from_) +
           " to " + std::to_string(to_);
}

ChunkServerIdType TransferLeader::GetTargetPeer() const {
    return to_;
}


AddPeer::AddPeer(ChunkServerIdType peerID) {
    this->add_ = peerID;
}

ChunkServerIdType AddPeer::GetTargetPeer() const {
    return add_;
}

ApplyStatus AddPeer::Apply(const CopySetInfo &originInfo,
                           CopySetConf *newConf) {
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
        LOG(WARNING) << originInfo.CopySetInfoStr()
                   << " apply add peer " << this->add_
                   << " failed, config change item do not match, "
                      "report candidatePeerId is "
                   << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() != ConfigChangeType::ADD_PEER) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                   << " apply add peer " << this->add_
                   << " failed, config change type do not match, "
                      "report type is " << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail add, no instruction and warning
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr()
                   << " apply add peer " << this->add_
                   << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // add not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

std::string AddPeer::OperatorStepToString() {
    return "add peer " + std::to_string(add_);
}

RemovePeer::RemovePeer(ChunkServerIdType peerID) {
    this->remove_ = peerID;
}

ApplyStatus RemovePeer::Apply(const CopySetInfo &originInfo,
                              CopySetConf *newConf) {
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

    // config change item do not match,
    // may be mds reboot and drop the operator, but old operator is ongoing,
    // so the new generated operator must be failed and removed
    if (originInfo.candidatePeerInfo.id != remove_) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                   << " apply remove peer " << this->remove_
                   << " failed, config change item do not match, "
                      "report candidatePeerId is "
                   << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    if (originInfo.configChangeInfo.type() != ConfigChangeType::REMOVE_PEER) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
                   << " apply remove peer " << this->remove_
                   << " failed, config change type do not match, "
                      "report type is "
                   << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // fail remove, no instruction and warning
    // operator failed and be removed
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr()
                   << " apply remove peer " << this->remove_
                   << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // remove not complete, no instruction to copyset
    return ApplyStatus::OnGoing;
}

std::string RemovePeer::OperatorStepToString() {
    return "remove peer " + std::to_string(remove_);
}

ChunkServerIdType RemovePeer::GetTargetPeer() const {
    return remove_;
}

ChangePeer::ChangePeer(ChunkServerIdType oldOne, ChunkServerIdType newOne) {
    old_ = oldOne;
    new_ = newOne;
}

std::string ChangePeer::OperatorStepToString() {
    return "change peer from " + std::to_string(old_)
        + " to " + std::to_string(new_);
}

ChunkServerIdType ChangePeer::GetTargetPeer() const {
    return new_;
}

ChunkServerIdType ChangePeer::GetOldPeer() const {
    return old_;
}

ApplyStatus ChangePeer::Apply(
    const CopySetInfo &originInfo, CopySetConf *newConf) {
    assert(newConf != nullptr);

    // 如果info中已经包含new_, 说明变更成功
    if (originInfo.ContainPeer(new_) &&
        !originInfo.ContainPeer(old_)) {
        return ApplyStatus::Finished;
    }

    // 如果没有candidate信息，下发该operator
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

    // 如果上报上来的configchange item与记录的不符合，则移除
    if (originInfo.candidatePeerInfo.id != new_) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
            << " apply change peer from " << old_ << " to " << new_
            << " failed, config change item do not match, "
                        "report candidatePeerId is "
            << originInfo.candidatePeerInfo.id;
        return ApplyStatus::Failed;
    }

    // 上报上来的变更类型与mds中记录的不同
    if (originInfo.configChangeInfo.type() !=  ConfigChangeType::CHANGE_PEER) {
        LOG(WARNING) << originInfo.CopySetInfoStr()
            << " apply change peer from " << old_ << " to " << new_
            << " failed, config change type do not match, report type is "
            << originInfo.configChangeInfo.type();
        return ApplyStatus::Failed;
    }

    // 上报失败信息
    if (!originInfo.configChangeInfo.finished() &&
        originInfo.configChangeInfo.has_err()) {
        LOG(ERROR) << originInfo.CopySetInfoStr()
                   << " apply change peer from " << old_ << " to " << new_
                   << " failed, report err: "
                   << originInfo.configChangeInfo.err().errmsg();
        return ApplyStatus::Failed;
    }

    // 变更未完成，继续
    return ApplyStatus::OnGoing;
}
}  // namespace schedule
}  // namespace mds
}  // namespace curve

