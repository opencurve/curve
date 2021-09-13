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
 * Project: curve
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#include <algorithm>
#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "curvefs/proto/metaserver.pb.h"

using ::curvefs::metaserver::MetaStatusCode;

namespace curvefs {
namespace client {
namespace rpcclient {
int TaskExecutor::DoRPCTask(std::shared_ptr<TaskContext> task) {
    task_ = task;
    task_->rpcTimeoutMs = opt_.rpcTimeoutMS;

    int retCode = -1;
    bool needRetry = true;

    do {
        retCode = ExcuteTask();

        needRetry = OnReturn(retCode);
        if (needRetry) {
            task_->retryTimes++;
            needRetry = PreProcessBeforeRetry(retCode);
        }
    } while (needRetry);

    return retCode;
}

bool TaskExecutor::OnReturn(int retCode) {
    bool needRetry = false;

    // rpc fail or get target fail
    if (retCode < 0) {
        needRetry = true;
    } else {
        switch (retCode) {
        case MetaStatusCode::OK:
            break;

        case MetaStatusCode::OVERLOAD:
            needRetry = true;
            break;

        case MetaStatusCode::REDIRECTED:
            needRetry = true;
            // need get refresh leader
            OnReDirected();
            break;

        case MetaStatusCode::COPYSET_NOTEXIST:
            needRetry = true;
            // need refresh leader
            OnCopysetNotExist();
            break;

        default:
            break;
        }
    }

    return needRetry;
}

bool TaskExecutor::PreProcessBeforeRetry(int retCode) {
    if (task_->retryTimes >= opt_.maxRetry) {
        LOG(INFO) << "retry exceed maxRetry: " << opt_.maxRetry;
        return false;
    }

    if (!task_->suspend &&
        task_->retryTimes >= opt_.maxRetryTimesBeforeConsiderSuspend) {
        task_->suspend = true;
        LOG(ERROR) << task_->TaskContextStr() << " retried "
                   << opt_.maxRetryTimesBeforeConsiderSuspend
                   << " times, set suspend flag! ";
    }

    if (retCode == -brpc::ERPCTIMEDOUT || retCode == -ETIMEDOUT) {
        uint64_t nextTimeout = 0;
        uint64_t retriedTimes = task_->retryTimes;
        bool leaderMayChange =
            metaCache_->IsLeaderMayChange(task_->target.groupID);

        if (retriedTimes < opt_.minRetryTimesForceTimeoutBackoff &&
            leaderMayChange) {
            nextTimeout = opt_.rpcTimeoutMS;
        } else {
            nextTimeout = TimeoutBackOff();
        }

        task_->rpcTimeoutMs = nextTimeout;
        LOG(WARNING) << "rpc timeout, next timeout = " << nextTimeout
                     << task_->TaskContextStr();
        return true;
    }

    // over load
    if (retCode == MetaStatusCode::OVERLOAD) {
        uint64_t nextsleeptime = OverLoadBackOff();
        LOG(WARNING) << "metaserver overload, sleep(us) = " << nextsleeptime
                     << ", " << task_->TaskContextStr();
        bthread_usleep(nextsleeptime);
        return true;
    }

    if (!task_->retryDirectly) {
        bthread_usleep(opt_.retryIntervalUS);
    }
    return true;
}

bool TaskExecutor::GetTarget() {
    if (!metaCache_->GetTarget(task_->fsID, task_->inodeID, &task_->target,
                               &task_->applyIndex)) {
        LOG(ERROR) << "fetch target for task fail, " << task_->TaskContextStr();
        return false;
    }
    return true;
}

int TaskExecutor::ExcuteTask() {
    // fetch target
    if (!GetTarget()) {
        return -1;
    }

    // get channel and send rpc
    auto channel = channelManager_->GetOrCreateChannel(
        task_->target.metaServerID, task_->target.endPoint);
    if (channel == nullptr) {
        return -EHOSTDOWN;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(task_->rpcTimeoutMs);
    return task_->rpctask(task_->target.groupID.poolID,
                          task_->target.groupID.copysetID,
                          task_->target.partitionID,
                          task_->target.txId,
                          task_->applyIndex,
                          channel.get(),
                          &cntl);
}
void TaskExecutor::OnSuccess() {}

void TaskExecutor::OnCopysetNotExist() { RefreshLeader(); }

void TaskExecutor::OnReDirected() { RefreshLeader(); }

void TaskExecutor::RefreshLeader() {
    MetaserverID oldTarget = task_->target.metaServerID;
    bool ok = metaCache_->GetTarget(task_->fsID, task_->inodeID, &task_->target,
                                    &task_->applyIndex, true);
    LOG_IF(WARNING, !ok) << "Refresh leader failed, task:"
                         << task_->TaskContextStr();
    task_->retryDirectly = (oldTarget != task_->target.metaServerID);
}


uint64_t TaskExecutor::OverLoadBackOff() {
    uint64_t curpowTime = std::min(task_->retryTimes, maxOverloadPow_);

    uint64_t nextsleeptime = opt_.retryIntervalUS * (1 << curpowTime);

    // -10% ~ 10% jitter
    uint64_t random_time = std::rand() % (nextsleeptime / 5 + 1);
    random_time -= nextsleeptime / 10;
    nextsleeptime += random_time;

    nextsleeptime = std::min(nextsleeptime, opt_.maxRetrySleepIntervalUS);
    nextsleeptime = std::max(nextsleeptime, opt_.retryIntervalUS);

    return nextsleeptime;
}

uint64_t TaskExecutor::TimeoutBackOff() {
    uint64_t curpowTime = std::min(task_->retryTimes, maxTimeoutPow_);

    uint64_t nextTimeout = opt_.rpcTimeoutMS * (1 << curpowTime);

    nextTimeout = std::min(nextTimeout, opt_.maxRPCTimeoutMS);
    nextTimeout = std::max(nextTimeout, opt_.rpcTimeoutMS);

    return nextTimeout;
}

void TaskExecutor::SetRetryParam() {
    using curve::common::MaxPowerTimesLessEqualValue;

    uint64_t overloadTimes =
        opt_.maxRetrySleepIntervalUS / opt_.retryIntervalUS;

    maxOverloadPow_ = MaxPowerTimesLessEqualValue(overloadTimes);

    uint64_t timeoutTimes = opt_.maxRPCTimeoutMS / opt_.rpcTimeoutMS;
    maxTimeoutPow_ = MaxPowerTimesLessEqualValue(timeoutTimes);
}


bool CreateInodeExcutor::GetTarget() {
    if (!metaCache_->SelectTarget(task_->fsID, &task_->target,
                                  &task_->applyIndex)) {
        LOG(ERROR) << "select target for task fail, "
                   << task_->TaskContextStr();
        return false;
    }
    return true;
}
}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs
