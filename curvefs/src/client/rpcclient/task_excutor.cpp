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

#include <butil/fast_rand.h>
#include <algorithm>
#include <utility>

#include "curvefs/src/client/rpcclient/task_excutor.h"
#include "curvefs/proto/metaserver.pb.h"

using ::curvefs::metaserver::MetaStatusCode;

namespace curvefs {
namespace client {
namespace rpcclient {

MetaStatusCode ConvertToMetaStatusCode(int retcode) {
    if (retcode < 0) {
        return MetaStatusCode::RPC_ERROR;
    }
    return static_cast<MetaStatusCode>(retcode);
}

int TaskExecutor::DoRPCTask() {
    task_->rpcTimeoutMs = opt_.rpcTimeoutMS;
    return DoRPCTaskInner(nullptr);
}

void TaskExecutor::DoAsyncRPCTask(TaskExecutorDone *done) {
    brpc::ClosureGuard done_guard(done);

    task_->rpcTimeoutMs = opt_.rpcTimeoutMS;
    int ret = DoRPCTaskInner(done);
    if (ret < 0) {
        done->SetRetCode(ret);
        return;
    }
    done_guard.release();
    return;
}

int TaskExecutor::DoRPCTaskInner(TaskExecutorDone *done) {
    int retCode = -1;
    bool needRetry = true;

    do {
        if (task_->retryTimes++ > opt_.maxRetry) {
            LOG(ERROR) << task_->TaskContextStr()
                       << " retry times exceeds the limit";
            break;
        }

        if (task_->refreshTxId && !metaCache_->RefreshTxId()) {
            LOG(ERROR) << "Refresh partition txid failed.";
            bthread_usleep(opt_.retryIntervalUS);
            continue;
        }

        if (!HasValidTarget() && !GetTarget()) {
            LOG(WARNING) << "get target fail for " << task_->TaskContextStr()
                         << ", sleep and retry";
            bthread_usleep(opt_.retryIntervalUS);
            continue;
        }

        std::shared_ptr<brpc::Channel> channel;
        if (!task_->streaming) {
            channel = channelManager_->GetOrCreateChannel(
                task_->target.metaServerID, task_->target.endPoint);
        } else {
            channel = channelManager_->GetOrCreateStreamChannel(
                task_->target.metaServerID, task_->target.endPoint);
        }

        if (!channel) {
            LOG(WARNING) << "GetOrCreateChannel fail for "
                         << task_->TaskContextStr() << ", sleep and retry";
            bthread_usleep(opt_.retryIntervalUS);
            continue;
        }

        retCode = ExcuteTask(channel.get(), done);
        needRetry = OnReturn(retCode);

        if (needRetry) {
            PreProcessBeforeRetry(retCode);
        }
    } while (needRetry);

    return retCode;
}

bool TaskExecutor::OnReturn(int retCode) {
    bool needRetry = false;

    // rpc fail
    if (retCode < 0) {
        needRetry = true;
        ResetChannelIfNotHealth();
        RefreshLeader();
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

        case MetaStatusCode::PARTITION_NOT_FOUND:
            // need refresh partition, may be deleted
            if (OnPartitionNotExist()) {
                needRetry = true;
            }
            break;

        case MetaStatusCode::PARTITION_ALLOC_ID_FAIL:
            // TODO(@lixiaocui @cw123): metaserver and mds heartbeat should
            // report this status
            needRetry = true;
            // need choose a new coopyset
            OnPartitionAllocIDFail();
            break;

        case MetaStatusCode::RPC_STREAM_ERROR:
            needRetry = true;
            break;

        default:
            break;
        }
    }

    return needRetry;
}

void TaskExecutor::ResetChannelIfNotHealth() {
    channelManager_->ResetSenderIfNotHealth(task_->target.metaServerID);
}

void TaskExecutor::PreProcessBeforeRetry(int retCode) {
    if (task_->retryTimes >= opt_.maxRetryTimesBeforeConsiderSuspend) {
        if (!task_->suspend) {
            task_->suspend = true;
            LOG(ERROR) << task_->TaskContextStr() << " retried "
                       << opt_.maxRetryTimesBeforeConsiderSuspend
                       << " times, set suspend flag! ";
        } else {
            LOG_IF(ERROR, 0 == task_->retryTimes %
                                   opt_.maxRetryTimesBeforeConsiderSuspend)
                << task_->TaskContextStr() << " retried " << task_->retryTimes
                << " times";
        }
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
        return;
    }

    // over load
    if (retCode == MetaStatusCode::OVERLOAD) {
        uint64_t nextsleeptime = OverLoadBackOff();
        LOG(WARNING) << "metaserver overload, sleep(us) = " << nextsleeptime
                     << ", " << task_->TaskContextStr();
        bthread_usleep(nextsleeptime);
        return;
    }

    if (!task_->retryDirectly) {
        bthread_usleep(opt_.retryIntervalUS);
    }
}

bool TaskExecutor::GetTarget() {
    if (!metaCache_->GetTarget(task_->fsID, task_->inodeID, &task_->target,
                               &task_->applyIndex)) {
        LOG(ERROR) << "fetch target for task fail, " << task_->TaskContextStr();
        return false;
    }
    return true;
}

int TaskExecutor::ExcuteTask(brpc::Channel *channel,
    TaskExecutorDone *done) {
    task_->cntl_.Reset();
    task_->cntl_.set_timeout_ms(task_->rpcTimeoutMs);
    return task_->rpctask(task_->target.groupID.poolID,
                          task_->target.groupID.copysetID,
                          task_->target.partitionID, task_->target.txId,
                          task_->applyIndex, channel, &task_->cntl_, done);
}

void TaskExecutor::OnSuccess() {}

void TaskExecutor::OnCopysetNotExist() { RefreshLeader(); }

bool TaskExecutor::OnPartitionNotExist() {
    LOG(ERROR) << "<<< fsid = " << task_->fsID;
    return metaCache_->ListPartitions(task_->fsID);
}

void TaskExecutor::OnReDirected() { RefreshLeader(); }

void TaskExecutor::RefreshLeader() {
    // refresh leader according to copyset
    MetaserverID oldTarget = task_->target.metaServerID;

    bool ok =
        metaCache_->GetTargetLeader(&task_->target, &task_->applyIndex, true);

    VLOG(3) << "refresh leader for {inodeid:" << task_->inodeID
            << ", pool:" << task_->target.groupID.poolID
            << ", copyset:" << task_->target.groupID.copysetID << "} "
            << (ok ? " success" : " failure");

    // if leader change, upper layer needs to retry
    task_->retryDirectly = (oldTarget != task_->target.metaServerID);
}

void TaskExecutor::OnPartitionAllocIDFail() {
    metaCache_->MarkPartitionUnavailable(task_->target.partitionID);
    task_->target.Reset();
}

uint64_t TaskExecutor::OverLoadBackOff() {
    uint64_t curpowTime = std::min(task_->retryTimes, maxOverloadPow_);

    uint64_t nextsleeptime = opt_.retryIntervalUS * (1 << curpowTime);

    // -10% ~ 10% jitter
    uint64_t random_time = butil::fast_rand() % (nextsleeptime / 5 + 1);
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

bool TaskExecutor::HasValidTarget() const { return task_->target.IsValid(); }

void TaskExecutor::SetRetryParam() {
    using curve::common::MaxPowerTimesLessEqualValue;

    uint64_t overloadTimes =
        opt_.maxRetrySleepIntervalUS / opt_.retryIntervalUS;

    maxOverloadPow_ = MaxPowerTimesLessEqualValue(overloadTimes);

    uint64_t timeoutTimes = opt_.maxRPCTimeoutMS / opt_.rpcTimeoutMS;
    maxTimeoutPow_ = MaxPowerTimesLessEqualValue(timeoutTimes);
}

void TaskExecutorDone::Run() {
    std::unique_ptr<TaskExecutorDone> self_guard(this);
    brpc::ClosureGuard done_guard(done_);

    bool needRetry = true;
    needRetry = excutor_->OnReturn(code_);
    if (needRetry) {
        excutor_->PreProcessBeforeRetry(code_);
        code_ = excutor_->DoRPCTaskInner(this);
        if (code_ < 0) {
            done_->SetMetaStatusCode(ConvertToMetaStatusCode(code_));
            return;
        }
        self_guard.release();
        done_guard.release();
    } else {
        done_->SetMetaStatusCode(ConvertToMetaStatusCode(code_));
    }
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
