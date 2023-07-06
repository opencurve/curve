/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Created Date: 2023-06-09
 * Author: wanghai (SeanHai)
*/

#include <string>
#include <algorithm>
#include "src/client/rpc_excutor.h"
#include "src/client/auth_client.h"
#include "src/common/timeutility.h"

namespace curve {
namespace client {

using curve::common::TimeUtility;

// rpc发送和mds地址切换状态机
int RPCExcutorRetryPolicy::DoRPCTask(RPCFunc rpctask, uint64_t maxRetryTimeMS) {
    // 记录上一次正在服务的mds index
    int lastWorkingMDSIndex = currentWorkingMDSAddrIndex_;

    // 记录当前正在使用的mds index
    int curRetryMDSIndex = currentWorkingMDSAddrIndex_;

    // 记录当前mds重试的次数
    uint64_t currentMDSRetryCount = 0;

    // 执行起始时间点
    uint64_t startTime = TimeUtility::GetTimeofDayMs();

    // rpc超时时间
    uint64_t rpcTimeOutMS = retryOpt_.rpcTimeoutMs;

    // The count of normal retry
    uint64_t normalRetryCount = 0;

    int retcode = -1;
    bool retryUnlimit = (maxRetryTimeMS == 0);
    while (GoOnRetry(startTime, maxRetryTimeMS)) {
        // 1. 创建当前rpc需要使用的channel和controller，执行rpc任务
        retcode = ExcuteTask(curRetryMDSIndex, rpcTimeOutMS, rpctask);

        // 2. 根据rpc返回值进行预处理
        if (retcode < 0) {
            curRetryMDSIndex = PreProcessBeforeRetry(
                retcode, retryUnlimit, &normalRetryCount, &currentMDSRetryCount,
                curRetryMDSIndex, &lastWorkingMDSIndex, &rpcTimeOutMS);
            continue;
            // 3. 此时rpc是正常返回的，更新当前正在服务的mds地址index
        } else {
            currentWorkingMDSAddrIndex_.store(curRetryMDSIndex);
            break;
        }
    }

    return retcode;
}

bool RPCExcutorRetryPolicy::GoOnRetry(uint64_t startTimeMS,
                                      uint64_t maxRetryTimeMS) {
    if (maxRetryTimeMS == 0) {
        return true;
    }

    uint64_t currentTime = TimeUtility::GetTimeofDayMs();
    return currentTime - startTimeMS < maxRetryTimeMS;
}

int RPCExcutorRetryPolicy::PreProcessBeforeRetry(int status, bool retryUnlimit,
                                                 uint64_t *normalRetryCount,
                                                 uint64_t *curMDSRetryCount,
                                                 int curRetryMDSIndex,
                                                 int *lastWorkingMDSIndex,
                                                 uint64_t *timeOutMS) {
    int nextMDSIndex = 0;
    bool rpcTimeout = false;
    bool needChangeMDS = false;

    // If retryUnlimit is set, sleep a long time to retry no matter what the
    // error it is.
    if (retryUnlimit) {
        if (++(*normalRetryCount) >
            retryOpt_.normalRetryTimesBeforeTriggerWait) {
            bthread_usleep(retryOpt_.waitSleepMs * 1000);
        }

        // 1. 访问存在的IP地址，但无人监听：ECONNREFUSED
        // 2. 正常发送RPC情况下，对端进程挂掉了：EHOSTDOWN
        // 3. 对端server调用了Stop：ELOGOFF
        // 4. 对端链接已关闭：ECONNRESET
        // 5. 在一个mds节点上rpc失败超过限定次数
        // 在这几种场景下，主动切换mds。
    } else if (status == -EHOSTDOWN || status == -ECONNRESET ||
               status == -ECONNREFUSED || status == -brpc::ELOGOFF ||
               *curMDSRetryCount >= retryOpt_.maxFailedTimesBeforeChangeAddr) {
        needChangeMDS = true;

        // 在开启健康检查的情况下，在底层tcp连接失败时
        // rpc请求会本地直接返回 EHOSTDOWN
        // 这种情况下，增加一些睡眠时间，避免大量的重试请求占满bthread
        // TODO(wuhanqing): 关闭健康检查
        if (status == -EHOSTDOWN) {
            bthread_usleep(retryOpt_.rpcRetryIntervalUS);
        }
    } else if (status == -brpc::ERPCTIMEDOUT || status == -ETIMEDOUT) {
        rpcTimeout = true;
        needChangeMDS = false;
        // 触发超时指数退避
        *timeOutMS *= 2;
        *timeOutMS = std::min(*timeOutMS, retryOpt_.maxRPCTimeoutMS);
        *timeOutMS = std::max(*timeOutMS, retryOpt_.rpcTimeoutMs);
    }

    // 获取下一次需要重试的mds索引
    nextMDSIndex = GetNextMDSIndex(needChangeMDS, curRetryMDSIndex,
                                   lastWorkingMDSIndex);  // NOLINT

    // 更新curMDSRetryCount和rpctimeout
    if (nextMDSIndex != curRetryMDSIndex) {
        *curMDSRetryCount = 0;
        *timeOutMS = retryOpt_.rpcTimeoutMs;
    } else {
        ++(*curMDSRetryCount);
        // 还是在当前mds上重试，且rpc不是超时错误，就进行睡眠，然后再重试
        if (!rpcTimeout) {
            bthread_usleep(retryOpt_.rpcRetryIntervalUS);
        }
    }

    return nextMDSIndex;
}
/**
 * 根据输入状态获取下一次需要重试的mds索引，mds切换逻辑：
 * 记录三个状态：curRetryMDSIndex、lastWorkingMDSIndex、
 *             currentWorkingMDSIndex
 * 1. 开始的时候curRetryMDSIndex = currentWorkingMDSIndex
 *            lastWorkingMDSIndex = currentWorkingMDSIndex
 * 2. 如果rpc失败，会触发切换curRetryMDSIndex，如果这时候lastWorkingMDSIndex
 *    与currentWorkingMDSIndex相等，这时候会顺序切换到下一个mds索引，
 *    如果lastWorkingMDSIndex与currentWorkingMDSIndex不相等，那么
 *    说明有其他接口更新了currentWorkingMDSAddrIndex_，那么本次切换
 *    直接切换到currentWorkingMDSAddrIndex_
 */
int RPCExcutorRetryPolicy::GetNextMDSIndex(bool needChangeMDS,
                                           int currentRetryIndex,
                                           int *lastWorkingindex) {
    int nextMDSIndex = 0;
    if (std::atomic_compare_exchange_strong(
            &currentWorkingMDSAddrIndex_, lastWorkingindex,
            currentWorkingMDSAddrIndex_.load())) {
        int size = retryOpt_.addrs.size();
        nextMDSIndex =
            needChangeMDS ? (currentRetryIndex + 1) % size : currentRetryIndex;
    } else {
        nextMDSIndex = *lastWorkingindex;
    }

    return nextMDSIndex;
}

int RPCExcutorRetryPolicy::ExcuteTask(int mdsindex, uint64_t rpcTimeOutMS,
                                      RPCFunc task) {
    assert(mdsindex >= 0 &&
           mdsindex < static_cast<int>(retryOpt_.addrs.size()));

    const std::string &mdsaddr = retryOpt_.addrs[mdsindex];

    brpc::Channel channel;
    int ret = channel.Init(mdsaddr.c_str(), nullptr);
    if (ret != 0) {
        LOG(WARNING) << "Init channel failed! addr = " << mdsaddr;
        // 返回EHOSTDOWN给上层调用者，促使其切换mds
        return -EHOSTDOWN;
    }

    brpc::Controller cntl;
    cntl.set_log_id(GetLogId());
    cntl.set_timeout_ms(rpcTimeOutMS);

    return task(mdsindex, rpcTimeOutMS, &channel, &cntl);
}

}  // namespace client
}  // namespace curve
