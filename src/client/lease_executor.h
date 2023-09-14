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
 * File Created: Saturday, 23rd February 2019 1:41:23 pm
 * Author: tongguangxun
 */

#ifndef SRC_CLIENT_LEASE_EXECUTOR_H_
#define SRC_CLIENT_LEASE_EXECUTOR_H_

#include <brpc/periodic_task.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <memory>
#include <string>

#include "src/client/client_common.h"
#include "src/client/client_config.h"
#include "src/client/iomanager4file.h"
#include "src/client/mds_client.h"

namespace curve {
namespace client {

class RefreshSessionTask;

/**
 * Please refresh the result. If the session does not exist, there is no need to renew it
 * If the session exists but the lease renewal fails, continue to renew the contract
 * Successfully renewed the contract, FInfo_ Only in t will there be corresponding file information
 */
struct LeaseRefreshResult {
    enum class Status {
        OK,
        FAILED,
        NOT_EXIST
    };
    Status status;
    FInfo_t finfo;
};

class LeaseExecutorBase {
 public:
    virtual ~LeaseExecutorBase() = default;
    virtual bool RefreshLease() { return true; }
};

/**
 * The fileinstance corresponding to each vdisk will maintain heartbeat with the mds
 * Heartbeat is achieved through LeaseExecutor, which periodically
 * Go to MDS to renew the contract and bring back the latest version information of the current file on the MDS side
 * Then check if the version information has changed, and if so, notify the iomanager
 * Updated version. If the renewal fails, the user's newly sent IO needs to be returned in error
 */
class LeaseExecutor : public LeaseExecutorBase {
 public:
    /**
     * Constructor
     * @param: leaseopt is the option configuration for the current lease renewal
     * @param: mdsclient is a renewed client with mds
     * @param: iomanager will schedule IO in case of contract renewal failure or version change
     */
    LeaseExecutor(const LeaseOption& leaseOpt, const UserInfo& userinfo,
                  MDSClient* mdscllent, IOManager4File* iomanager);

    ~LeaseExecutor();

    /**
     * LeaseExecutor requires finfo to save the filename
     * LeaseSession_t is the execution configuration of the current leaeexcutor
     * @param: fi is the current version information of the file that needs to be renewed
     * @param: lease is the lease information for renewal
     * @return: Successfully returns true, otherwise returns false
     */
    bool Start(const FInfo_t& fi, const LeaseSession_t&  lease);

    /**
     *Stop Renewal
     */
    void Stop();

    /**
     *Notify iomanagerdisable io if the current lease renewal fails
     */
    bool LeaseValid();

    /**
     *Test use, active failure increases refresh failure
     */
    void InvalidLease() {
        for (uint32_t i = 0; i <= leaseoption_.mdsRefreshTimesPerLease; i++) {
            IncremRefreshFailed();
        }
    }

    /**
     * @brief Renew Task Executor
     * @return Do you want to continue executing the refresh session task
     */
    bool RefreshLease() override;

    /**
     * @brief test use, reset refresh session task
     */
    void ResetRefreshSessionTask();

 private:
    /**
     * During a lease period, rfreshTimesPerLease will be renewed times, increasing every time the renewal fails
     * When consecutive renewals of rfreshTimesPerLease fail times, disable IO
     */
    void IncremRefreshFailed();

    /**
     * @brief Updating local file information consistent with MDS record.
     *        Currently, only seqnum and file status are updated
     * @param fileInfo Latest file information returned by refresh session RPC
     */
    void CheckNeedUpdateFileInfo(const FInfo& fileInfo);

 private:
    // File name for lease renewal with mds
    std::string             fullFileName_;

    // client for renewal
    MDSClient*              mdsclient_;

    // User information used to initiate a expression
    UserInfo_t              userinfo_;

    // IO manager, calls its interface when a file needs to update version information or disable IO
    IOManager4File*         iomanager_;

    // Configuration information for the current lease execution
    LeaseOption           leaseoption_;

    // The lease information transmitted from the mds end, including the lease duration of the current file and the sessionid
    LeaseSession_t          leasesession_;

    // Record whether the current lease is available
    std::atomic<bool>       isleaseAvaliable_;

    // Record the current number of consecutive renewal failures
    std::atomic<uint64_t>   failedrefreshcount_;

    // refresh session scheduled tasks will be executed at fixed intervals
    std::unique_ptr<RefreshSessionTask> task_;
};

// RefreshSession Recurring Task
// Manage using brpc::PeriodicTaskManager
// Call OnTriggeringTask when the timer is triggered, and decide whether to continue timing triggering based on the return value
// If no longer triggered, call OnDestroyingTask for cleaning operation
class RefreshSessionTask : public brpc::PeriodicTask {
 public:
    using Task = std::function<bool(void)>;

    RefreshSessionTask(LeaseExecutorBase* leaseExecutor,
                       uint64_t intervalUs)
        : leaseExecutor_(leaseExecutor),
          refreshIntervalUs_(intervalUs),
          stopped_(false),
          stopMtx_(),
          terminated_(false),
          terminatedMtx_(),
          terminatedCv_() {}

    RefreshSessionTask(const RefreshSessionTask& other)
        : leaseExecutor_(other.leaseExecutor_),
          refreshIntervalUs_(other.refreshIntervalUs_),
          stopped_(false),
          stopMtx_(),
          terminated_(false),
          terminatedMtx_(),
          terminatedCv_() {}

    virtual ~RefreshSessionTask() = default;

    /**
     * @brief: Execute current function after timer timeout
     * @param next_abstime Absolute time for the next execution of the task
     * @return true Continue to regularly execute the current task
     *         false Stop executing the current task
     */
    bool OnTriggeringTask(timespec* next_abstime) override {
        std::lock_guard<bthread::Mutex> lk(stopMtx_);
        if (stopped_) {
            return false;
        }

        *next_abstime = butil::microseconds_from_now(refreshIntervalUs_);
        return leaseExecutor_->RefreshLease();
    }

    /**
     * @brief Stop executing the current task again
     */
    void Stop() {
        std::lock_guard<bthread::Mutex> lk(stopMtx_);
        stopped_ = true;
    }

    /**
     * @brief is called after the task stops
     */
    void OnDestroyingTask() override {
        std::unique_lock<bthread::Mutex> lk(terminatedMtx_);
        terminated_ = true;
        terminatedCv_.notify_one();
    }

    /**
     * @brief Wait for the task to exit
     */
    void WaitTaskExit() {
        std::unique_lock<bthread::Mutex> lk(terminatedMtx_);
        while (terminated_ != true) {
            terminatedCv_.wait(lk);
        }
    }

    /**
     * @brief Get refresh session time interval (us)
     * @return refresh session task time interval (us)
     */
    uint64_t RefreshIntervalUs() const {
        return refreshIntervalUs_;
    }

 private:
    LeaseExecutorBase* leaseExecutor_;
    uint64_t refreshIntervalUs_;

    bool stopped_;
    bthread::Mutex stopMtx_;

    bool terminated_;
    bthread::Mutex terminatedMtx_;
    bthread::ConditionVariable terminatedCv_;
};

}   // namespace client
}   // namespace curve

#endif  // SRC_CLIENT_LEASE_EXECUTOR_H_
