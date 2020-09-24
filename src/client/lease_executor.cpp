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
 * File Created: Saturday, 23rd February 2019 1:41:31 pm
 * Author: tongguangxun
 */
#include <glog/logging.h>

#include "src/common/timeutility.h"
#include "src/client/lease_executor.h"
#include "src/client/service_helper.h"

using curve::common::TimeUtility;

namespace curve {
namespace client {
LeaseExecutor::LeaseExecutor(const LeaseOption& leaseOpt,
                           UserInfo_t userinfo,
                           MDSClient* mdsclient,
                           IOManager4File* iomanager):
                           isleaseAvaliable_(true),
                           failedrefreshcount_(0) {
    userinfo_    = userinfo;
    mdsclient_   = mdsclient;
    iomanager_   = iomanager;
    leaseoption_ = leaseOpt;
    task_ = nullptr;
}

LeaseExecutor::~LeaseExecutor() {
    if (task_) {
        task_->WaitTaskExit();
    }
}

bool LeaseExecutor::Start(const FInfo_t& fi, const LeaseSession_t&  lease) {
    fullFileName_ = fi.fullPathName;

    leasesession_ = lease;
    if (leasesession_.leaseTime <= 0) {
        LOG(ERROR) << "Invalid lease time, filename = " << fullFileName_;
        return false;
    }

    if (leaseoption_.mdsRefreshTimesPerLease == 0) {
        LOG(ERROR) << "Invalid refreshTimesPerLease, filename = "
                   << fullFileName_;
        return false;
    }

    iomanager_->UpdateFileInfo(fi);

    auto interval =
        leasesession_.leaseTime / leaseoption_.mdsRefreshTimesPerLease;

    task_.reset(new (std::nothrow) RefreshSessionTask(this, interval));
    if (task_ == nullptr) {
        LOG(ERROR) << "Allocate RefreshSessionTask failed, filename = "
                   << fullFileName_;
        return false;
    }

    timespec abstime = butil::microseconds_from_now(interval);
    brpc::PeriodicTaskManager::StartTaskAt(task_.get(), abstime);

    return true;
}

bool LeaseExecutor::RefreshLease() {
    if (!LeaseValid()) {
        LOG(INFO) << "lease not valid!";
        iomanager_->LeaseTimeoutBlockIO();
    }

    LeaseRefreshResult response;
    LIBCURVE_ERROR ret = mdsclient_->RefreshSession(
        fullFileName_, userinfo_, leasesession_.sessionID, &response);

    if (LIBCURVE_ERROR::FAILED == ret) {
        LOG(WARNING) << "Refresh session rpc failed, filename = "
                     << fullFileName_;
        return true;
    } else if (LIBCURVE_ERROR::AUTHFAIL == ret) {
        iomanager_->LeaseTimeoutBlockIO();
        LOG(ERROR) << "Refresh session auth fail, block io. "
                   << "session id = " << leasesession_.sessionID
                   << ", filename = " << fullFileName_;
        return true;
    }

    if (response.status == LeaseRefreshResult::Status::OK) {
        CheckNeedUpdateVersion(response.finfo.seqnum);
        failedrefreshcount_.store(0);
        isleaseAvaliable_.store(true);
        iomanager_->RefeshSuccAndResumeIO();
        return true;
    } else if (response.status == LeaseRefreshResult::Status::NOT_EXIST) {
        iomanager_->LeaseTimeoutBlockIO();
        isleaseAvaliable_.store(false);
        LOG(ERROR) << "session or file not exists, no longer refresh!"
                   << ", sessionid = " << leasesession_.sessionID
                   << ", filename = " << fullFileName_;
        return false;
    } else {
        LOG(ERROR) << "Refresh session failed, filename = " << fullFileName_;
        return true;
    }
    return true;
}

std::string LeaseExecutor::GetLeaseSessionID() {
    return leasesession_.sessionID;
}

void LeaseExecutor::Stop() {
    if (task_ != nullptr) {
        task_->Stop();
    }
}

bool LeaseExecutor::LeaseValid() {
    return isleaseAvaliable_.load();
}

void LeaseExecutor::IncremRefreshFailed() {
    failedrefreshcount_.fetch_add(1);
    if (failedrefreshcount_.load() >= leaseoption_.mdsRefreshTimesPerLease) {
        isleaseAvaliable_.store(false);
        iomanager_->LeaseTimeoutBlockIO();
        LOG(ERROR) << "session invalid now!";
    }
}

void LeaseExecutor::CheckNeedUpdateVersion(uint64_t newversion) {
    const uint64_t currentFileSn = iomanager_->GetLatestFileSn();

    DVLOG(9) << "new file version = " << newversion
        << ", current version = " << currentFileSn
        << ", filename = " << fullFileName_;
    if (newversion > currentFileSn) {
        iomanager_->SetLatestFileSn(newversion);
    }
}

void LeaseExecutor::ResetRefreshSessionTask() {
    if (task_ == nullptr) {
        return;
    }

    // 等待前一个任务退出
    task_->Stop();
    task_->WaitTaskExit();

    auto interval = task_->RefreshIntervalUs();

    task_.reset(new (std::nothrow) RefreshSessionTask(this, interval));
    timespec abstime = butil::microseconds_from_now(interval);
    brpc::PeriodicTaskManager::StartTaskAt(task_.get(), abstime);

    isleaseAvaliable_.store(true);
}

}   // namespace client
}   // namespace curve
