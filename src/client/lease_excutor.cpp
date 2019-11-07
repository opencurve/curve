/*
 * Project: curve
 * File Created: Saturday, 23rd February 2019 1:41:31 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */
#include <glog/logging.h>

#include "src/common/timeutility.h"
#include "src/client/lease_excutor.h"
#include "src/client/service_helper.h"

using curve::common::TimeUtility;

namespace curve {
namespace client {
LeaseExcutor::LeaseExcutor(const LeaseOption_t& leaseOpt,
                           UserInfo_t userinfo,
                           MDSClient* mdsclient,
                           IOManager4File* iomanager):
                           isleaseAvaliable_(true),
                           failedrefreshcount_(0) {
    userinfo_    = userinfo;
    mdsclient_   = mdsclient;
    iomanager_   = iomanager;
    leaseoption_ = leaseOpt;
    refreshTask_ = nullptr;
}

bool LeaseExcutor::Start(FInfo_t fi, LeaseSession_t  lease) {
    finfo_ = fi;
    leasesession_ = lease;
    if (leasesession_.leaseTime <= 0) {
        LOG(ERROR) << "invalid lease time";
        return false;
    }

    iomanager_->UpdataFileInfo(finfo_);
    timerTaskWorker_.Start();

    auto refreshleasetask = [this]() {
        this->RefreshLease();
    };

    auto Interval = leasesession_.leaseTime/leaseoption_.mdsRefreshTimesPerLease;  // NOLINT

    refreshTask_ = new (std::nothrow) TimerTask(Interval);
    if (refreshTask_ == nullptr) {
        LOG(ERROR) << "allocate failed!";
        return false;
    }
    refreshTask_->AddCallback(refreshleasetask);
    timerTaskWorker_.AddTimerTask(refreshTask_);
    LOG(INFO) << "add timer task "
              << refreshTask_->GetTimerID()
              << " for lease refresh!";
    return true;
}

void LeaseExcutor::RefreshLease() {
    if (!LeaseValid()) {
        LOG(INFO) << "lease not valid!";
        iomanager_->LeaseTimeoutBlockIO();
    }
    leaseRefreshResult response;
    LIBCURVE_ERROR ret = mdsclient_->RefreshSession(finfo_.fullPathName,
                                                    userinfo_,
                                                    leasesession_.sessionID,
                                                    &response);
    if (LIBCURVE_ERROR::FAILED == ret) {
        LOG(ERROR) << "refresh session rpc failed!";
        IncremRefreshFailed();
        return;
    } else if (LIBCURVE_ERROR::AUTHFAIL == ret) {
        iomanager_->LeaseTimeoutBlockIO();
        LOG(WARNING) << "refresh session auth fail, block io. "
                     << "session id = " << leasesession_.sessionID;
        return;
    }

    if (response.status == leaseRefreshResult::Status::OK) {
        CheckNeedUpdateVersion(response.finfo.seqnum);
        failedrefreshcount_.store(0);
        isleaseAvaliable_.store(true);
        iomanager_->RefeshSuccAndResumeIO();
    } else if (response.status == leaseRefreshResult::Status::NOT_EXIST) {
        iomanager_->LeaseTimeoutBlockIO();
        refreshTask_->SetDeleteSelf();
        isleaseAvaliable_.store(false);
        LOG(WARNING) << "session or file not exists, no longer refresh!"
                     << ", sessionid = " << leasesession_.sessionID;
    } else {
        LOG(WARNING) << leasesession_.sessionID << " lease refresh failed!";
        IncremRefreshFailed();
    }
    return;
}

std::string LeaseExcutor::GetLeaseSessionID() {
    return leasesession_.sessionID;
}

void LeaseExcutor::Stop() {
    if (refreshTask_ != nullptr) {
        timerTaskWorker_.CancelTimerTask(refreshTask_);
        delete refreshTask_;
        refreshTask_ = nullptr;
    }
    timerTaskWorker_.Stop();
}

bool LeaseExcutor::LeaseValid() {
    return isleaseAvaliable_.load();
}

void LeaseExcutor::IncremRefreshFailed() {
    failedrefreshcount_.fetch_add(1);
    if (failedrefreshcount_.load() >= leaseoption_.mdsRefreshTimesPerLease) {
        isleaseAvaliable_.store(false);
        iomanager_->LeaseTimeoutBlockIO();
        LOG(ERROR) << "session invalid now!";
    }
}

void LeaseExcutor::CheckNeedUpdateVersion(uint64_t newversion) {
    DVLOG(9) <<"newversion = " << newversion
             << ", current seq = " << finfo_.seqnum;
    if (newversion > finfo_.seqnum) {
        finfo_.seqnum = newversion;
        iomanager_->UpdataFileInfo(finfo_);
    }
}

}   // namespace client
}   // namespace curve
