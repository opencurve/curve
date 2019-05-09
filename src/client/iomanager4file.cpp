/*
 * Project: curve
 * File Created: Monday, 17th September 2018 4:15:38 pm
 * Author: tongguangxun
 * Copyright (c) 2018 NetEase
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>   // NOLINT

#include "src/client/metacache.h"
#include "src/client/iomanager4file.h"
#include "src/client/file_instance.h"
#include "src/client/io_tracker.h"
#include "src/client/splitor.h"

namespace curve {
namespace client {
IOManager4File::IOManager4File():
                    disableio_(false),
                    startWaitInflightIO_(false),
                    inflightIONum_(0) {
}

bool IOManager4File::Initialize(IOOption_t ioOpt) {
    ioopt_ = ioOpt;
    mc_.Init(ioopt_.metaCacheOpt);
    Splitor::Init(ioopt_.ioSplitOpt);

    scheduler_ = new (std::nothrow) RequestScheduler();
    if (scheduler_ == nullptr ||
        -1 == scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_)) {
        LOG(ERROR) << "Init scheduler_ failed!";
        return false;
    }
    scheduler_->Run();

    return true;
}

void IOManager4File::UnInitialize() {
    if (scheduler_ != nullptr) {
        scheduler_->Fini();
        delete scheduler_;
        scheduler_ = nullptr;
    }
}

int IOManager4File::Read(char* buf,
                        off_t offset,
                        size_t length,
                        MDSClient* mdsclient) {
    if (disableio_.load(std::memory_order_acquire)) {
        return -LIBCURVE_ERROR::DISABLEIO;
    }
    if (startWaitInflightIO_.load(std::memory_order_acquire)) {
        WaitInflightIOComeBack();
    }
    IOTracker temp(this, &mc_, scheduler_);
    inflightIONum_.fetch_add(1, std::memory_order_release);
    temp.StartRead(nullptr, buf, offset, length, mdsclient, &fi_);

    int ret = temp.Wait();
    RefreshInflightIONum();
    return ret;
}

int IOManager4File::Write(const char* buf,
                        off_t offset,
                        size_t length,
                        MDSClient* mdsclient) {
    if (disableio_.load(std::memory_order_acquire)) {
        return -LIBCURVE_ERROR::DISABLEIO;
    }
    if (startWaitInflightIO_.load(std::memory_order_acquire)) {
        WaitInflightIOComeBack();
    }
    IOTracker temp(this, &mc_, scheduler_);
    inflightIONum_.fetch_add(1, std::memory_order_release);
    temp.StartWrite(nullptr, buf, offset, length, mdsclient, &fi_);

    int ret = temp.Wait();
    RefreshInflightIONum();
    return ret;
}

void IOManager4File::AioRead(CurveAioContext* aioctx,
                            MDSClient* mdsclient) {
    if (disableio_.load(std::memory_order_acquire)) {
        aioctx->ret = -LIBCURVE_ERROR::DISABLEIO;
        aioctx->cb(aioctx);
        return;
    }
    if (startWaitInflightIO_.load(std::memory_order_acquire)) {
        WaitInflightIOComeBack();
    }
    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_, scheduler_);
    if (temp == nullptr) {
        LOG(ERROR) << "allocate tracker failed!";
        aioctx->ret = -LIBCURVE_ERROR::FAILED;
        aioctx->cb(aioctx);
        return;
    }
    inflightIONum_.fetch_add(1, std::memory_order_release);
    temp->StartRead(aioctx,
                    static_cast<char*>(aioctx->buf),
                    aioctx->offset,
                    aioctx->length,
                    mdsclient,
                    &fi_);
}

void IOManager4File::AioWrite(CurveAioContext* aioctx,
                            MDSClient* mdsclient) {
    if (disableio_.load(std::memory_order_acquire)) {
        aioctx->ret = -LIBCURVE_ERROR::DISABLEIO;
        aioctx->cb(aioctx);
        return;
    }
    if (startWaitInflightIO_.load(std::memory_order_acquire)) {
        WaitInflightIOComeBack();
    }
    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_, scheduler_);
    if (temp == nullptr) {
        LOG(ERROR) << "allocate tracker failed!";
        aioctx->ret = -LIBCURVE_ERROR::FAILED;
        aioctx->cb(aioctx);
        return;
    }
    inflightIONum_.fetch_add(1, std::memory_order_release);
    temp->StartWrite(aioctx,
                    static_cast<const char*>(aioctx->buf),
                    aioctx->offset,
                    aioctx->length,
                    mdsclient,
                    &fi_);
}

void IOManager4File::UpdataFileInfo(const FInfo_t& fi) {
    fi_ = fi;
}

void IOManager4File::HandleAsyncIOResponse(IOTracker* iotracker) {
    RefreshInflightIONum();
    delete iotracker;
}

void IOManager4File::StartWaitInflightIO() {
    startWaitInflightIO_.store(true, std::memory_order_release);
}

void IOManager4File::WaitInflightIOComeBack() {
    LOG(INFO) << "Snapshot version changed, wait inflight I/O to complete,"
              << "count = " << startWaitInflightIO_;
    std::unique_lock<std::mutex> lk(mtx_);
    inflightcv_.wait(lk, [this]() {
        return inflightIONum_.load(std::memory_order_acquire) == 0;
    });
    startWaitInflightIO_.store(false, std::memory_order_release);
    LOG(INFO) << "inflight I/O ALL come back.";
}

void IOManager4File::LeaseTimeoutDisableIO() {
    disableio_.store(true, std::memory_order_release);
}

void IOManager4File::RefeshSuccAndResumeIO() {
    disableio_.store(false, std::memory_order_release);
}

void IOManager4File::RefreshInflightIONum() {
    inflightIONum_.fetch_sub(1, std::memory_order_release);
    if (inflightIONum_.load() == 0) {
        inflightcv_.notify_one();
    }
}
}   // namespace client
}   // namespace curve
