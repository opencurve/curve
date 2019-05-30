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
                    blockIO_(false),
                    inflightIONum_(0),
                    scheduler_(nullptr) {
}

bool IOManager4File::Initialize(IOOption_t ioOpt,
                                ClientMetric_t* clientMetric) {
    ioopt_ = ioOpt;
    if (clientMetric == nullptr) {
        LOG(ERROR) << "metric pointer is null!";
        return false;
    }
    clientMetric_ = clientMetric;
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
    // 然后需要等到所有inflight的IO全部返回才能析构scheduler
    WaitInflightIOAllComeBack();

    if (scheduler_ != nullptr) {
        scheduler_->Fini();
        delete scheduler_;
        scheduler_ = nullptr;
    }
}

void IOManager4File::GetInflightIOToken() {
    // lease续约失败的时候需要阻塞IO直到续约成功
    if (blockIO_.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> lk(leaseRefreshmtx_);
        leaseRefreshcv_.wait(lk, [&]()->bool{
            return !blockIO_.load();
        });
    }
    // 当inflight IO超过限制的时候要等到其他inflight IO回来才能继续发送
    // 因为rpc内部需要同一链路上目前允许的最大的未发送的数据量为8MB，因此
    // 不能无限制的向下发送异步IO.
    if (CheckInflightIOOverFlow()) {
        WaitForInflightIOComeBack();
    }

    IncremInflightIONum();
}

void IOManager4File::ReleaseInflightIOToken() {
    DecremInflightIONum();
    if (inflightIONum_.load() == 0) {
        inflightIOAllComeBackcv_.notify_all();
    }
    // 一旦有inflight IO回来就唤醒被block的IO
    inflightIOComeBackcv_.notify_one();
}

int IOManager4File::Read(char* buf,
                        off_t offset,
                        size_t length,
                        MDSClient* mdsclient) {
    FlightIOGuard guard(this);

    IOTracker temp(this, &mc_, scheduler_, clientMetric_);
    temp.StartRead(nullptr, buf, offset, length, mdsclient, &fi_);

    return temp.Wait();
}

int IOManager4File::Write(const char* buf,
                        off_t offset,
                        size_t length,
                        MDSClient* mdsclient) {
    FlightIOGuard guard(this);

    IOTracker temp(this, &mc_, scheduler_, clientMetric_);
    temp.StartWrite(nullptr, buf, offset, length, mdsclient, &fi_);

    return temp.Wait();
}

/**
 * CheckInflightIOOverFlow会检查当前未返回的io数量是否超过我们限制的最大未返回IO数量
 * 但是真正的inflight IO数量与上层并发调用aio read write的线程数有关。
 * 假设我们设置的maxinflight=100，上层有三个线程在同时调用aioread，如果这个时候
 * inflight io数量为99，那么并发状况下这3个线程在CheckInflightIOOverFlow都会通过
 * 然后向下并发执行IncremInflightIONum，这个时候真正的inflight IO为102，下一个IO
 * 下发的时候需要等到inflight IO数量小于100才能继续，也就是等至少3个IO回来才能继续下发。
 * 这个误差是可以接受的，他与qemu一侧并发度有关，误差有上限。
 * 如果想要精确控制inflight IO数量，就需要在每个接口处加锁，让原本可以并发的逻辑变成了
 * 串行，这样得不偿失。因此我们这里选择容忍一定误差范围。
 */
int IOManager4File::AioRead(CurveAioContext* aioctx,
                            MDSClient* mdsclient) {
    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, clientMetric_);
    if (temp == nullptr) {
        LOG(ERROR) << "allocate tracker failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    GetInflightIOToken();

    temp->StartRead(aioctx,
                    static_cast<char*>(aioctx->buf),
                    aioctx->offset,
                    aioctx->length,
                    mdsclient,
                    &fi_);
    return LIBCURVE_ERROR::OK;
}

int IOManager4File::AioWrite(CurveAioContext* aioctx,
                            MDSClient* mdsclient) {
    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, clientMetric_);
    if (temp == nullptr) {
        LOG(ERROR) << "allocate tracker failed!";
        return -LIBCURVE_ERROR::FAILED;
    }

    GetInflightIOToken();

    temp->StartWrite(aioctx,
                    static_cast<const char*>(aioctx->buf),
                    aioctx->offset,
                    aioctx->length,
                    mdsclient,
                    &fi_);
    return LIBCURVE_ERROR::OK;
}

void IOManager4File::UpdataFileInfo(const FInfo_t& fi) {
    fi_ = fi;
}

void IOManager4File::HandleAsyncIOResponse(IOTracker* iotracker) {
    ReleaseInflightIOToken();
    delete iotracker;
}

void IOManager4File::WaitInflightIOAllComeBack() {
    LOG(INFO) << "wait inflight I/O to complete, count = " << inflightIONum_;
    std::unique_lock<std::mutex> lk(inflightIOAllComeBackmtx_);
    inflightIOAllComeBackcv_.wait(lk, [this]() {
        return inflightIONum_.load(std::memory_order_acquire) == 0;
    });
    LOG(INFO) << "inflight I/O ALL come back.";
}

void IOManager4File::LeaseTimeoutBlockIO() {
    blockIO_.store(true, std::memory_order_release);
}

void IOManager4File::RefeshSuccAndResumeIO() {
    blockIO_.store(false, std::memory_order_release);
    leaseRefreshcv_.notify_all();
}

}   // namespace client
}   // namespace curve
