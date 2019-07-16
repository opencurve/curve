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
                    inflightIONum_(0),
                    scheduler_(nullptr) {
}

bool IOManager4File::Initialize(const std::string& filename,
                                const IOOption_t& ioOpt,
                                MDSClient* mdsclient) {
    ioopt_ = ioOpt;

    mc_.Init(ioopt_.metaCacheOpt, mdsclient);
    Splitor::Init(ioopt_.ioSplitOpt);

    confMetric_.maxInFlightIONum.set_value(
                ioopt_.inflightOpt.maxInFlightIONum);

    fileMetric_ = new (std::nothrow) FileMetric(filename);
    if (fileMetric_ == nullptr) {
        LOG(ERROR) << "allocate client metric failed!";
        return false;
    }

    scheduler_ = new (std::nothrow) RequestScheduler();
    if (scheduler_ == nullptr ||
        -1 == scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_)) {
        LOG(ERROR) << "Init scheduler_ failed!";
        return false;
    }
    scheduler_->Run();

    int ret = taskPool_.Start(ioopt_.taskThreadOpt.taskThreadPoolSize,
                              ioopt_.taskThreadOpt.taskQueueCapacity);
    if (ret != 0) {
        LOG(ERROR) << "task thread pool start failed!";
        return false;
    }

    LOG(INFO) << "iomanager init success!";
    return true;
}

void IOManager4File::UnInitialize() {
    // 然后需要等到所有inflight的IO全部返回才能析构scheduler
    WaitInflightIOAllComeBack();

    taskPool_.Stop();

    if (scheduler_ != nullptr) {
        scheduler_->Fini();
        delete scheduler_;
        scheduler_ = nullptr;
    }

    if (fileMetric_ != nullptr) {
        delete fileMetric_;
        fileMetric_ = nullptr;
    }
}

void IOManager4File::GetInflightIOToken() {
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

int IOManager4File::Read(char* buf, off_t offset,
                        size_t length, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::READ);
    FlightIOGuard guard(this);

    IOTracker temp(this, &mc_, scheduler_, fileMetric_);
    temp.StartRead(nullptr, buf, offset, length, mdsclient, &fi_);

    int rc = temp.Wait();
    return rc;
}

int IOManager4File::Write(const char* buf, off_t offset,
                         size_t length, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::WRITE);
    FlightIOGuard guard(this);

    IOTracker temp(this, &mc_, scheduler_, fileMetric_);
    temp.StartWrite(nullptr, buf, offset, length, mdsclient, &fi_);

    int rc = temp.Wait();
    return rc;
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
int IOManager4File::AioRead(CurveAioContext* ctx, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::READ);

    GetInflightIOToken();
    MetricHelper::IncremInflightIO(fileMetric_);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartRead(ctx, static_cast<char*>(ctx->buf),
                        ctx->offset, ctx->length, mdsclient, &this->fi_);
    };

    MetricHelper::IncremUserQPSCount(fileMetric_, ctx->length, OpType::READ);

    taskPool_.Enqueue(task);
    return LIBCURVE_ERROR::OK;
}

int IOManager4File::AioWrite(CurveAioContext* ctx, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::WRITE);

    GetInflightIOToken();
    MetricHelper::IncremInflightIO(fileMetric_);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartWrite(ctx, static_cast<const char*>(ctx->buf),
                         ctx->offset, ctx->length, mdsclient, &this->fi_);
    };

    MetricHelper::IncremUserQPSCount(fileMetric_, ctx->length, OpType::WRITE);

    taskPool_.Enqueue(task);
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
    scheduler_->LeaseTimeoutBlockIO();
}

void IOManager4File::RefeshSuccAndResumeIO() {
    scheduler_->RefeshSuccAndResumeIO();
}

}   // namespace client
}   // namespace curve
