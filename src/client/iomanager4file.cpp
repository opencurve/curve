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
Atomic<uint64_t> IOManager::idRecorder_(1);
IOManager4File::IOManager4File(): scheduler_(nullptr), exit_(false) {
}

bool IOManager4File::Initialize(const std::string& filename,
                                const IOOption_t& ioOpt,
                                MDSClient* mdsclient) {
    ioopt_ = ioOpt;

    mc_.Init(ioopt_.metaCacheOpt, mdsclient);
    Splitor::Init(ioopt_.ioSplitOpt);

    confMetric_.maxInFlightRPCNum.set_value(
                ioopt_.ioSenderOpt.inflightOpt.maxInFlightRPCNum);

    int ret = RequestClosure::AddInflightCntl(id_,
              ioopt_.ioSenderOpt.inflightOpt);
    if (ret != 0) {
        LOG(ERROR) << "add inflight control for rpc failed!";
        return false;
    }

    fileMetric_ = new (std::nothrow) FileMetric(filename);
    if (fileMetric_ == nullptr) {
        LOG(ERROR) << "allocate client metric failed!";
        return false;
    }

    // IO Manager中不控制inflight IO数量，所以传入UINT64_MAX
    // 但是IO Manager需要控制所有inflight IO在关闭的时候都被回收掉
    inflightCntl_.SetMaxInflightNum(UINT64_MAX);

    scheduler_ = new (std::nothrow) RequestScheduler();
    if (scheduler_ == nullptr ||
        -1 == scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_, fileMetric_)) {
        LOG(ERROR) << "Init scheduler_ failed!";
        return false;
    }
    scheduler_->Run();

    ret = taskPool_.Start(ioopt_.taskThreadOpt.taskThreadPoolSize,
                          ioopt_.taskThreadOpt.taskQueueCapacity);
    if (ret != 0) {
        LOG(ERROR) << "task thread pool start failed!";
        return false;
    }

    LOG(INFO) << "iomanager init success! conf info: "
              << "taskThreadPoolSize = "
              << ioopt_.taskThreadOpt.taskThreadPoolSize
              << ", taskQueueCapacity = "
              << ioopt_.taskThreadOpt.taskQueueCapacity;
    return true;
}

void IOManager4File::UnInitialize() {
    bool exitFlag = false;
    std::mutex exitMtx;
    std::condition_variable exitCv;
    auto task = [&]() {
        std::unique_lock<std::mutex> lk(exitMtx);
        exitFlag = true;
        exitCv.notify_one();
    };

    taskPool_.Enqueue(task);

    {
        std::unique_lock<std::mutex> lk(exitMtx);
        exitCv.wait(lk, [&](){ return exitFlag; });
    }

    taskPool_.Stop();

    if (scheduler_ != nullptr) {
        scheduler_->WakeupBlockQueueAtExit();
        inflightCntl_.WaitInflightAllComeBack();
        scheduler_->Fini();
    }

    RequestClosure::DeleteInflightCntl(id_);

    {
        // 这个锁保证设置exit_和delete scheduler_是原子的
        // 这样保证在scheduler_被析构的时候lease线程不会使用scheduler_
        // jira: http://jira.netease.com/browse/CLDCFS-1395
        std::unique_lock<std::mutex> lk(exitMtx_);
        exit_ = true;

        delete scheduler_;
        delete fileMetric_;
        scheduler_ = nullptr;
        fileMetric_ = nullptr;
    }
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

int IOManager4File::AioRead(CurveAioContext* ctx, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::READ);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    inflightCntl_.IncremInflightNum();
    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartRead(ctx, static_cast<char*>(ctx->buf),
                        ctx->offset, ctx->length, mdsclient, &this->fi_);
    };

    taskPool_.Enqueue(task);
    return LIBCURVE_ERROR::OK;
}

int IOManager4File::AioWrite(CurveAioContext* ctx, MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::WRITE);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    inflightCntl_.IncremInflightNum();
    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartWrite(ctx, static_cast<const char*>(ctx->buf),
                         ctx->offset, ctx->length, mdsclient, &this->fi_);
    };

    taskPool_.Enqueue(task);
    return LIBCURVE_ERROR::OK;
}

void IOManager4File::UpdataFileInfo(const FInfo_t& fi) {
    fi_ = fi;
}

void IOManager4File::HandleAsyncIOResponse(IOTracker* iotracker) {
    inflightCntl_.DecremInflightNum();
    delete iotracker;
}

void IOManager4File::LeaseTimeoutBlockIO() {
    std::unique_lock<std::mutex> lk(exitMtx_);
    if (exit_ == false) {
        scheduler_->LeaseTimeoutBlockIO();
    } else {
        LOG(WARNING) << "io manager already exit, no need block io!";
    }
}

void IOManager4File::RefeshSuccAndResumeIO() {
    std::unique_lock<std::mutex> lk(exitMtx_);
    if (exit_ == false) {
        scheduler_->RefeshSuccAndResumeIO();
    } else {
        LOG(WARNING) << "io manager already exit, no need resume io!";
    }
}
}   // namespace client
}   // namespace curve
