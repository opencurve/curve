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
 * File Created: Monday, 17th September 2018 4:15:38 pm
 * Author: tongguangxun
 */

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
                                const IOOption& ioOpt,
                                MDSClient* mdsclient) {
    ioopt_ = ioOpt;

    mc_.Init(ioopt_.metaCacheOpt, mdsclient);
    Splitor::Init(ioopt_.ioSplitOpt);

    inflightRpcCntl_.SetMaxInflightNum(
        ioopt_.ioSenderOpt.inflightOpt.fileMaxInFlightRPCNum);

    fileMetric_ = new (std::nothrow) FileMetric(filename);
    if (fileMetric_ == nullptr) {
        LOG(ERROR) << "allocate client metric failed!";
        return false;
    }

    // IO Manager中不控制inflight IO数量，所以传入UINT64_MAX
    // 但是IO Manager需要控制所有inflight IO在关闭的时候都被回收掉
    inflightCntl_.SetMaxInflightNum(UINT64_MAX);

    scheduler_ = new (std::nothrow) RequestScheduler();
    if (scheduler_ == nullptr) {
        return false;
    }

    int ret = scheduler_->Init(ioopt_.reqSchdulerOpt, &mc_, fileMetric_);
    if (-1 == ret) {
        LOG(ERROR) << "Init scheduler_ failed!";
        delete scheduler_;
        scheduler_ = nullptr;
        return false;
    }
    scheduler_->Run();

    if (ioopt_.throttleOption.enable) {
        throttle_.reset(new common::Throttle());
    }

    ret = taskPool_.Start(ioopt_.taskThreadOpt.isolationTaskThreadPoolSize,
                          ioopt_.taskThreadOpt.isolationTaskQueueCapacity);
    if (ret != 0) {
        LOG(ERROR) << "task thread pool start failed!";
        return false;
    }

    LOG(INFO) << "iomanager init success! conf info: "
              << "isolationTaskThreadPoolSize = "
              << ioopt_.taskThreadOpt.isolationTaskThreadPoolSize
              << ", isolationTaskQueueCapacity = "
              << ioopt_.taskThreadOpt.isolationTaskQueueCapacity;
    return true;
}

void IOManager4File::UnInitialize() {
    // stop throttle first
    if (throttle_) {
        throttle_->Stop();
    }

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

    {
        // 这个锁保证设置exit_和delete scheduler_是原子的
        // 这样保证在scheduler_被析构的时候lease线程不会使用scheduler_
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

    butil::IOBuf data;

    IOTracker temp(this, &mc_, scheduler_, fileMetric_);
    temp.SetUserDataType(UserDataType::IOBuffer);
    temp.StartRead(&data, offset, length, mdsclient, this->GetFileInfo(),
                   throttle_.get());

    int rc = temp.Wait();

    if (rc < 0) {
        return rc;
    } else {
        size_t nc = data.copy_to(buf, length);
        return nc == length ? rc : -LIBCURVE_ERROR::FAILED;
    }
}

int IOManager4File::Write(const char* buf,
                          off_t offset,
                          size_t length,
                          MDSClient* mdsclient) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::WRITE);
    FlightIOGuard guard(this);

    butil::IOBuf data;
    data.append_user_data(const_cast<char*>(buf), length, TrivialDeleter);

    IOTracker temp(this, &mc_, scheduler_, fileMetric_);
    temp.SetUserDataType(UserDataType::IOBuffer);
    temp.StartWrite(&data, offset, length, mdsclient, this->GetFileInfo(),
                    throttle_.get());

    int rc = temp.Wait();
    return rc;
}

int IOManager4File::AioRead(CurveAioContext* ctx, MDSClient* mdsclient,
                            UserDataType dataType) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::READ);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    temp->SetUserDataType(dataType);
    inflightCntl_.IncremInflightNum();
    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartAioRead(ctx, mdsclient, this->GetFileInfo(),
                           throttle_.get());
    };

    taskPool_.Enqueue(task);
    return LIBCURVE_ERROR::OK;
}

int IOManager4File::AioWrite(CurveAioContext* ctx, MDSClient* mdsclient,
                             UserDataType dataType) {
    MetricHelper::IncremUserRPSCount(fileMetric_, OpType::WRITE);

    IOTracker* temp = new (std::nothrow) IOTracker(this, &mc_,
                                                   scheduler_, fileMetric_);
    if (temp == nullptr) {
        ctx->ret = -LIBCURVE_ERROR::FAILED;
        ctx->cb(ctx);
        LOG(ERROR) << "allocate tracker failed!";
        return LIBCURVE_ERROR::OK;
    }

    temp->SetUserDataType(dataType);
    inflightCntl_.IncremInflightNum();
    auto task = [this, ctx, mdsclient, temp]() {
        temp->StartAioWrite(ctx, mdsclient, this->GetFileInfo(),
                            throttle_.get());
    };

    taskPool_.Enqueue(task);
    return LIBCURVE_ERROR::OK;
}

void IOManager4File::UpdateFileInfo(const FInfo_t& fi) {
    mc_.UpdateFileInfo(fi);
}

void IOManager4File::UpdateFileThrottleParams(
    const common::ReadWriteThrottleParams& params) {
    if (throttle_) {
        throttle_->UpdateThrottleParams(params);
    }
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

void IOManager4File::ResumeIO() {
    std::unique_lock<std::mutex> lk(exitMtx_);
    if (exit_ == false) {
        scheduler_->ResumeIO();
    } else {
        LOG(WARNING) << "io manager already exit, no need resume io!";
    }
}

void IOManager4File::ReleaseInflightRpcToken() {
    inflightRpcCntl_.ReleaseInflightToken();
}

void IOManager4File::GetInflightRpcToken() {
    inflightRpcCntl_.GetInflightToken();
}

}   // namespace client
}   // namespace curve
