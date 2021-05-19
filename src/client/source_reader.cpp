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

/**
 * Project: curve
 * File Created: 2020-12-23
 * Author: wanghai01
 */

#include "src/client/source_reader.h"

#include "include/client/libcurve.h"
#include "src/client/libcurve_file.h"
#include "src/client/request_closure.h"
#include "src/client/file_instance.h"

namespace curve {
namespace client {

using curve::common::ReadLockGuard;

struct CurveAioCombineContext {
    RequestClosure* done;
    CurveAioContext curveCtx;
};

void CurveAioCallback(struct CurveAioContext* context) {
    auto curveCombineCtx = reinterpret_cast<CurveAioCombineContext *>(
        reinterpret_cast<char *>(context) -
        offsetof(CurveAioCombineContext, curveCtx));
    RequestClosure* done = curveCombineCtx->done;
    if (context->ret < 0) {
        done->SetFailed(LIBCURVE_ERROR::FAILED);
    } else {
        done->SetFailed(LIBCURVE_ERROR::OK);
    }

    delete curveCombineCtx;
    brpc::ClosureGuard doneGuard(done);
}

SourceReader::ReadHandler::~ReadHandler() {
    if (ownfile_ && file_ != nullptr) {
        file_->UnInitialize();
        delete file_;
        file_ = nullptr;
    }
}

SourceReader::~SourceReader() {
    Stop();
}

void SourceReader::Run() {
    if (!running_) {
        sleeper_.reset(new common::InterruptibleSleeper());
        fdCloseThread_.reset(new std::thread(&SourceReader::Closefd, this));
        running_ = true;
        LOG(INFO) << "SourceReader fdCloseThread run successfully";
    } else {
        LOG(WARNING) << "SourceReader fdCloseThread is running!";
    }
}

void SourceReader::Stop() {
    if (running_) {
        sleeper_->interrupt();
        fdCloseThread_->join();
        sleeper_.reset();
        fdCloseThread_.reset();
        running_ = false;
        LOG(INFO) << "SourceReader fdCloseThread stoped successfully";
    } else {
        LOG(WARNING) << "SourceReader fdCloseThread already stopped!";
    }
}

void SourceReader::SetOption(const FileServiceOption& opt) {
    fileOption_ = opt;
}

std::unordered_map<std::string, SourceReader::ReadHandler>&
SourceReader::GetReadHandlers() {
    return readHandlers_;
}

void SourceReader::SetReadHandlers(
    const std::unordered_map<std::string, ReadHandler>& handlers) {
    curve::common::WriteLockGuard wlk(rwLock_);
    readHandlers_.clear();

    for (const auto& handler : handlers) {
        readHandlers_.emplace(std::piecewise_construct,
                              std::forward_as_tuple(handler.first),
                              std::forward_as_tuple(handler.second.file_,
                                                    handler.second.lastUsedSec_,
                                                    handler.second.ownfile_));
    }
}

SourceReader::ReadHandler* SourceReader::GetReadHandler(
    const std::string& fileName, const UserInfo& userInfo,
    MDSClient* mdsclient) {
    {
        ReadLockGuard rlk(rwLock_);
        auto iter = readHandlers_.find(fileName);
        if (iter != readHandlers_.end()) {
            iter->second.lastUsedSec_.store(::time(nullptr),
                                            std::memory_order_relaxed);
            return &iter->second;
        }
    }

    FileInstance* instance = FileInstance::Open4Readonly(
        fileOption_, mdsclient->shared_from_this(), fileName, userInfo);
    if (instance == nullptr) {
        return nullptr;
    }

    instance->GetIOManager4File()->SetDisableStripe();

    curve::common::WriteLockGuard wlk(rwLock_);
    auto res = readHandlers_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(fileName),
        std::forward_as_tuple(instance, ::time(nullptr), true));

    if (res.second == false) {
        instance->UnInitialize();
        delete instance;
    }

    return &res.first->second;
}

int SourceReader::Read(const std::vector<RequestContext*>& reqCtxVec,
                       const UserInfo& userInfo, MDSClient* mdsClient) {
    if (reqCtxVec.empty()) {
        return 0;
    }

    static std::once_flag flag;
    std::call_once(flag, []() { GetInstance().Run(); });

    for (auto reqCtx : reqCtxVec) {
        brpc::ClosureGuard doneGuard(reqCtx->done_);
        std::string fileName = reqCtx->sourceInfo_.cloneFileSource;
        CurveAioCombineContext* curveCombineCtx = new CurveAioCombineContext();
        curveCombineCtx->done = reqCtx->done_;
        curveCombineCtx->curveCtx.offset =
            reqCtx->sourceInfo_.cloneFileOffset + reqCtx->offset_;
        curveCombineCtx->curveCtx.length = reqCtx->rawlength_;
        curveCombineCtx->curveCtx.buf = &reqCtx->readData_;
        curveCombineCtx->curveCtx.op = LIBCURVE_OP::LIBCURVE_OP_READ;
        curveCombineCtx->curveCtx.cb = CurveAioCallback;

        ReadHandler* handler = GetReadHandler(fileName, userInfo, mdsClient);
        if (handler == nullptr) {
            LOG(ERROR) << "Get ReadHandler failed, filename = " << fileName;
            return -1;
        }

        int ret = handler->file_->AioRead(&curveCombineCtx->curveCtx,
                                          UserDataType::IOBuffer);
        if (ret != LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "Read curve failed failed, filename = " << fileName
                       << ", error = " << ret;
            delete curveCombineCtx;
            return -1;
        } else {
            doneGuard.release();
        }
    }

    return 0;
}

void SourceReader::Closefd() {
    const std::chrono::seconds sleepSec(
        fileOption_.ioOpt.closeFdThreadOption.fdCloseTimeInterval);
    const uint32_t fdTimeout = fileOption_.ioOpt.closeFdThreadOption.fdTimeout;

    while (sleeper_->wait_for(sleepSec)) {
        LOG(INFO) << "Scan read handlers...";
        curve::common::WriteLockGuard writeLockGuard(rwLock_);
        auto iter = readHandlers_.begin();
        while (iter != readHandlers_.end()) {
            time_t lastUsedSec = iter->second.lastUsedSec_;
            if (::time(nullptr) - lastUsedSec > fdTimeout) {
                LOG(INFO) << iter->first
                          << " timedout, last used: " << lastUsedSec;
                iter = readHandlers_.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

}  // namespace client
}  // namespace curve
