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

namespace curve {
namespace client {

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

SourceReader::SourceReader() : fileClient_(new FileClient()) {}

SourceReader::~SourceReader() {
    Stop();
    Uinit();
}

void SourceReader::Run() {
    if (!running_) {
        running_ = true;
        fdCloseThread_ = std::thread(&SourceReader::Closefd, this);
        LOG(INFO) << "SourceReader fdCloseThread run successfully";
    } else {
        LOG(WARNING) << "SourceReader fdCloseThread is running!";
    }
}

void SourceReader::Stop() {
    if (running_) {
        running_ = false;
        sleeper_.interrupt();
        fdCloseThread_.join();
        LOG(INFO) << "SourceReader fdCloseThread stoped successfully";
    } else {
        LOG(WARNING) << "SourceReader fdCloseThread already stopped!";
    }
}

int SourceReader::Init(const std::string& configPath) {
    if (inited_) {
        LOG(WARNING) << "SourceReader already inited!";
        return 0;
    }

    if (LIBCURVE_ERROR::OK == fileClient_->Init(configPath)) {
        inited_ = true;
        return LIBCURVE_ERROR::OK;
    }
    return -LIBCURVE_ERROR::FAILED;
}

void SourceReader::Uinit() {
    if (!inited_) {
        LOG(WARNING) << "SourceReader not inited!";
        return;
    }
    if (fileClient_ != nullptr) {
        for (auto &pair : fdMap_) {
            if (LIBCURVE_ERROR::OK != fileClient_->Close(pair.second.first)) {
                LOG(ERROR) << "Close fd failed, fd = " << pair.second.first;
                return;
            }
        }
        fdMap_.clear();
        fileClient_->UnInit();
        delete fileClient_;
        fileClient_ = nullptr;
        inited_ = false;
    }
}

SourceReader& SourceReader::GetInstance() {
    static SourceReader originReader;
    return originReader;
}

int SourceReader::Getfd(const std::string& fileName,
                        const UserInfo_t& userInfo) {
    {
        curve::common::ReadLockGuard readLockGuard(rwLock_);
        auto iter = fdMap_.find(fileName);
        if (iter != fdMap_.end()) {
            iter->second.second = time(0);
            return iter->second.first;
        }
    }
    {
        int fd = 0;
        curve::common::WriteLockGuard writeLockGuard(rwLock_);
        auto iter = fdMap_.find(fileName);
        if (iter != fdMap_.end()) {
            return iter->second.first;
        } else {
            fd = fileClient_->Open4ReadOnly(fileName, userInfo);
            fdMap_.emplace(fileName, std::make_pair(fd, time(0)));
            return fd;
        }
    }
    LOG(ERROR) << "Getfd failed in fdMap_!";
    return -1;
}

int SourceReader::Read(std::vector<RequestContext*> reqCtxVec,
                                 const UserInfo_t& userInfo) {
    if (reqCtxVec.size() <= 0) {
        return 0;
    }

    for (auto reqCtx : reqCtxVec) {
        brpc::ClosureGuard doneGuard(reqCtx->done_);
        std::string fileName = reqCtx->sourceInfo_.cloneFileSource;
        CurveAioCombineContext *curveCombineCtx = new CurveAioCombineContext();
        curveCombineCtx->done = reqCtx->done_;
        curveCombineCtx->curveCtx.offset = reqCtx->sourceInfo_.cloneFileOffset
                                            + reqCtx->offset_;
        curveCombineCtx->curveCtx.length = reqCtx->rawlength_;
        curveCombineCtx->curveCtx.buf = &reqCtx->readData_;
        curveCombineCtx->curveCtx.op = LIBCURVE_OP::LIBCURVE_OP_READ;
        curveCombineCtx->curveCtx.cb = CurveAioCallback;

        int ret = fileClient_->AioRead(Getfd(fileName, userInfo),
                            &curveCombineCtx->curveCtx, UserDataType::IOBuffer);
        if (ret !=  LIBCURVE_ERROR::OK) {
            LOG(ERROR) << "Read curve file failed."
                    << "file name: " << fileName
                    << " ,error code: " << ret;
            delete curveCombineCtx;
            return -1;
        } else {
            doneGuard.release();
        }
    }
    return 0;
}

int SourceReader::Closefd() {
    while (running_) {
        {
            curve::common::WriteLockGuard writeLockGuard(rwLock_);
            for (auto iter = fdMap_.begin(); iter != fdMap_.end();) {
                int fd = iter->second.first;
                time_t timestamp = iter->second.second;
                if (time(0) - timestamp > fileClient_->GetClientConfig().
                                        GetFileServiceOption().
                                        ioOpt.closeFdThreadOption.fdTimeout) {
                    fileClient_->Close(fd);
                    iter = fdMap_.erase(iter);
                } else {
                    iter++;
                }
            }
        }

        sleeper_.wait_for(std::chrono::seconds(fileClient_->GetClientConfig().
        GetFileServiceOption().ioOpt.closeFdThreadOption.fdCloseTimeInterval));
    }
}

}  // namespace client
}  // namespace curve
