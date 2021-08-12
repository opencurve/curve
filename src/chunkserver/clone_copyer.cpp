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
 * Created Date: Wednesday March 20th 2019
 * Author: yangyaokai
 */

#include "src/chunkserver/clone_copyer.h"
#include "src/chunkserver/clone_core.h"
#include "src/common/timeutility.h"

namespace curve {
namespace chunkserver {

std::ostream& operator<<(std::ostream& out, const AsyncDownloadContext& rhs) {
    out  << "{ location: " << rhs.location
        << ", offset: " << rhs.offset
        << ", size: " << rhs.size
        << " }";
    return out;
}

struct CurveAioCombineContext {
    DownloadClosure* done;
    CurveAioContext curveCtx;
};

void CurveAioCallback(struct CurveAioContext* context) {
    auto curveCombineCtx = reinterpret_cast<CurveAioCombineContext *>(
        reinterpret_cast<char *>(context) -
        offsetof(CurveAioCombineContext, curveCtx));
    DownloadClosure* done = curveCombineCtx->done;
    if (context->ret < 0) {
        done->SetFailed();
    }
    delete curveCombineCtx;

    brpc::ClosureGuard doneGuard(done);
}

void OriginCopyer::DeleteExpiredCurveCache(void* arg) {
    OriginCopyer* taskCopyer = static_cast<OriginCopyer*>(arg);
    std::unique_lock<std::mutex> lock(taskCopyer->mtx_);
    std::unique_lock<std::mutex> lockTime(taskCopyer->timeMtx_);
    timespec now = butil::seconds_from_now(0);

    while (taskCopyer->curveOpenTime_.size() > 0) {
        CurveOpenTimestamp oldestCache = *taskCopyer->curveOpenTime_.begin();
        if (now.tv_sec - oldestCache.lastUsedSec <
            taskCopyer->curveFileTimeoutSec_) {
            break;
        }

        taskCopyer->curveClient_->Close(oldestCache.fd);
        taskCopyer->fdMap_.erase(oldestCache.fileName);
        taskCopyer->curveOpenTime_.pop_front();
    }

    if (taskCopyer->curveOpenTime_.size() > 0) {
        int64_t nextTimer = taskCopyer->curveFileTimeoutSec_ - now.tv_sec
            + taskCopyer->curveOpenTime_.begin()->lastUsedSec;
        timespec nextTimespec = butil::seconds_from_now(nextTimer);
        taskCopyer->timerId_ = taskCopyer->timer_.schedule(
            &DeleteExpiredCurveCache, arg, nextTimespec);
    } else {
        taskCopyer->timerId_ = bthread::TimerThread::INVALID_TASK_ID;
    }
}

OriginCopyer::OriginCopyer()
    : curveClient_(nullptr)
    , s3Client_(nullptr) {}

int OriginCopyer::Init(const CopyerOptions& options) {
    curveFileTimeoutSec_ = options.curveFileTimeoutSec;
    curveClient_ = options.curveClient;
    s3Client_ = options.s3Client;
    if (curveClient_ != nullptr) {
        int errorCode = curveClient_->Init(options.curveConf.c_str());
        if (errorCode != 0) {
            LOG(ERROR) << "Init curve client failed."
                    << "error code: " << errorCode;
            return -1;
        }
        curveUser_ = options.curveUser;
    } else {
        LOG(WARNING) << "Curve client is disabled.";
    }
    if (s3Client_ != nullptr) {
        s3Client_->Init(options.s3Conf);
    } else {
        LOG(WARNING) << "s3 adapter is disabled.";
    }
    bthread::TimerThreadOptions timerOptions;
    timerOptions.bvar_prefix = "curve file lastUsedSec";
    int rc = timer_.start(&timerOptions);
    if (rc == 0) {
        LOG(INFO) << "init curveFile timer thread success";
    } else {
        LOG(FATAL) << "init curveFile timer thread failed, " << berror(rc);
    }
    timerId_ = bthread::TimerThread::INVALID_TASK_ID;
    return 0;
}

int OriginCopyer::Fini() {
    std::unique_lock<std::mutex> lock(mtx_);
    std::unique_lock<std::mutex> lockTime(timeMtx_);
    if (curveClient_ != nullptr) {
        for (auto &pair : fdMap_) {
            curveClient_->Close(pair.second);
        }
        curveClient_->UnInit();
    }
    if (s3Client_ != nullptr) {
        s3Client_->Deinit();
    }
    if (timerId_ != bthread::TimerThread::INVALID_TASK_ID) {
        timer_.unschedule(timerId_);
    }
    curveOpenTime_.clear();
    return 0;
}

void OriginCopyer::DownloadAsync(DownloadClosure* done) {
    brpc::ClosureGuard doneGuard(done);
    AsyncDownloadContext* context = done->GetDownloadContext();
    std::string originPath;
    OriginType type =
        LocationOperator::ParseLocation(context->location, &originPath);
    if (type == OriginType::CurveOrigin) {
        off_t chunkOffset;
        std::string fileName;
        bool parseSuccess = LocationOperator::ParseCurveChunkPath(
            originPath, &fileName, &chunkOffset);
        if (!parseSuccess) {
            LOG(ERROR) << "Parse curve chunk path failed."
                       << "originPath: " << originPath;
            done->SetFailed();
            return;
        }
        DownloadFromCurve(fileName, chunkOffset + context->offset,
                          context->size, context->buf,
                          done);
        doneGuard.release();
    } else if (type == OriginType::S3Origin) {
        DownloadFromS3(originPath, context->offset,
                       context->size, context->buf,
                       done);
        doneGuard.release();
    } else {
        LOG(ERROR) << "Unknown origin location."
                   << "location: " << context->location;
        done->SetFailed();
    }
}

void OriginCopyer::DownloadFromS3(const string& objectName,
                                 off_t off,
                                 size_t size,
                                 char* buf,
                                 DownloadClosure* done) {
    brpc::ClosureGuard doneGuard(done);
    if (s3Client_ == nullptr) {
        LOG(ERROR) << "Failed to get s3 object."
                   << "s3 adapter is disabled";
        done->SetFailed();
        return;
    }

    GetObjectAsyncCallBack cb =
        [=] (const S3Adapter* adapter,
             const std::shared_ptr<GetObjectAsyncContext>& context) {
            brpc::ClosureGuard doneGuard(done);
            if (context->retCode != 0) {
                done->SetFailed();
            }
        };

    auto context = std::make_shared<GetObjectAsyncContext>();
    context->key = objectName;
    context->buf = buf;
    context->offset = off;
    context->len = size;
    context->cb = cb;

    s3Client_->GetObjectAsync(context);
    doneGuard.release();
}

void OriginCopyer::DownloadFromCurve(const string& fileName,
                                    off_t off,
                                    size_t size,
                                    char* buf,
                                    DownloadClosure* done) {
    brpc::ClosureGuard doneGuard(done);
    if (curveClient_ == nullptr) {
        LOG(ERROR) << "Failed to read curve file."
                   << "curve client is disabled";
        done->SetFailed();
        return;
    }

    int fd = 0;
    {
        std::unique_lock<std::mutex> lock(mtx_);
        std::unique_lock<std::mutex> lockTime(timeMtx_);
        auto iter = fdMap_.find(fileName);
        if (iter != fdMap_.end()) {
            fd = iter->second;
            auto fdIter = std::find_if(
                curveOpenTime_.cbegin(), curveOpenTime_.cend(),
                [&] (const CurveOpenTimestamp& s) {return s.fd == fd;});
            if (fdIter != curveOpenTime_.cend()) {
                timespec now = butil::seconds_from_now(0);
                curveOpenTime_.emplace_back(fd, fileName, now.tv_sec);
                curveOpenTime_.erase(fdIter);
            } else {
                LOG(ERROR) << "curve cache fd " << fd << " disappered";
            }
        } else {
            fd = curveClient_->Open4ReadOnly(fileName, curveUser_, true);
            if (fd < 0) {
                LOG(ERROR) << "Open curve file failed."
                        << "file name: " << fileName
                        << " ,return code: " << fd;
                done->SetFailed();
                return;
            }
            fdMap_[fileName] = fd;
            timespec now = butil::seconds_from_now(0);
            curveOpenTime_.emplace_back(fd, fileName, now.tv_sec);
            if (timerId_ == bthread::TimerThread::INVALID_TASK_ID) {
                timespec nextTimespec =
                    butil::seconds_from_now(curveFileTimeoutSec_);
                timerId_ = timer_.schedule(
                    &DeleteExpiredCurveCache, this, nextTimespec);
            }
        }
    }

    CurveAioCombineContext *curveCombineCtx = new CurveAioCombineContext();
    curveCombineCtx->done = done;
    curveCombineCtx->curveCtx.offset = off;
    curveCombineCtx->curveCtx.length = size;
    curveCombineCtx->curveCtx.buf = buf;
    curveCombineCtx->curveCtx.op = LIBCURVE_OP::LIBCURVE_OP_READ;
    curveCombineCtx->curveCtx.cb = CurveAioCallback;

    int ret = curveClient_->AioRead(fd,  &curveCombineCtx->curveCtx);
    if (ret !=  LIBCURVE_ERROR::OK) {
        LOG(ERROR) << "Read curve file failed."
                   << "file name: " << fileName
                   << " ,error code: " << ret;
        delete curveCombineCtx;
        done->SetFailed();
    } else {
        doneGuard.release();
    }
}

}  // namespace chunkserver
}  // namespace curve
