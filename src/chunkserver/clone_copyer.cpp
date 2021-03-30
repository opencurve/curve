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

OriginCopyer::OriginCopyer()
    : curveClient_(nullptr)
    , s3Client_(nullptr) {}

int OriginCopyer::Init(const CopyerOptions& options) {
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
    return 0;
}

int OriginCopyer::Fini() {
    if (curveClient_ != nullptr) {
        for (auto &pair : fdMap_) {
            curveClient_->Close(pair.second);
        }
        curveClient_->UnInit();
    }
    if (s3Client_ != nullptr) {
        s3Client_->Deinit();
    }
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
        auto iter = fdMap_.find(fileName);
        if (iter != fdMap_.end()) {
            fd = iter->second;
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
