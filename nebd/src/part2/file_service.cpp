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
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 */

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <butil/iobuf.h>

#include "nebd/src/part2/file_service.h"
#include "src/common/telemetry/telemetry.h"
#include "src/common/telemetry/brpc_carrier.h"

namespace nebd {
namespace server {
using nebd::client::RetCode;
using OpenFlags = nebd::client::ProtoOpenFlags;

/**
 * use curl -L clientIp:nebd-serverPort/flags/dropRpc?setvalue=true
 * to modify the parameter dynamic
 */
static bool pass_bool(const char*, bool) { return true; }
DEFINE_bool(dropRpc, false, "drop the request rpc");
DEFINE_validator(dropRpc, &pass_bool);

void SetResponse(NebdServerAioContext* context, RetCode retCode) {
    switch (context->op) {
        case LIBAIO_OP::LIBAIO_OP_READ:
        {
            nebd::client::ReadResponse* response =
                dynamic_cast<nebd::client::ReadResponse*>(context->response);
            response->set_retcode(retCode);
            if (context->ret >= 0) {
                brpc::Controller* cntl =
                    dynamic_cast<brpc::Controller *>(context->cntl);
                cntl->response_attachment() =
                    *reinterpret_cast<butil::IOBuf*>(context->buf);
            }

            break;
        }
        case LIBAIO_OP::LIBAIO_OP_WRITE:
        {
            nebd::client::WriteResponse* response =
                dynamic_cast<nebd::client::WriteResponse*>(context->response);
            response->set_retcode(retCode);
            break;
        }
        case LIBAIO_OP::LIBAIO_OP_FLUSH:
        {
            nebd::client::FlushResponse* response =
                dynamic_cast<nebd::client::FlushResponse*>(context->response);
            response->set_retcode(retCode);
            break;
        }
        case LIBAIO_OP::LIBAIO_OP_DISCARD:
        {
            nebd::client::DiscardResponse* response =
                dynamic_cast<nebd::client::DiscardResponse*>(context->response);
            response->set_retcode(retCode);
            break;
        }
        default:
            break;
    }
}

void NebdFileServiceCallback(NebdServerAioContext* context) {
    CHECK(context != nullptr);
    std::unique_ptr<NebdServerAioContext> contextGuard(context);
    std::unique_ptr<butil::IOBuf> iobufGuard(
        reinterpret_cast<butil::IOBuf*>(context->buf));
    brpc::ClosureGuard doneGuard(context->done);
    // for test
    if (FLAGS_dropRpc) {
        doneGuard.release();
        delete context->done;
        LOG(ERROR) << Op2Str(context->op)
                    << " file failed and drop the request rpc.";
        return;
    }

    if (context->ret < 0 && !context->returnRpcWhenIoError) {
        LOG(ERROR) << *context;
        // drop the rpc to ensure not return ioerror
        doneGuard.release();
        delete context->done;
        LOG(ERROR) << Op2Str(context->op)
                    << " file failed and drop the request rpc.";
    } else if (context->ret < 0) {
        LOG(ERROR) << *context;
        SetResponse(context, RetCode::kNoOK);
    } else {
        SetResponse(context, RetCode::kOK);
    }
}

void NebdFileServiceImpl::OpenFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::OpenFileRequest* request,
    nebd::client::OpenFileResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    int fd =
        fileManager_->Open(request->filename(),
                           request->has_flags() ? &request->flags() : nullptr);

    if (fd > 0) {
        response->set_retcode(RetCode::kOK);
        response->set_fd(fd);
        LOG(INFO) << "Open file success. "
                  << "filename: " << request->filename()
                  << ", fd: " << fd;
    } else {
        LOG(ERROR) << "Open file failed. "
                   << "filename: " << request->filename()
                   << ", return code: " << fd;
    }
}

void NebdFileServiceImpl::Write(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::WriteRequest* request,
    nebd::client::WriteResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    NebdServerAioContext* aioContext
        = new (std::nothrow) NebdServerAioContext();
    aioContext->offset = request->offset();
    aioContext->size = request->size();
    aioContext->op = LIBAIO_OP::LIBAIO_OP_WRITE;
    aioContext->cb = NebdFileServiceCallback;
    aioContext->returnRpcWhenIoError = returnRpcWhenIoError_;

    brpc::Controller* cntl = dynamic_cast<brpc::Controller *>(cntl_base);

    std::unique_ptr<butil::IOBuf> buf(new butil::IOBuf());
    *buf = cntl->request_attachment();

    size_t copySize = buf->size();
    if (copySize != aioContext->size) {
        LOG(ERROR) << "Copy attachment failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", copy size: " << copySize;
        return;
    }

    aioContext->buf = buf.get();
    aioContext->response = response;
    aioContext->done = done;
    aioContext->cntl = cntl_base;
    int rc = fileManager_->AioWrite(request->fd(), aioContext);
    if (rc < 0) {
        LOG(ERROR) << "Write file failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", return code: " << rc;
    } else {
        buf.release();
        doneGuard.release();
    }
}

void NebdFileServiceImpl::Read(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::ReadRequest* request,
    nebd::client::ReadResponse* response,
    google::protobuf::Closure* done) {
    auto span = curve::telemetry::StartRpcServerSpan(
        "AioRead", "NebdFileServiceImpl::Read", cntl_base);
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    auto *aioContext = new (std::nothrow) NebdServerAioContext();
    aioContext->offset = request->offset();
    aioContext->size = request->size();
    aioContext->op = LIBAIO_OP::LIBAIO_OP_READ;
    aioContext->cb = NebdFileServiceCallback;
    aioContext->returnRpcWhenIoError = returnRpcWhenIoError_;

    std::unique_ptr<butil::IOBuf> buf(new butil::IOBuf());
    aioContext->buf = buf.get();

    aioContext->response = response;
    aioContext->done = done;
    aioContext->cntl = cntl_base;
    int rc = fileManager_->AioRead(request->fd(), aioContext);
    if (rc < 0) {
        span->SetStatus(trace::StatusCode::kError);
        LOG(ERROR) << "Read file failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", return code: " << rc;
    } else {
        span->SetStatus(trace::StatusCode::kOk);
        buf.release();
        doneGuard.release();
    }
}

void NebdFileServiceImpl::Flush(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::FlushRequest* request,
    nebd::client::FlushResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    NebdServerAioContext* aioContext
        = new (std::nothrow) NebdServerAioContext();
    aioContext->op = LIBAIO_OP::LIBAIO_OP_FLUSH;
    aioContext->cb = NebdFileServiceCallback;
    aioContext->response = response;
    aioContext->done = done;
    aioContext->cntl = cntl_base;
    aioContext->returnRpcWhenIoError = returnRpcWhenIoError_;
    int rc = fileManager_->Flush(request->fd(), aioContext);
    if (rc < 0) {
        LOG(ERROR) << "Flush file failed. "
                   << "fd: " << request->fd()
                   << ", return code: " << rc;
    } else {
        doneGuard.release();
    }
}

void NebdFileServiceImpl::Discard(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::DiscardRequest* request,
    nebd::client::DiscardResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    NebdServerAioContext* aioContext
        = new (std::nothrow) NebdServerAioContext();
    aioContext->offset = request->offset();
    aioContext->size = request->size();
    aioContext->op = LIBAIO_OP::LIBAIO_OP_DISCARD;
    aioContext->cb = NebdFileServiceCallback;
    aioContext->response = response;
    aioContext->done = done;
    aioContext->cntl = cntl_base;
    aioContext->returnRpcWhenIoError = returnRpcWhenIoError_;
    int rc = fileManager_->Discard(request->fd(), aioContext);
    if (rc < 0) {
        LOG(ERROR) << "Flush file failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", return code: " << rc;
    } else {
        doneGuard.release();
    }
}

void NebdFileServiceImpl::GetInfo(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::GetInfoRequest* request,
    nebd::client::GetInfoResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    NebdFileInfo fileInfo;
    int rc = fileManager_->GetInfo(request->fd(), &fileInfo);
    if (rc < 0) {
        LOG(ERROR) << "Get file info failed. "
                   << "fd: " << request->fd()
                   << ", return code: " << rc;
    } else {
        nebd::client::FileInfo* info = new nebd::client::FileInfo();
        info->set_size(fileInfo.size);
        info->set_objsize(fileInfo.obj_size);
        info->set_objnums(fileInfo.num_objs);
        response->set_retcode(RetCode::kOK);
        response->set_allocated_info(info);
    }
}

void NebdFileServiceImpl::CloseFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::CloseFileRequest* request,
    nebd::client::CloseFileResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    int rc = fileManager_->Close(request->fd(), true);
    if (rc < 0) {
        LOG(ERROR) << "Close file failed. "
                   << "fd: " << request->fd()
                   << ", return code: " << rc;
    } else {
        response->set_retcode(RetCode::kOK);
        LOG(INFO) << "Close file success. "
                  << "fd: " << request->fd();
    }
}

void NebdFileServiceImpl::ResizeFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::ResizeRequest* request,
    nebd::client::ResizeResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    int rc = fileManager_->Extend(request->fd(), request->newsize());
    if (rc < 0) {
        LOG(ERROR) << "Resize file failed. "
                   << "fd: " << request->fd()
                   << ", newsize: " << request->newsize()
                   << ", return code: " << rc;
    } else {
        response->set_retcode(RetCode::kOK);
    }
}

void NebdFileServiceImpl::InvalidateCache(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::InvalidateCacheRequest* request,
    nebd::client::InvalidateCacheResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    int rc = fileManager_->InvalidCache(request->fd());
    if (rc < 0) {
        LOG(ERROR) << "Invalid file cache failed. "
                   << "fd: " << request->fd()
                   << ", return code: " << rc;
    } else {
        response->set_retcode(RetCode::kOK);
    }
}

}  // namespace server
}  // namespace nebd
