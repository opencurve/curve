/*
 * Project: nebd
 * Created Date: Thursday January 16th 2020
 * Author: yangyaokai
 * Copyright (c) 2020 netease
 */

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "src/part2/file_service.h"

namespace nebd {
namespace server {

using nebd::client::RetCode;

void NebdFileServiceCallback(NebdServerAioContext* context) {
    CHECK(context != nullptr);
    std::unique_ptr<NebdServerAioContext> contextGuard(context);
    brpc::ClosureGuard doneGuard(context->done);
    switch (context->op) {
        case LIBAIO_OP::LIBAIO_OP_READ:
        {
            nebd::client::ReadResponse* response =
                dynamic_cast<nebd::client::ReadResponse*>(context->response);
            if (context->ret < 0) {
                response->set_retcode(RetCode::kNoOK);
                LOG(ERROR) << "Read file failed. "
                           << "return code: " << context->ret;
            } else {
                brpc::Controller* cntl =
                    dynamic_cast<brpc::Controller *>(context->cntl);
                cntl->response_attachment().append(context->buf,
                                                   context->size);
                response->set_retcode(RetCode::kOK);
            }
            delete[] reinterpret_cast<char*>(context->buf);
            break;
        }
        case LIBAIO_OP::LIBAIO_OP_WRITE:
        {
            nebd::client::WriteResponse* response =
                dynamic_cast<nebd::client::WriteResponse*>(context->response);
            if (context->ret < 0) {
                response->set_retcode(RetCode::kNoOK);
                LOG(ERROR) << "Write file failed. "
                           << "return code: " << context->ret;
            } else {
                response->set_retcode(RetCode::kOK);
            }
            delete[] reinterpret_cast<char*>(context->buf);
            break;
        }
        case LIBAIO_OP::LIBAIO_OP_FLUSH:
        {
            nebd::client::FlushResponse* response =
                dynamic_cast<nebd::client::FlushResponse*>(context->response);
            if (context->ret < 0) {
                response->set_retcode(RetCode::kNoOK);
                LOG(ERROR) << "Flush file failed. "
                           << "return code: " << context->ret;
            } else {
                response->set_retcode(RetCode::kOK);
            }
            break;
        }
        case LIBAIO_OP::LIBAIO_OP_DISCARD:
        {
            nebd::client::DiscardResponse* response =
                dynamic_cast<nebd::client::DiscardResponse*>(context->response);
            if (context->ret < 0) {
                response->set_retcode(RetCode::kNoOK);
                LOG(ERROR) << "Discard file failed. "
                           << "return code: " << context->ret;
            } else {
                response->set_retcode(RetCode::kOK);
            }
            break;
        }
        default:
            break;
    }
}

void NebdFileServiceImpl::OpenFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::OpenFileRequest* request,
    nebd::client::OpenFileResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    int fd = fileManager_->Open(request->filename());
    if (fd > 0) {
        response->set_retcode(RetCode::kOK);
        response->set_fd(fd);
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

    brpc::Controller* cntl = dynamic_cast<brpc::Controller *>(cntl_base);
    aioContext->buf = new char[aioContext->size];
    size_t copySize =
        cntl->request_attachment().copy_to(aioContext->buf, aioContext->size);
    if (copySize != aioContext->size) {
        LOG(ERROR) << "Copy attachment failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", copy size: " << copySize;
        delete[] reinterpret_cast<char*>(aioContext->buf);
        return;
    }

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
        delete[] reinterpret_cast<char*>(aioContext->buf);
    } else {
        doneGuard.release();
    }
}

void NebdFileServiceImpl::Read(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::ReadRequest* request,
    nebd::client::ReadResponse* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    response->set_retcode(RetCode::kNoOK);

    NebdServerAioContext* aioContext
        = new (std::nothrow) NebdServerAioContext();
    aioContext->offset = request->offset();
    aioContext->size = request->size();
    aioContext->op = LIBAIO_OP::LIBAIO_OP_READ;
    aioContext->cb = NebdFileServiceCallback;
    aioContext->buf = new char[request->size()];
    aioContext->response = response;
    aioContext->done = done;
    aioContext->cntl = cntl_base;
    int rc = fileManager_->AioRead(request->fd(), aioContext);
    if (rc < 0) {
        LOG(ERROR) << "Read file failed. "
                   << "fd: " << request->fd()
                   << ", offset: " << request->offset()
                   << ", size: " << request->size()
                   << ", return code: " << rc;
        delete[] reinterpret_cast<char*>(aioContext->buf);
    } else {
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
