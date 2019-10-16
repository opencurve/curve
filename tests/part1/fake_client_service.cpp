/*
 * Project: nebd
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 * Copyright (c) 2018 netease
 */
#include "tests/part1/fake_client_service.h"

namespace nebd {
namespace client {

uint64_t filesize = 50*1024*1024;  // 50MB
char *buf = reinterpret_cast<char *>(malloc(filesize));

void ClientService::OpenFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::OpenFileRequest* request,
                       ::nebd::client::OpenFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "OpenFile.";
    if (buf == nullptr) {
        response->set_retcode(RetCode::kNoOK);
        response->set_retmsg("OpenFile FAIL");
        return;
    }

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("OpenFile OK");
    response->set_fd(1);

    return;
}

void ClientService::CloseFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::CloseFileRequest* request,
                       ::nebd::client::CloseFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "CloseFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("CloseFile OK");

    return;
}

void ClientService::Read(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ReadRequest* request,
                       ::nebd::client::ReadResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "Read.";

    cntl->response_attachment().append(buf + request->offset(),
                                request->size());
    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Read OK");

    return;
}

void ClientService::Write(::google::protobuf::RpcController* controller,
                       const ::nebd::client::WriteRequest* request,
                       ::nebd::client::WriteResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "Write.";

    memcpy(buf + request->offset(),
            cntl->request_attachment().to_string().c_str(), request->size());
    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Write OK");

    return;
}

void ClientService::Discard(::google::protobuf::RpcController* controller,
                       const ::nebd::client::DiscardRequest* request,
                       ::nebd::client::DiscardResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "Discard.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Discard OK");

    return;
}

void ClientService::StatFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::StatFileRequest* request,
                       ::nebd::client::StatFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "StatFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("StatFile OK");
    response->set_size(filesize);

    return;
}

void ClientService::ResizeFile(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ResizeRequest* request,
                       ::nebd::client::ResizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "ResizeFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("ResizeFile OK");

    return;
}

void ClientService::Flush(::google::protobuf::RpcController* controller,
                       const ::nebd::client::FlushRequest* request,
                       ::nebd::client::FlushResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "Flush.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Flush OK");

    return;
}

void ClientService::GetInfo(::google::protobuf::RpcController* controller,
                       const ::nebd::client::GetInfoRequest* request,
                       ::nebd::client::GetInfoResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "GetInfo.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("GetInfo OK");
    response->set_objsize(filesize);

    return;
}

void ClientService::InvalidateCache(
                       ::google::protobuf::RpcController* controller,
                       const ::nebd::client::InvalidateCacheRequest* request,
                       ::nebd::client::InvalidateCacheResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << "InvalidateCache.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("InvalidateCache OK");

    return;
}

}  // namespace client
}  // namespace nebd

