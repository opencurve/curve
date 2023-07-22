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
 * Created Date: 2019-08-12
 * Author: hzchenwei7
 */
#include "nebd/test/part1/fake_file_service.h"

namespace nebd {
namespace client {

const int64_t kBufferSize = 1024;
char buffer[kBufferSize];

void FakeNebdFileService::OpenFile(::google::protobuf::RpcController* controller,  // NOLINT
                        const ::nebd::client::OpenFileRequest* request,
                        ::nebd::client::OpenFileResponse* response,
                        ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", OpenFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("OpenFile OK");
    response->set_fd(1);

    return;
}

void FakeNebdFileService::CloseFile(::google::protobuf::RpcController* controller,  // NOLINT
                       const ::nebd::client::CloseFileRequest* request,
                       ::nebd::client::CloseFileResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", CloseFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("CloseFile OK");

    return;
}

void FakeNebdFileService::Read(::google::protobuf::RpcController* controller,
                       const ::nebd::client::ReadRequest* request,
                       ::nebd::client::ReadResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", Read.";

    cntl->response_attachment().append(buffer + request->offset(),
                                       request->size());
    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Read OK");

    return;
}

void FakeNebdFileService::Write(::google::protobuf::RpcController* controller,
                       const ::nebd::client::WriteRequest* request,
                       ::nebd::client::WriteResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", Write.";

    // memcpy(buffer + request->offset(),
    //        cntl->request_attachment().to_string().c_str(),
    //        request->size());
    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Write OK");

    return;
}

void FakeNebdFileService::Discard(::google::protobuf::RpcController* controller,
                       const ::nebd::client::DiscardRequest* request,
                       ::nebd::client::DiscardResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", Discard.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Discard OK");

    return;
}

void FakeNebdFileService::ResizeFile(::google::protobuf::RpcController* controller,  // NOLINT
                       const ::nebd::client::ResizeRequest* request,
                       ::nebd::client::ResizeResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", ResizeFile.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("ResizeFile OK");

    fileSize_ = request->newsize();

    return;
}

void FakeNebdFileService::Flush(::google::protobuf::RpcController* controller,
                       const ::nebd::client::FlushRequest* request,
                       ::nebd::client::FlushResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", Flush.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("Flush OK");

    return;
}

void FakeNebdFileService::GetInfo(::google::protobuf::RpcController* controller,
                       const ::nebd::client::GetInfoRequest* request,
                       ::nebd::client::GetInfoResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", GetInfo.";
    nebd::client::FileInfo* info = new nebd::client::FileInfo();
    info->set_size(fileSize_);
    info->set_objsize(fileSize_);
    info->set_objnums(1);
    info->set_blocksize(blockSize_);
    response->set_retcode(RetCode::kOK);
    response->set_retmsg("GetInfo OK");
    response->set_allocated_info(info);

    return;
}

void FakeNebdFileService::InvalidateCache(
                       ::google::protobuf::RpcController* controller,
                       const ::nebd::client::InvalidateCacheRequest* request,
                       ::nebd::client::InvalidateCacheResponse* response,
                       ::google::protobuf::Closure* done) {
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);

    LOG(INFO) << "logid = " << cntl->log_id() << ", InvalidateCache.";

    response->set_retcode(RetCode::kOK);
    response->set_retmsg("InvalidateCache OK");

    return;
}

}  // namespace client
}  // namespace nebd
