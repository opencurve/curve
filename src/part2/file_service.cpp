#include "src/part2/file_service.h"

namespace nebd {
namespace server {

void NebdFileServiceImpl::OpenFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::OpenFileRequest* request,
    nebd::client::OpenFileResponse* response, google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::Write(google::protobuf::RpcController* cntl_base,
                                  const nebd::client::WriteRequest* request,
                                  nebd::client::WriteResponse* response,
                                  google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::Read(google::protobuf::RpcController* cntl_base,
                                 const nebd::client::ReadRequest* request,
                                 nebd::client::ReadResponse* response,
                                 google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::StatFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::StatFileRequest* request,
    nebd::client::StatFileResponse* response, google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::GetInfo(google::protobuf::RpcController* cntl_base,
                                    const nebd::client::GetInfoRequest* request,
                                    nebd::client::GetInfoResponse* response,
                                    google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::Flush(google::protobuf::RpcController* cntl_base,
                                  const nebd::client::FlushRequest* request,
                                  nebd::client::FlushResponse* response,
                                  google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::CloseFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::CloseFileRequest* request,
    nebd::client::CloseFileResponse* response,
    google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::Discard(google::protobuf::RpcController* cntl_base,
                                    const nebd::client::DiscardRequest* request,
                                    nebd::client::DiscardResponse* response,
                                    google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::ResizeFile(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::ResizeRequest* request,
    nebd::client::ResizeResponse* response, google::protobuf::Closure* done) {
    // TODO
}

void NebdFileServiceImpl::InvalidateCache(
    google::protobuf::RpcController* cntl_base,
    const nebd::client::InvalidateCacheRequest* request,
    nebd::client::InvalidateCacheResponse* response,
    google::protobuf::Closure* done) {
    // TODO
}

}  // namespace server
}  // namespace nebd
