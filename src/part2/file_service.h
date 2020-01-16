#ifndef SRC_PART2_FILE_SERVICE_H_
#define SRC_PART2_FILE_SERVICE_H_

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <vector>
#include <string>
#include <memory>

#include "src/common/client.pb.h"
#include "src/part2/file_manager.h"

namespace nebd {
namespace server {

class NebdFileServiceImpl : public nebd::client::NebdFileService {
 public:
    explicit NebdFileServiceImpl(std::shared_ptr<NebdFileManager> fileManager)
        : fileManager_(fileManager) {}

    virtual ~NebdFileServiceImpl() {}

    virtual void OpenFile(google::protobuf::RpcController* cntl_base,
                          const nebd::client::OpenFileRequest* request,
                          nebd::client::OpenFileResponse* response,
                          google::protobuf::Closure* done);

    virtual void Write(google::protobuf::RpcController* cntl_base,
                       const nebd::client::WriteRequest* request,
                       nebd::client::WriteResponse* response,
                       google::protobuf::Closure* done);

    virtual void Read(google::protobuf::RpcController* cntl_base,
                      const nebd::client::ReadRequest* request,
                      nebd::client::ReadResponse* response,
                      google::protobuf::Closure* done);

    virtual void StatFile(google::protobuf::RpcController* cntl_base,
                          const nebd::client::StatFileRequest* request,
                          nebd::client::StatFileResponse* response,
                          google::protobuf::Closure* done);

    virtual void GetInfo(google::protobuf::RpcController* cntl_base,
                         const nebd::client::GetInfoRequest* request,
                         nebd::client::GetInfoResponse* response,
                         google::protobuf::Closure* done);

    virtual void Flush(google::protobuf::RpcController* cntl_base,
                       const nebd::client::FlushRequest* request,
                       nebd::client::FlushResponse* response,
                       google::protobuf::Closure* done);

    virtual void CloseFile(google::protobuf::RpcController* cntl_base,
                           const nebd::client::CloseFileRequest* request,
                           nebd::client::CloseFileResponse* response,
                           google::protobuf::Closure* done);

    virtual void Discard(google::protobuf::RpcController* cntl_base,
                         const nebd::client::DiscardRequest* request,
                         nebd::client::DiscardResponse* response,
                         google::protobuf::Closure* done);

    virtual void ResizeFile(google::protobuf::RpcController* cntl_base,
                            const nebd::client::ResizeRequest* request,
                            nebd::client::ResizeResponse* response,
                            google::protobuf::Closure* done);

    virtual void InvalidateCache(google::protobuf::RpcController* cntl_base,
                            const nebd::client::InvalidateCacheRequest* request,
                            nebd::client::InvalidateCacheResponse* response,
                            google::protobuf::Closure* done);

 private:
    std::shared_ptr<NebdFileManager> fileManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_FILE_SERVICE_H_
