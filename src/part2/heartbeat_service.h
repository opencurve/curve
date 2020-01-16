#ifndef SRC_PART2_HEARTBEAT_SERVICE_H_
#define SRC_PART2_HEARTBEAT_SERVICE_H_

#include <memory>

#include "src/common/heartbeat.pb.h"
#include "src/part2/file_manager.h"

namespace nebd {
namespace server {

class NebdHeartbeatServiceImpl : public nebd::client::NebdHeartbeatService {
 public:
    explicit NebdHeartbeatServiceImpl(
        std::shared_ptr<NebdFileManager> fileManager)
        : fileManager_(fileManager) {}
    virtual ~NebdHeartbeatServiceImpl() {}
    virtual void KeepAlive(google::protobuf::RpcController* cntl_base,
                           const nebd::client::HeartbeatRequest* request,
                           nebd::client::HeartbeatResponse* response,
                           google::protobuf::Closure* done);

 private:
    std::shared_ptr<NebdFileManager>  fileManager_;
};

}  // namespace server
}  // namespace nebd

#endif  // SRC_PART2_HEARTBEAT_SERVICE_H_
