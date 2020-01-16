#ifndef SRC_PART1_CONNECTION_MANAGER_H_
#define SRC_PART1_CONNECTION_MANAGER_H_

#include <brpc/channel.h>
#include <thread>   // NOLINT
#include <memory>
#include "src/part1/nebd_metacache.h"

namespace nebd {
namespace client {

class ConnectionManager {
 public:
    ConnectionManager(uint32_t heartbeatIntervalS,
                      std::shared_ptr<NebdClientMetaCache> metaCache);
    ~ConnectionManager();
    void Run();
    brpc::Channel* GetChannel();

 private:
    void SendHeartBeat();

 private:
    brpc::Channel* channel_;
    uint32_t heartbeatIntervalS_;
    std::shared_ptr<NebdClientMetaCache>  metaCache_;
    std::thread heartbeatThread_;
};

}  // namespace client
}  // namespace nebd

#endif  // SRC_PART1_CONNECTION_MANAGER_H_
