
#include "src/part1/connection_manager.h"

namespace nebd {
namespace client {

ConnectionManager::ConnectionManager(
    uint32_t heartbeatIntervalS,
    std::shared_ptr<NebdClientMetaCache> metaCache)
    : heartbeatIntervalS_(heartbeatIntervalS)
    , metaCache_(metaCache) {
    //  TODO
}

ConnectionManager::~ConnectionManager() {
    //  TODO
}

void ConnectionManager::Run() {
    //  TODO
}

brpc::Channel* ConnectionManager::GetChannel() {
    return channel_;
}

void ConnectionManager::SendHeartBeat() {
    //  TODO
}

}  // namespace client
}  // namespace nebd