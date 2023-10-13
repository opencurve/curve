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
 * Created Date: 2019-12-05
 * Author: lixiaocui
 */

#ifndef TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_
#define TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_

#include <braft/node.h>
#include <braft/node_manager.h>

#include <atomic>
#include <string>
#include <thread>  //NOLINT
#include <vector>

#include "include/chunkserver/chunkserver_common.h"
#include "proto/heartbeat.pb.h"
#include "src/chunkserver/cli.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "src/chunkserver/heartbeat.h"
#include "src/common/configuration.h"
#include "src/common/uri_parser.h"
#include "test/client/fake/fakeMDS.h"

namespace curve {
namespace chunkserver {

using ::curve::common::UriParser;

class HeartbeatTestCommon {
 public:
    explicit HeartbeatTestCommon(const std::string& filename) {
        hbtestCommon_ = this;
        handlerReady_.store(false, std::memory_order_release);

        mds_ = new FakeMDS(filename);
        mds_->SetChunkServerHeartbeatCallback(HeartbeatCallback);
        mds_->Initialize();
        mds_->StartService();
    }

    std::atomic<bool>& GetReady() { return handlerReady_; }

    std::mutex& GetMutex() { return hbMtx_; }

    std::condition_variable& GetCV() { return hbCV_; }

    void UnInitializeMds() {
        mds_->UnInitialize();
        delete mds_;
    }

    /**
     * CleanPeer: Clear the specified copyset data on the peer
     *
     * @param[in] poolId Logical pool ID
     * @param[in] copysetId copyset ID
     * @param[in] peer chunkserver IP
     */
    void CleanPeer(LogicPoolID poolId, CopysetID copysetId,
                   const std::string& peer);

    /**
     * CreateCopysetPeers: Create a copyset of the specified configuration on
     * the specified chunkserverlist
     *
     * @param[in] poolId Logical pool ID
     * @param[in] copysetId copyset ID
     * @param[in] cslist The chunkserver list for the copyset to be created
     * @param[in] conf Use this configuration as the initial configuration to
     * create a copyset
     */
    void CreateCopysetPeers(LogicPoolID poolId, CopysetID copysetId,
                            const std::vector<std::string>& cslist,
                            const std::string& conf);

    /**
     * WaitCopysetReady: Wait for the specified copyset to select the leader
     *
     * @param[in] poolId Logical pool ID
     * @param[in] copysetId copyset ID
     * @param[in] conf specifies the copyset replication group members
     */
    void WaitCopysetReady(LogicPoolID poolId, CopysetID copysetId,
                          const std::string& conf);

    /**
     * TransferLeaderSync: Trigger transferleader and waits for completion
     *
     * @param[in] poolId Logical pool ID
     * @param[in] copysetId copyset ID
     * @param[in] conf specifies the copyset replication group members
     * @param[in] newLeader Target Leader
     */
    void TransferLeaderSync(LogicPoolID poolId, CopysetID copysetId,
                            const std::string& conf,
                            const std::string& newLeader);

    /**
     * WailForConfigChangeOk: Determine whether the chunkserver has reported the
     * expected copyset information within the specified time limit
     * (timeLimitMs).
     *
     * @param[in] conf mds needs to issue a change command to the specified
     * copyset
     * @param[in] expectedInfo replication group configuration after change
     * @param[in] timeLimitMs waiting time
     *
     * @return false - Copyset configuration failed to meet expectations within
     * the specified time, true - met expectations
     */
    bool WailForConfigChangeOk(
        const ::curve::mds::heartbeat::CopySetConf& conf,
        ::curve::mds::heartbeat::CopySetInfo expectedInfo, int timeLimitMs);

    /**
     * SameCopySetInfo: Compare two copysetInfo structures to check if they are
     * identical.
     *
     * @param[in] orig The copysetInfo to compare.
     * @param[in] expect The expected copysetInfo for comparison.
     *
     * @return true if they are identical, false if they are not.
     */
    bool SameCopySetInfo(const ::curve::mds::heartbeat::CopySetInfo& orig,
                         const ::curve::mds::heartbeat::CopySetInfo& expect);

    /**
     * ReleaseHeartbeat: Set the callback in the heartbeat to nullptr.
     */
    void ReleaseHeartbeat();

    /**
     * SetHeartbeatInfo: Copy the cntl and other information received by mds to
     * the member variable
     */
    void SetHeartbeatInfo(::google::protobuf::RpcController* cntl,
                          const HeartbeatRequest* request,
                          HeartbeatResponse* response,
                          ::google::protobuf::Closure* done);

    /**
     * GetHeartbeat: Set the current member's cntl and other variables into the
     * RPC.
     */
    void GetHeartbeat(::google::protobuf::RpcController** cntl,
                      const HeartbeatRequest** request,
                      HeartbeatResponse** response,
                      ::google::protobuf::Closure** done);

    /**
     * HeartbeatCallback: heartbeat callback
     */
    static void HeartbeatCallback(::google::protobuf::RpcController* controller,
                                  const HeartbeatRequest* request,
                                  HeartbeatResponse* response,
                                  ::google::protobuf::Closure* done);

 private:
    FakeMDS* mds_;

    mutable std::mutex hbMtx_;
    std::condition_variable hbCV_;
    std::atomic<bool> handlerReady_;

    ::google::protobuf::RpcController* cntl_;
    const HeartbeatRequest* req_;
    HeartbeatResponse* resp_;
    ::google::protobuf::Closure* done_;

    static HeartbeatTestCommon* hbtestCommon_;
};

int RemovePeersData(bool rmChunkServerMeta = false);

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_HEARTBEAT_TEST_COMMON_H_
