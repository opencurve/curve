/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-12
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_HEARTBEAT_H_
#define CURVEFS_SRC_METASERVER_HEARTBEAT_H_

#include <braft/node.h>  // NodeImpl
#include <braft/node_manager.h>
#include <gtest/gtest_prod.h>

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <thread>  //NOLINT
#include <vector>
#include "curvefs/proto/heartbeat.pb.h"
#include "curvefs/src/metaserver/common/types.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "src/common/concurrent/concurrent.h"
#include "src/common/wait_interval.h"
#include "absl/types/optional.h"

namespace curvefs {
namespace metaserver {
using curve::common::Thread;
using curve::fs::LocalFileSystem;
using curvefs::common::Peer;
using curvefs::mds::heartbeat::BlockGroupStatInfo;
using curvefs::mds::heartbeat::CopySetConf;
using curvefs::mds::heartbeat::BlockGroupDeallcateStatusCode;
using curvefs::metaserver::copyset::CopysetNode;
using curvefs::metaserver::copyset::CopysetNodeManager;

using HeartbeatRequest = curvefs::mds::heartbeat::MetaServerHeartbeatRequest;
using HeartbeatResponse = curvefs::mds::heartbeat::MetaServerHeartbeatResponse;
using MetaServerSpaceStatus = curvefs::mds::heartbeat::MetaServerSpaceStatus;
using CopysetNodePtr = std::shared_ptr<CopysetNode>;
using PeerId = braft::PeerId;
using TaskStatus = butil::Status;
using BlockGroupStatInfoMap = std::map<uint32_t, BlockGroupStatInfo>;

class ResourceCollector;

/**
 * heartbeat subsystem option
 */
struct HeartbeatOptions {
    MetaServerID metaserverId;
    std::string metaserverToken;
    std::string storeUri;
    std::string mdsListenAddr;
    std::string ip;
    uint32_t port;
    uint32_t intervalSec;
    uint32_t timeout;
    CopysetNodeManager* copysetNodeManager;
    ResourceCollector* resourceCollector;
    std::shared_ptr<LocalFileSystem> fs;
};

class HeartbeatTaskExecutor;

/**
 * heartbeat subsystem
 */
class Heartbeat {
 public:
    Heartbeat() {}
    ~Heartbeat() {}

    /**
     * @brief init heartbeat subsystem
     * @param[in] options
     * @return 0:success; not 0: fail
     */
    int Init(const HeartbeatOptions& options);

    /**
     * @brief clean heartbeat subsystem
     * @return 0:success; not 0: fail
     */
    int Fini();

    /**
     * @brief run heartbeat subsystem
     * @return 0:success; not 0: fail
     */
    int Run();

 private:
    FRIEND_TEST(HeartbeatTest, Test_BuildRequest);

    /**
     * @brief stop heartbeat subsystem
     * @return 0:success; not 0: fail
     */
    int Stop();

    /*
     * heartbeat work thread
     */
    void HeartbeatWorker();

    void BuildCopysetInfo(curvefs::mds::heartbeat::CopySetInfo* info,
                         CopysetNode* copyset);

    void BuildBlockGroupStatInfo(CopysetNode *copyset,
                                 BlockGroupStatInfoMap *blockGroupStatInfoMap);

    int BuildRequest(HeartbeatRequest* request);

    int SendHeartbeat(const HeartbeatRequest& request,
                      HeartbeatResponse* response);

    /*
     * print HeartbeatRequest to log
     */
    void DumpHeartbeatRequest(const HeartbeatRequest& request);

    /*
     * print HeartbeatResponse to log
     */
    void DumpHeartbeatResponse(const HeartbeatResponse& response);

    bool GetMetaserverSpaceStatus(MetaServerSpaceStatus* status,
                                  uint64_t ncopysets);

    // Handle heartbeat and send recyclable BlockGroup requests
    void DeallocateBolckGroup(const HeartbeatResponse &response);

 private:
    friend class HeartbeatTest;

 private:
    Thread hbThread_;

    std::atomic<bool> toStop_;

    ::curve::common::WaitInterval waitInterval_;

    CopysetNodeManager* copysetMan_;

    // metaserver store path
    std::string storePath_;

    HeartbeatOptions options_;

    // MDS addr list
    std::vector<std::string> mdsEps_;

    // index of current service mds
    int inServiceIndex_;

    // MetaServer addr
    butil::EndPoint msEp_;

    // heartbeat subsystem init time, use unix time
    uint64_t startUpTime_;

    std::unique_ptr<HeartbeatTaskExecutor> taskExecutor_;
};

// execute tasks from heartbeat response
class HeartbeatTaskExecutor {
 public:
    HeartbeatTaskExecutor(CopysetNodeManager* mgr, const butil::EndPoint& ep);

    void ExecTasks(const HeartbeatResponse& response);

    // for unit test
    void SetDeallocTask(uint32_t fsid, uint64_t offset) {
        deallocTask_[fsid] = offset;
    }

 private:
    void ExecOneTask(const CopySetConf& conf);

    void DoTransferLeader(CopysetNode* node, const CopySetConf& conf);
    void DoAddPeer(CopysetNode* node, const CopySetConf& conf);
    void DoRemovePeer(CopysetNode* node, const CopySetConf& conf);
    void DoChangePeer(CopysetNode* node, const CopySetConf& conf);
    void DoPurgeCopyset(PoolId poolid, CopysetId copysetid);

    bool NeedPurge(const CopySetConf& conf);

 private:
    friend class Heartbeat;

    CopysetNodeManager* copysetMgr_;
    butil::EndPoint ep_;

    // key is fsid, value is blockgroupOffset
    std::map<uint32_t, uint64_t> deallocTask_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_HEARTBEAT_H_
