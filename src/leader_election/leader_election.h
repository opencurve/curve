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
 * Created Date: Tue June 25th 2019
 * Author: lixiaocui
 */

#ifndef SRC_LEADER_ELECTION_LEADER_ELECTION_H_
#define SRC_LEADER_ELECTION_LEADER_ELECTION_H_

#include <fiu.h>
#include <memory>
#include <string>

#include "src/kvstorageclient/etcd_client.h"
#include "src/common/namespace_define.h"

namespace curve {
namespace election {

using ::curve::kvstorage::EtcdClientImp;
using ::curve::common::LEADERCAMPAIGNNPFX;

struct LeaderElectionOptions {
    // etcd client
    std::shared_ptr<EtcdClientImp> etcdCli;

    // session with ttl, within ttl timeout
    uint32_t sessionInterSec;

    // Overtime for running for leader
    uint32_t electionTimeoutMs;

    // leader name, it is recommended to use ip+port for differentiation
    std::string leaderUniqueName;

    // key that need to be contested
    std::string campaginPrefix;
};

class LeaderElection {
 public:
    explicit LeaderElection(LeaderElectionOptions opt) {
        opt_ = opt;
        realPrefix_ = LEADERCAMPAIGNNPFX + opt.campaginPrefix;
    }

    /**
     * @brief CampaignLeader
     *
     * @return 0 indicates a successful election, -1 indicates a failed election
     */
    int CampaignLeader();

    /**
     * @brief StartObserverLeader starts the leader node monitoring thread
     */
    void StartObserverLeader();

    /**
     * @brief LeaderResign Leader proactively resigns from its leadership position. After successful resignation, other nodes can compete to become the new leader
     */
    int LeaderResign();

    /**
     * @brief returns the leader name
     */
    const std::string& GetLeaderName() {
        return opt_.leaderUniqueName;
    }

 public:
    /**
     * @brief Monitor the leader node created in etcd. Under normal circumstances, this function continuously blocks. 
     *        Exiting indicates a leader change or, from the client's perspective, an abnormality in etcd, which leads to process termination
     */
    int ObserveLeader();

 private:
    // option
    LeaderElectionOptions opt_;

    // realPrefix_ = leader campaign public prefix + custom prefix
    std::string realPrefix_;

    // The ID number recorded in the object manager after leader election
    uint64_t leaderOid_;
};
}  // namespace election
}  // namespace curve

#endif  // SRC_LEADER_ELECTION_LEADER_ELECTION_H_

