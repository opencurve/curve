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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_MDS_H_
#define CURVEFS_SRC_MDS_MDS_H_

#include <bvar/bvar.h>

#include <memory>
#include <string>

#include "curvefs/src/mds/fs_manager.h"
#include "src/common/configuration.h"
#include "src/kvstorageclient/etcd_client.h"
#include "src/leader_election/leader_election.h"

namespace curvefs {
namespace mds {

using ::curve::common::Configuration;
using ::curve::election::LeaderElection;
using ::curve::election::LeaderElectionOptions;
using ::curve::kvstorage::KVStorageClient;

struct MDSOptions {
    int dummyPort;
    std::string mdsListenAddr;
    SpaceOptions spaceOptions;
    MetaserverOptions metaserverOptions;
};

class Mds {
 public:
    Mds();
    ~Mds();

    Mds(const Mds&) = delete;
    Mds& operator=(const Mds&) = delete;

    void InitOptions(std::shared_ptr<Configuration> conf);
    void Init();
    void Run();
    void Stop();

    // Start dummy server for metric
    void StartDummyServer();

    // Start leader election
    void StartCompaginLeader();

 private:
    void InitEtcdClient();
    void InitEtcdConf(EtcdConf* etcdConf);
    bool CheckEtcd();

    void InitLeaderElectionOption(LeaderElectionOptions* option);
    void InitLeaderElection(const LeaderElectionOptions& option);

 private:
    // mds configuration items
    std::shared_ptr<Configuration> conf_;
    // initialized or not
    bool inited_;
    // running as the main MDS or not
    bool running_;
    std::shared_ptr<FsManager> fsManager_;
    std::shared_ptr<FsStorage> fsStorage_;
    std::shared_ptr<SpaceClient> spaceClient_;
    std::shared_ptr<MetaserverClient> metaserverClient_;
    MDSOptions options_;

    bool etcdClientInited_;
    std::shared_ptr<curve::kvstorage::EtcdClientImp> etcdClient_;

    std::shared_ptr<curve::election::LeaderElection> leaderElection_;

    std::shared_ptr<curve::idgenerator::EtcdIdGenerator> idGen_;

    bvar::Status<std::string> status_;

    std::string etcdEndpoint_;
};

}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_MDS_H_
