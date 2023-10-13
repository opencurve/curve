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
 * Created Date: Monday March 9th 2020
 * Author: hzsunjianliang
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_

#include <memory>
#include <string>

#include "src/client/libcurve_file.h"
#include "src/client/libcurve_snapshot.h"
#include "src/common/configuration.h"
#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/leader_election/leader_election.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store_etcd.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/snapshotclone_service.h"

namespace curve {
namespace snapshotcloneserver {

extern const char metricExposePrefix[];
extern const char configMetricName[];
extern const char statusMetricName[];
extern const char ACTIVE[];
extern const char STANDBY[];

using EtcdClientImp = ::curve::kvstorage::EtcdClientImp;
using Configuration = ::curve::common::Configuration;
using LeaderElection = ::curve::election::LeaderElection;

struct SnapShotCloneServerOptions {
    CurveClientOptions clientOptions;
    SnapshotCloneServerOptions serverOption;

    // etcd options
    EtcdConf etcdConf;
    int etcdClientTimeout;
    int etcdRetryTimes;

    // leaderelections options
    std::string campaginPrefix;
    int sessionInterSec;
    int electionTimeoutMs;

    int dummyPort;

    // s3
    std::string s3ConfPath;
};

class SnapShotCloneServer {
 public:
    explicit SnapShotCloneServer(std::shared_ptr<Configuration> config)
        : conf_(config) {}
    /**
     * @brief: Initialize all configurations required for snapshotcloneserver
     * through configuration
     */
    void InitAllSnapshotCloneOptions(void);

    /**
     * @brief leader election, if not selected, continue to wait. If selected,
     * establish a watch and return
     */
    void StartCompaginLeader(void);

    /**
     * @brief: Start dummyPort to check the active and standby snapshotserver
     *        Survival and various configuration metrics and version information
     */
    void StartDummy(void);

    /**
     * @brief initializes various core structures of clone and snapshot
     */
    bool Init(void);

    /**
     * @brief: Start the logic and thread pool of each component
     */
    bool Start(void);

    /**
     * @brief Stop all services
     */
    void Stop(void);

    /**
     * @brief Start RPC service until external kill
     */
    void RunUntilQuit(void);

 private:
    bool InitEtcdClient(void);

 private:
    std::shared_ptr<Configuration> conf_;
    SnapShotCloneServerOptions snapshotCloneServerOptions_;
    // Mark yourself as active/standby
    bvar::Status<std::string> status_;
    // Client interacting with ETCD
    std::shared_ptr<EtcdClientImp> etcdClient_;
    std::shared_ptr<LeaderElection> leaderElection_;

    std::shared_ptr<SnapshotClient> snapClient_;
    std::shared_ptr<FileClient> fileClient_;
    std::shared_ptr<CurveFsClientImpl> client_;

    std::shared_ptr<SnapshotCloneMetaStoreEtcd> metaStore_;
    std::shared_ptr<SnapshotDataStore> dataStore_;
    std::shared_ptr<SnapshotReference> snapshotRef_;
    std::shared_ptr<SnapshotMetric> snapshotMetric_;
    std::shared_ptr<SnapshotCoreImpl> snapshotCore_;
    std::shared_ptr<SnapshotTaskManager> snapshotTaskManager_;
    std::shared_ptr<SnapshotServiceManager> snapshotServiceManager_;

    std::shared_ptr<CloneMetric> cloneMetric_;
    std::shared_ptr<CloneReference> cloneRef_;
    std::shared_ptr<CloneCoreImpl> cloneCore_;
    std::shared_ptr<CloneTaskManager> cloneTaskMgr_;
    std::shared_ptr<CloneServiceManagerBackendImpl> cloneServiceManagerBackend_;
    std::shared_ptr<CloneServiceManager> cloneServiceManager_;
    std::shared_ptr<SnapshotCloneServiceImpl> service_;
    std::shared_ptr<brpc::Server> server_;
};
}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVER_H_
