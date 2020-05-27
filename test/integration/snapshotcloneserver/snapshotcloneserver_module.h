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
 * Created Date: Mon Dec 23 2019
 * Author: xuchaojie
 */

#ifndef TEST_INTEGRATION_SNAPSHOTCLONESERVER_SNAPSHOTCLONESERVER_MODULE_H_
#define TEST_INTEGRATION_SNAPSHOTCLONESERVER_SNAPSHOTCLONESERVER_MODULE_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <brpc/server.h>

#include <memory>

#include "src/snapshotcloneserver/snapshotclone_service.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"
#include "src/common/configuration.h"

#include "src/common/snapshotclone/snapshotclone_define.h"
#include "src/snapshotcloneserver/common/curvefs_client.h"

#include "src/snapshotcloneserver/snapshot/snapshot_data_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_data_store_s3.h"
#include "src/snapshotcloneserver/common/snapshotclone_meta_store.h"
#include "src/snapshotcloneserver/snapshot/snapshot_task_manager.h"
#include "src/snapshotcloneserver/snapshot/snapshot_core.h"
#include "src/snapshotcloneserver/common/config.h"
#include "src/snapshotcloneserver/common/snapshotclone_metric.h"


#include "test/integration/snapshotcloneserver/fake_curvefs_client.h"
#include "test/integration/snapshotcloneserver/fake_snapshot_data_store.h"
#include "test/integration/snapshotcloneserver/fake_snapshotclone_meta_store.h"


using ::curve::common::Configuration;


namespace curve {
namespace snapshotcloneserver {

class SnapshotCloneServerModule {
 public:
    int Start(const SnapshotCloneServerOptions &option);

    void Stop();


    std::shared_ptr<FakeCurveFsClient> GetCurveFsClient() {
        return client_;
    }

    std::shared_ptr<FakeSnapshotCloneMetaStore> GetMetaStore() {
        return metaStore_;
    }

    std::shared_ptr<FakeSnapshotDataStore> GetDataStore() {
        return dataStore_;
    }

 private:
    SnapshotCloneServerOptions serverOption_;

    std::shared_ptr<FakeCurveFsClient> client_;
    std::shared_ptr<FakeSnapshotCloneMetaStore> metaStore_;
    std::shared_ptr<FakeSnapshotDataStore> dataStore_;


    std::shared_ptr<SnapshotServiceManager> snapshotServiceManager_;
    std::shared_ptr<CloneServiceManager> cloneServiceManager_;

    std::shared_ptr<SnapshotCloneServiceImpl> service_;
    std::shared_ptr<brpc::Server> server_;
};

}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // TEST_INTEGRATION_SNAPSHOTCLONESERVER_SNAPSHOTCLONESERVER_MODULE_H_
