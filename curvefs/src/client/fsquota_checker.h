/*
 *  Copyright (c) 2023 NetEase Inc.
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

#ifndef CURVEFS_SRC_CLIENT_FSQUOTA_CHECKER_H_
#define CURVEFS_SRC_CLIENT_FSQUOTA_CHECKER_H_

#include <time.h>
#include <brpc/periodic_task.h>
#include "curvefs/src/client/rpcclient/metaserver_client.h"

namespace curvefs {
namespace client {

class FsQuotaChecker {
 public:
    static FsQuotaChecker& GetInstance() {
        static FsQuotaChecker instance_;
        return instance_;
    }

    void Init(uint32_t fsId, std::shared_ptr<rpcclient::MdsClient> mdsClient,
              std::shared_ptr<rpcclient::MetaServerClient> metaserverClient);

    bool QuotaBytesCheck(uint64_t incBytes);

    void UpdateQuotaCache();

 private:
    uint32_t fsId_;
    std::atomic<uint64_t> fsCapacityCache_;
    std::atomic<uint64_t> fsUsedBytesCache_;
    std::shared_ptr<rpcclient::MdsClient> mdsClient_;
    std::shared_ptr<rpcclient::MetaServerClient> metaserverClient_;
};

class UpdateQuotaCacheTask : public brpc::PeriodicTask {
 public:
    explicit UpdateQuotaCacheTask(FsQuotaChecker* fsQuotaChecker,
                                  int64_t interval_s)
        : fsQuotaChecker_(fsQuotaChecker), interval_s_(interval_s) {}
    bool OnTriggeringTask(timespec* next_abstime) override;
    void OnDestroyingTask() override;

 private:
    FsQuotaChecker* fsQuotaChecker_;
    int64_t interval_s_;
};

void StartUpdateQuotaCacheTask(FsQuotaChecker* updater, int64_t interval_s);

}  // namespace client

}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FSQUOTA_CHECKER_H_
