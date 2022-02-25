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
 * @Project: curve
 * @Date: 2021-12-15 10:54:13
 * @Author: chenwei
 */
#ifndef CURVEFS_SRC_METASERVER_PARTITION_CLEAN_MANAGER_H_
#define CURVEFS_SRC_METASERVER_PARTITION_CLEAN_MANAGER_H_

#include <list>
#include <memory>
#include "curvefs/src/metaserver/partition_cleaner.h"
#include "curvefs/src/metaserver/mdsclient/mds_client.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::mdsclient::MdsClient;

struct PartitionCleanOption {
    uint32_t scanPeriodSec;
    uint32_t inodeDeletePeriodMs;
    std::shared_ptr<S3ClientAdaptor> s3Adaptor;
    std::shared_ptr<MdsClient> mdsClient;
};

class PartitionCleanManager {
 public:
    PartitionCleanManager() {
        isStop_ = true;
        inProcessingCleaner_ = nullptr;
        LOG(INFO) << "PartitionCleanManager constructor.";
    }

    ~PartitionCleanManager() {
        LOG(INFO) << "~PartitionCleanManager destructor.";
    }

    static PartitionCleanManager& GetInstance() {
        static PartitionCleanManager instance_;
        return instance_;
    }

    void Add(uint32_t partitionId,
             const std::shared_ptr<PartitionCleaner>& cleaner,
             copyset::CopysetNode *copysetNode);

    void Init(const PartitionCleanOption& option) {
        scanPeriodSec_ = option.scanPeriodSec;
        inodeDeletePeriodMs_ = option.inodeDeletePeriodMs;
        S3ClientAdaptor_ = option.s3Adaptor;
        mdsClient_ = option.mdsClient;
        partitionCleanerCount.expose_as("partition_clean_manager_", "cleaner");
    }

    void Run();

    void Fini();

    void ScanLoop();

    void Remove(uint32_t partitionId);

    uint32_t GetCleanerCount() {
        return partitionCleanerCount.get_value();
    }

 private:
    std::list<std::shared_ptr<PartitionCleaner>> partitonCleanerList_;
    std::shared_ptr<PartitionCleaner> inProcessingCleaner_;
    std::shared_ptr<S3ClientAdaptor> S3ClientAdaptor_;
    std::shared_ptr<MdsClient> mdsClient_;
    uint32_t scanPeriodSec_;
    uint32_t inodeDeletePeriodMs_;
    Atomic<bool> isStop_;
    Thread thread_;
    InterruptibleSleeper sleeper_;
    curve::common::RWLock rwLock_;
    bvar::Adder<uint32_t> partitionCleanerCount;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_PARTITION_CLEAN_MANAGER_H_
