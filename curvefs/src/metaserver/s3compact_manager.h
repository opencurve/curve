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
 * @Date: 2021-09-09
 * @Author: majie1
 */

#ifndef CURVEFS_SRC_METASERVER_S3COMPACT_MANAGER_H_
#define CURVEFS_SRC_METASERVER_S3COMPACT_MANAGER_H_

#include <memory>
#include <string>
#include <utility>
#include <mutex>
#include <vector>

#include "curvefs/proto/common.pb.h"
#include "curvefs/src/metaserver/s3compact.h"
#include "curvefs/src/metaserver/s3infocache.h"
#include "src/common/configuration.h"
#include "src/common/interruptible_sleeper.h"
#include "src/common/s3_adapter.h"
#include "curvefs/src/metaserver/s3compact_worker.h"

namespace curvefs {
namespace metaserver {

using curve::common::Configuration;
using curve::common::InterruptibleSleeper;
using curve::common::RWLock;
using curve::common::S3Adapter;
using curve::common::S3AdapterOption;
using curvefs::common::S3Info;

class S3AdapterManager {
 private:
    std::mutex mtx_;
    bool inited_;
    uint64_t size_;  // same size as worker thread count
    std::vector<std::unique_ptr<S3Adapter>> s3adapters_;
    std::vector<bool> used_;
    S3AdapterOption opts_;

 public:
    explicit S3AdapterManager(uint64_t size, const S3AdapterOption& opts)
        : inited_(false), size_(size), opts_(opts) {}
    virtual ~S3AdapterManager() = default;
    virtual void Init();
    virtual void Deinit();
    virtual std::pair<uint64_t, S3Adapter*> GetS3Adapter();
    virtual void ReleaseS3Adapter(uint64_t index);
    virtual S3AdapterOption GetBasicS3AdapterOption();
};

struct S3CompactWorkQueueOption {
    S3AdapterOption s3opts;
    bool enable;
    uint64_t threadNum;
    uint64_t fragmentThreshold;
    uint64_t maxChunksPerCompact;
    uint64_t enqueueSleepMS;
    std::vector<std::string> mdsAddrs;
    std::string metaserverIpStr;
    uint64_t metaserverPort;
    uint64_t s3infocacheSize;
    uint64_t s3ReadMaxRetry;
    uint64_t s3ReadRetryInterval;

    void Init(std::shared_ptr<Configuration> conf);
};

class S3CompactManager {
 private:
    S3CompactWorkQueueOption opts_;
    std::unique_ptr<S3InfoCache> s3infoCache_;
    std::unique_ptr<S3AdapterManager> s3adapterManager_;

    S3CompactWorkerContext workerContext_;
    S3CompactWorkerOptions workerOptions_;

    std::vector<std::unique_ptr<S3CompactWorker>> workers_;

    bool inited_{false};

    S3CompactManager() = default;
    ~S3CompactManager() = default;

 public:
    static S3CompactManager& GetInstance() {
        static S3CompactManager instance_;
        return instance_;
    }

    void Init(std::shared_ptr<Configuration> conf);
    void Register(S3Compact s3compact);
    void Cancel(uint32_t partitionId);

    int Run();
    void Stop();
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_S3COMPACT_MANAGER_H_
